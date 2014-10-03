/*
  Copyright (c) 2014 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "connection.hpp"

#include "auth.hpp"
#include "auth_requests.hpp"
#include "auth_responses.hpp"
#include "common.hpp"
#include "constants.hpp"
#include "connecter.hpp"
#include "timer.hpp"
#include "config.hpp"
#include "result_response.hpp"
#include "supported_response.hpp"
#include "startup_request.hpp"
#include "query_request.hpp"
#include "options_request.hpp"
#include "register_request.hpp"
#include "error_response.hpp"
#include "event_response.hpp"
#include "logger.hpp"
#include "cassandra.h"

#include "third_party/boost/boost/bind.hpp"

#include <iomanip>
#include <sstream>

namespace cass {

void Connection::StartupHandler::on_set(ResponseMessage* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_SUPPORTED:
      connection_->on_supported(response);
      break;

    case CQL_OPCODE_ERROR: {
      ErrorResponse* error
          = static_cast<ErrorResponse*>(response->response_body().get());
      if (error->code() == CQL_ERROR_PROTOCOL_ERROR &&
          error->message().find("Invalid or unsupported protocol version") != std::string::npos) {
        connection_->is_invalid_protocol_ = true;
      } else if (error->code() == CQL_ERROR_BAD_CREDENTIALS) {
        connection_->auth_error_ = error->message();
      }
      connection_->notify_error(error_response_message("Error response", error));
      break;
    }

    case CQL_OPCODE_AUTHENTICATE:
      connection_->on_authenticate();
      break;

    case CQL_OPCODE_AUTH_CHALLENGE:
      connection_->on_auth_challenge(
            static_cast<AuthResponseRequest*>(request_.get()),
            static_cast<AuthChallengeResponse*>(response->response_body().get())->token());
      break;

    case CQL_OPCODE_AUTH_SUCCESS:
      connection_->on_auth_success(
            static_cast<AuthResponseRequest*>(request_.get()),
            static_cast<AuthSuccessResponse*>(response->response_body().get())->token());
      break;

    case CQL_OPCODE_READY:
      connection_->on_ready();
      break;

    case CQL_OPCODE_RESULT:
      on_result_response(response);
      break;

    default:
      connection_->notify_error("Invalid opcode");
      break;
  }
}

void Connection::StartupHandler::on_error(CassError code,
                                          const std::string& message) {
  std::ostringstream ss;
  ss << "Error: '" << message
     << "' (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0') << code << ")";
  connection_->notify_error(ss.str());
}

void Connection::StartupHandler::on_timeout() {
  if (!connection_->is_closing()) {
    connection_->notify_error("Timed out");
  }
}

void Connection::StartupHandler::on_result_response(ResponseMessage* response) {
  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());
  switch (result->kind()) {
    case CASS_RESULT_KIND_SET_KEYSPACE:
      connection_->on_set_keyspace();
      break;
    default:
      connection_->notify_error(
          "Invalid result. Expected set keyspace.");
      break;
  }
}

Connection::Connection(uv_loop_t* loop, Logger* logger, const Config& config,
                       const Address& address,  const std::string& keyspace,
                       int protocol_version)
    : state_(CONNECTION_STATE_NEW)
    , is_defunct_(false)
    , is_invalid_protocol_(false)
    , is_registered_for_events_(false)
    , loop_(loop)
    , logger_(logger)
    , config_(config)
    , address_(address)
    , addr_string_(address.to_string())
    , keyspace_(keyspace)
    , protocol_version_(protocol_version)
    , response_(new ResponseMessage())
    , ssl_handshake_done_(false)
    , version_("3.0.0")
    , event_types_(0)
    , connect_timer_(NULL) {
  socket_.data = this;
  uv_tcp_init(loop_, &socket_);
}

void Connection::connect() {
  if (state_ == CONNECTION_STATE_NEW) {
    state_ = CONNECTION_STATE_CONNECTING;
    connect_timer_ = Timer::start(loop_, config_.connect_timeout(), this,
                                  on_connect_timeout);
    Connecter::connect(&socket_, address_, this, on_connect);
  }
}

bool Connection::execute(Handler* handler) {
  int8_t stream = stream_manager_.acquire_stream(handler);
  if (stream < 0) {
    return false;
  }

  handler->inc_ref(); // Connection reference
  handler->set_stream(stream);

  if (!handler->encode(protocol_version_, 0x00)) {
    stream_manager_.release_stream(handler->stream());
    handler->on_error(CASS_ERROR_LIB_MESSAGE_ENCODE,
                      "Operation unsupported by this protocol version");
    handler->dec_ref();
    return true; // Don't retry
  }

  logger_->trace("Connection: Sending message type %s with %d",
                 opcode_to_string(handler->request()->opcode()).c_str(), stream);

  pending_requests_.add_to_back(handler);

  uv_stream_t* sock_stream = copy_cast<uv_tcp_t*, uv_stream_t*>(&socket_);

  handler->set_state(Handler::REQUEST_STATE_WRITING);
  handler->start_timer(loop_, config_.request_timeout(), handler,
                       boost::bind(&Connection::on_timeout, this, _1));
  handler->write(sock_stream, handler,
                 boost::bind(&Connection::on_write, this, _1));

  return true;
}

void Connection::schedule_schema_agreement(const SharedRefPtr<SchemaChangeHandler>& handler, uint64_t wait) {
  PendingSchemaAgreement* pending_schema_agreement = new PendingSchemaAgreement(handler);
  pending_schema_aggreements_.add_to_back(pending_schema_agreement);
  pending_schema_agreement->timer
      = Timer::start(loop_,
                     wait,
                     pending_schema_agreement,
                     boost::bind(&Connection::on_pending_schema_agreement, this, _1));
}

void Connection::close() {
  if (state_ != CONNECTION_STATE_CLOSING) {
    uv_handle_t* handle = copy_cast<uv_tcp_t*, uv_handle_t*>(&socket_);
    if (!uv_is_closing(handle)) {
      if (state_ == CONNECTION_STATE_CONNECTED ||
          state_ == CONNECTION_STATE_READY) {
        uv_read_stop(copy_cast<uv_tcp_t*, uv_stream_t*>(&socket_));
      }
      state_ = CONNECTION_STATE_CLOSING;
      uv_close(handle, on_close);
    }
  }
}

void Connection::defunct() {
  is_defunct_ = true;
  close();
}

void Connection::consume(char* input, size_t size) {
  char* buffer = input;
  int remaining = size;

  while (remaining != 0) {
    int consumed = response_->decode(protocol_version_, buffer, remaining);
    if (consumed <= 0) {
      logger_->error("Connection: Error consuming message on host %s", addr_string_.c_str());
      remaining = 0;
      defunct();
      continue;
    }

    if (response_->is_body_ready()) {
      ScopedPtr<ResponseMessage> response(response_.release());
      response_.reset(new ResponseMessage());

      logger_->trace(
          "Connection: Consumed message type %s with stream %d, input %lu, remaining %d on host %s",
          opcode_to_string(response->opcode()).c_str(), static_cast<int>(response->stream()),
          size, remaining, addr_string_.c_str());

      if (response->stream() < 0) {
        if (response->opcode() == CQL_OPCODE_EVENT) {
          if (event_callback_) {
            event_callback_(static_cast<EventResponse*>(response->response_body().get()));
          }
        } else {
          logger_->error("Connection: Invalid response with opcode of %d returned on event stream",
                         response->opcode());
          defunct();
        }
      } else {
        Handler* handler = NULL;
        if (stream_manager_.get_item(response->stream(), handler)) {
          switch (handler->state()) {
            case Handler::REQUEST_STATE_READING:
              maybe_set_keyspace(response.get());
              pending_requests_.remove(handler);
              handler->stop_timer();
              handler->set_state(Handler::REQUEST_STATE_DONE);
              handler->on_set(response.get());
              handler->dec_ref();
              break;

            case Handler::REQUEST_STATE_WRITING:
              // There are cases when the read callback will happen
              // before the write callback. If this happens we have
              // to allow the write callback to cleanup.
              maybe_set_keyspace(response.get());
              handler->set_state(Handler::REQUEST_STATE_READ_BEFORE_WRITE);
              handler->on_set(response.get());
              break;

            case Handler::REQUEST_STATE_TIMEOUT:
              pending_requests_.remove(handler);
              handler->set_state(Handler::REQUEST_STATE_DONE);
              handler->dec_ref();
              break;

            default:
              assert(false && "Invalid request state after receiving response");
              break;
          }
        } else {
          logger_->error("Connection: Invalid stream returned from host %s",
                         addr_string_.c_str());
          defunct();
        }
      }
    }
    remaining -= consumed;
    buffer += consumed;
  }
}

void Connection::maybe_set_keyspace(ResponseMessage* response) {
  if (response->opcode() == CQL_OPCODE_RESULT) {
    ResultResponse* result =
        static_cast<ResultResponse*>(response->response_body().get());
    if (result->kind() == CASS_RESULT_KIND_SET_KEYSPACE) {
      keyspace_ = result->keyspace();
    }
  }
}

void Connection::on_connect(Connecter* connecter) {
  Connection* connection = static_cast<Connection*>(connecter->data());

  if (connection->connect_timer_ == NULL) {
    return; // Timed out
  }

  Timer::stop(connection->connect_timer_);
  connection->connect_timer_ = NULL;

  if (connecter->status() == Connecter::SUCCESS) {
    connection->logger_->debug("Connection: Connected to host %s",
                               connection->addr_string_.c_str());
    uv_read_start(copy_cast<uv_tcp_t*, uv_stream_t*>(&connection->socket_),
                  alloc_buffer, on_read);
    connection->state_ = CONNECTION_STATE_CONNECTED;
    connection->on_connected();
  } else {
    connection->logger_->info("Connection: Connect error '%s' on host %s",
                              uv_err_name(uv_last_error(connection->loop_)),
                              connection->addr_string_.c_str() );
    connection->notify_error("Unable to connect");
  }
}

void Connection::on_connect_timeout(Timer* timer) {
  Connection* connection = static_cast<Connection*>(timer->data());
  connection->connect_timer_ = NULL;
  connection->notify_error("Connection timeout");
}

void Connection::on_close(uv_handle_t* handle) {
  Connection* connection = static_cast<Connection*>(handle->data);

  connection->logger_->debug("Connection to host %s closed",
                             connection->addr_string_.c_str());

  while (!connection->pending_requests_.is_empty()) {
    Handler* handler = connection->pending_requests_.front();
    connection->pending_requests_.remove(handler);
    if (handler->state() == Handler::REQUEST_STATE_WRITING ||
        handler->state() == Handler::REQUEST_STATE_READING) {
      handler->on_timeout();
      handler->stop_timer();
    }
    handler->dec_ref();
  }

  while (!connection->pending_schema_aggreements_.is_empty()) {
    PendingSchemaAgreement* pending_schema_aggreement
        = connection->pending_schema_aggreements_.front();
    connection->pending_schema_aggreements_.remove(pending_schema_aggreement);
    pending_schema_aggreement->stop_timer();
    pending_schema_aggreement->handler->on_closing();
    delete pending_schema_aggreement;
  }

  if (connection->closed_callback_) {
    connection->closed_callback_(connection);
  }

  delete connection;
}

void Connection::on_read(uv_stream_t* client, ssize_t nread, uv_buf_t buf) {
  Connection* connection = static_cast<Connection*>(client->data);

  if (nread == -1) {
    if (uv_last_error(connection->loop_).code != UV_EOF) {
      connection->logger_->info("Connection: Read error '%s' on host %s",
                                uv_err_name(uv_last_error(connection->loop_)),
                                connection->addr_string_.c_str());
    }
    connection->defunct();
    free_buffer(buf);
    return;
  }
  connection->consume(buf.base, nread);
  free_buffer(buf);
}

void Connection::on_write(RequestWriter* writer) {
  Handler* handler = static_cast<Handler*>(writer->data());

  switch (handler->state()) {
    case Handler::REQUEST_STATE_WRITING:
      if (writer->status() == RequestWriter::SUCCESS) {
        handler->set_state(Handler::REQUEST_STATE_READING);
      } else {
        if (!is_closing()) {
          logger_->info("Connection: Write error '%s' on host %s",
                        uv_err_name(uv_last_error(loop_)),
                        addr_string_.c_str());
          defunct();
        }

        stream_manager_.release_stream(handler->stream());
        pending_requests_.remove(handler);
        handler->stop_timer();
        handler->set_state(Handler::REQUEST_STATE_DONE);
        handler->on_error(CASS_ERROR_LIB_WRITE_ERROR,
                          "Unable to write to socket");
        handler->dec_ref();
      }
      break;

    case Handler::REQUEST_STATE_TIMEOUT:
      // The read may still come back, handle cleanup there
      break;

    case Handler::REQUEST_STATE_READ_BEFORE_WRITE:
      // The read callback happened before the write callback
      // returned. This is now responsible for cleanup.
      pending_requests_.remove(handler);
      handler->stop_timer();
      handler->set_state(Handler::REQUEST_STATE_DONE);
      handler->dec_ref();
      break;

    default:
      assert(false && "Invalid request state after write finished");
      break;
  }
}

void Connection::on_timeout(RequestTimer* timer) {
  Handler* handler = static_cast<Handler*>(timer->data());
  logger_->info("Connection: Request timed out to host %s", addr_string_.c_str());
  // TODO (mpenick): We need to handle the case where we have too many
  // timeout requests and we run out of stream ids. The java-driver
  // uses a threshold to defunct the connneciton.
  handler->set_state(Handler::REQUEST_STATE_TIMEOUT);
  handler->on_timeout();
}

void Connection::on_connected() {
  // TODO (mpenick): Implement SSL
  execute(new StartupHandler(this, new OptionsRequest()));
}

void Connection::on_authenticate() {
  if (protocol_version_ == 1) {
    send_credentials();
  } else {
    send_initial_auth_response();
  }
}

void Connection::on_auth_challenge(AuthResponseRequest* request, const std::string& token) {
  AuthResponseRequest* auth_response
      = new AuthResponseRequest(request->auth()->evaluate_challenge(token),
                                request->auth().release());
  execute(new StartupHandler(this, auth_response));
}

void Connection::on_auth_success(AuthResponseRequest* request, const std::string& token) {
  request->auth()->on_authenticate_success(token);
  on_ready();
}

void Connection::on_ready() {
  if (!is_registered_for_events_ && event_types_ != 0) {
    is_registered_for_events_ = true;
    execute(new StartupHandler(this, new RegisterRequest(event_types_)));
    return;
  }

  if (keyspace_.empty()) {
    notify_ready();
  } else {
    QueryRequest* query = new QueryRequest();
    query->set_query("use \"" + keyspace_ + "\"");
    execute(new StartupHandler(this, query));
  }
}

void Connection::on_set_keyspace() {
  notify_ready();
}

void Connection::on_supported(ResponseMessage* response) {
  SupportedResponse* supported =
      static_cast<SupportedResponse*>(response->response_body().get());

  // TODO(mstump) do something with the supported info
  (void)supported;

  execute(new StartupHandler(this, new StartupRequest()));
}

void Connection::on_pending_schema_agreement(Timer* timer) {
  PendingSchemaAgreement* pending_schema_agreement
      = static_cast<PendingSchemaAgreement*>(timer->data());
  pending_schema_aggreements_.remove(pending_schema_agreement);
  pending_schema_agreement->handler->execute();
  delete pending_schema_agreement;
}

void Connection::notify_ready() {
  state_ = CONNECTION_STATE_READY;
  if (ready_callback_) {
    ready_callback_(this);
  }
}

void Connection::notify_error(const std::string& error) {
  logger_->error("Connection: Host %s had the following error on startup: '%s'",
                 addr_string_.c_str(), error.c_str());
  defunct();
}

void Connection::send_credentials() {
  ScopedPtr<V1Authenticator> v1_auth(config_.auth_provider()->new_authenticator_v1(address_));
  if (v1_auth) {
    V1Authenticator::Credentials credentials;
    v1_auth->get_credentials(&credentials);
    execute(new StartupHandler(this, new CredentialsRequest(credentials)));
  } else {
    send_initial_auth_response();
  }
}

void Connection::send_initial_auth_response() {
  Authenticator* auth = config_.auth_provider()->new_authenticator(address_);
  if (auth == NULL) {
    auth_error_ = "Authenticaion required but no auth provider set";
    notify_error(auth_error_);
  } else {
    AuthResponseRequest* auth_response
        = new AuthResponseRequest(auth->initial_response(), auth);
    execute(new StartupHandler(this, auth_response));
  }
}

void Connection::PendingSchemaAgreement::stop_timer() {
  if (timer != NULL) {
    Timer::stop(timer);
    timer = NULL;
  }
}

} // namespace cass
