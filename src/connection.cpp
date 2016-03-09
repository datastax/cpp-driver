/*
  Copyright (c) 2014-2016 DataStax

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
#include "cassandra.h"
#include "constants.hpp"
#include "connector.hpp"
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
#include "utils.hpp"

#include <iomanip>
#include <sstream>

#define SSL_READ_SIZE 8192
#define SSL_WRITE_SIZE 8192
#define SSL_ENCRYPTED_BUFS_COUNT 16

#define MAX_BUFFER_REUSE_NO 8
#define BUFFER_REUSE_SIZE 64 * 1024

#if UV_VERSION_MAJOR == 0
#define UV_ERRSTR(status, loop) uv_strerror(uv_last_error(loop))
#else
#define UV_ERRSTR(status, loop) uv_strerror(status)
#endif

namespace cass {

static void cleanup_pending_handlers(List<Handler>* pending) {
  while (!pending->is_empty()) {
    Handler* handler = pending->front();
    pending->remove(handler);
    if (handler->state() == Handler::REQUEST_STATE_WRITING ||
        handler->state() == Handler::REQUEST_STATE_READING) {
      handler->on_timeout();
      handler->stop_timer();
    }
    handler->dec_ref();
  }
}

void Connection::StartupHandler::on_set(ResponseMessage* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_SUPPORTED:
      connection_->on_supported(response);
      break;

    case CQL_OPCODE_ERROR: {
      ErrorResponse* error
          = static_cast<ErrorResponse*>(response->response_body().get());
      if (error->code() == CQL_ERROR_PROTOCOL_ERROR &&
          error->message().find("Invalid or unsupported protocol version") != StringRef::npos) {
        connection_->notify_error(error->message().to_string(), CONNECTION_ERROR_INVALID_PROTOCOL);
      } else if (error->code() == CQL_ERROR_BAD_CREDENTIALS) {
        connection_->notify_error(error->message().to_string(), CONNECTION_ERROR_AUTH);
      } else if (error->code() == CQL_ERROR_INVALID_QUERY &&
                 error->message().find("Keyspace") == 0 &&
                 error->message().find("does not exist") != StringRef::npos) {
        connection_->notify_error("Received error response " + error->error_message(), CONNECTION_ERROR_KEYSPACE);
      } else {
        connection_->notify_error("Received error response " + error->error_message());
      }
      break;
    }

    case CQL_OPCODE_AUTHENTICATE:
      connection_->on_authenticate();
      break;

    case CQL_OPCODE_AUTH_CHALLENGE:
      connection_->on_auth_challenge(
            static_cast<const AuthResponseRequest*>(request_.get()),
            static_cast<AuthChallengeResponse*>(response->response_body().get())->token());
      break;

    case CQL_OPCODE_AUTH_SUCCESS:
      connection_->on_auth_success(
            static_cast<const AuthResponseRequest*>(request_.get()),
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
    connection_->notify_error("Timed out", CONNECTION_ERROR_TIMEOUT);
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
      connection_->notify_error("Invalid result response. Expected set keyspace.");
      break;
  }
}

Connection::HeartbeatHandler::HeartbeatHandler(Connection* connection)
  : Handler(new OptionsRequest()) {
  set_connection(connection);
}

void Connection::HeartbeatHandler::on_set(ResponseMessage* response) {
  LOG_TRACE("Heartbeat completed on host %s",
            connection_->address_string().c_str());
  connection_->idle_start_time_ms_ = 0;
  connection_->heartbeat_outstanding_ = false;
}

void Connection::HeartbeatHandler::on_error(CassError code, const std::string& message) {
  LOG_WARN("An error occurred on host %s during a heartbeat request: %s",
           connection_->address_string().c_str(),
           message.c_str());
  connection_->heartbeat_outstanding_ = false;
}

void Connection::HeartbeatHandler::on_timeout() {
  LOG_WARN("Heartbeat request timed out on host %s",
           connection_->address_string().c_str());
  connection_->heartbeat_outstanding_ = false;
}

Connection::Connection(uv_loop_t* loop,
                       const Config& config,
                       Metrics* metrics,
                       const Address& address,
                       const std::string& keyspace,
                       int protocol_version,
                       Listener* listener)
    : state_(CONNECTION_STATE_NEW)
    , error_code_(CONNECTION_OK)
    , ssl_error_code_(CASS_OK)
    , pending_writes_size_(0)
    , loop_(loop)
    , config_(config)
    , metrics_(metrics)
    , address_(address)
    , addr_string_(address.to_string())
    , keyspace_(keyspace)
    , protocol_version_(protocol_version)
    , listener_(listener)
    , response_(new ResponseMessage())
    , stream_manager_(protocol_version)
    , ssl_session_(NULL)
    , idle_start_time_ms_(0)
    , heartbeat_outstanding_(false) {
  socket_.data = this;
  uv_tcp_init(loop_, &socket_);

  if (uv_tcp_nodelay(&socket_,
                     config.tcp_nodelay_enable() ? 1 : 0) != 0) {
    LOG_WARN("Unable to set tcp nodelay");
  }

  if (uv_tcp_keepalive(&socket_,
                      config.tcp_keepalive_enable() ? 1 : 0,
                      config.tcp_keepalive_delay_secs()) != 0) {
    LOG_WARN("Unable to set tcp keepalive");
  }

  SslContext* ssl_context = config_.ssl_context();
  if (ssl_context != NULL) {
    ssl_session_.reset(ssl_context->create_session(address_));
  }
}

Connection::~Connection()
{
  while (!buffer_reuse_list_.empty()) {
    uv_buf_t buf = buffer_reuse_list_.top();
    delete[] buf.base;
    buffer_reuse_list_.pop();
  }
}

void Connection::connect() {
  if (state_ == CONNECTION_STATE_NEW) {
    set_state(CONNECTION_STATE_CONNECTING);
    connect_timer_.start(loop_, config_.connect_timeout_ms(), this,
                         on_connect_timeout);
    Connector::connect(&socket_, address_, this, on_connect);
  }
}

bool Connection::write(Handler* handler, bool flush_immediately) {
  return internal_write(handler, flush_immediately, true);
}

bool Connection::internal_write(Handler* handler, bool flush_immediately, bool reset_idle_time) {
  int stream = stream_manager_.acquire(handler);
  if (stream < 0) {
    return false;
  }

  handler->inc_ref(); // Connection reference
  handler->set_connection(this);
  handler->set_stream(stream);

  if (pending_writes_.is_empty() || pending_writes_.back()->is_flushed()) {
    if (ssl_session_) {
      pending_writes_.add_to_back(new PendingWriteSsl(this));
    } else {
      pending_writes_.add_to_back(new PendingWrite(this));
    }
  }

  PendingWriteBase* pending_write = pending_writes_.back();

  int32_t request_size = pending_write->write(handler);
  if (request_size < 0) {
    stream_manager_.release(stream);
    switch (request_size) {
      case Request::ENCODE_ERROR_BATCH_WITH_NAMED_VALUES:
      case Request::ENCODE_ERROR_PARAMETER_UNSET:
        // Already handled
        break;

      default:
        handler->on_error(CASS_ERROR_LIB_MESSAGE_ENCODE,
                          "Operation unsupported by this protocol version");
        break;
    }
    handler->dec_ref();
    return true; // Don't retry
  }

  pending_writes_size_ += request_size;
  if (pending_writes_size_ > config_.write_bytes_high_water_mark()) {
    LOG_WARN("Exceeded write bytes water mark (current: %u water mark: %u) on connection to host %s",
             static_cast<unsigned int>(pending_writes_size_),
             config_.write_bytes_high_water_mark(),
             addr_string_.c_str());
    metrics_->exceeded_write_bytes_water_mark.inc();
    set_state(CONNECTION_STATE_OVERWHELMED);
  }

  LOG_TRACE("Sending message type %s with stream %d",
            opcode_to_string(handler->request()->opcode()).c_str(), stream);

  handler->set_state(Handler::REQUEST_STATE_WRITING);
  handler->start_timer(loop_,
                       config_.request_timeout_ms(),
                       handler,
                       Connection::on_timeout);

  if (flush_immediately) {
    pending_write->flush();
  }

  if (reset_idle_time) {
    idle_start_time_ms_ = 0;
    restart_heartbeat_timer();
  }

  return true;
}

void Connection::flush() {
  if (pending_writes_.is_empty()) return;

  pending_writes_.back()->flush();
}

void Connection::schedule_schema_agreement(const SharedRefPtr<SchemaChangeHandler>& handler, uint64_t wait) {
  PendingSchemaAgreement* pending_schema_agreement = new PendingSchemaAgreement(handler);
  pending_schema_agreements_.add_to_back(pending_schema_agreement);
  pending_schema_agreement->timer.start(loop_,
                                        wait,
                                        pending_schema_agreement,
                                        Connection::on_pending_schema_agreement);
}

void Connection::close() {
  internal_close(CONNECTION_STATE_CLOSE);
}

void Connection::defunct() {
  internal_close(CONNECTION_STATE_CLOSE_DEFUNCT);
}

void Connection::internal_close(ConnectionState close_state) {
  assert(close_state == CONNECTION_STATE_CLOSE ||
         close_state == CONNECTION_STATE_CLOSE_DEFUNCT);

  if (state_ != CONNECTION_STATE_CLOSE &&
      state_ != CONNECTION_STATE_CLOSE_DEFUNCT) {
    uv_handle_t* handle = copy_cast<uv_tcp_t*, uv_handle_t*>(&socket_);
    if (!uv_is_closing(handle)) {
      heartbeat_timer_.stop();
      connect_timer_.stop();
      if (state_ == CONNECTION_STATE_CONNECTED ||
          state_ == CONNECTION_STATE_READY) {
        uv_read_stop(copy_cast<uv_tcp_t*, uv_stream_t*>(&socket_));
      }
      set_state(close_state);
      uv_close(handle, on_close);
    }
  }
}

void Connection::set_state(ConnectionState new_state) {
  // Only update if the state changed
  if (new_state == state_) return;

  ConnectionState old_state = state_;

  switch (state_) {
    case CONNECTION_STATE_NEW:
      assert(new_state == CONNECTION_STATE_CONNECTING &&
             "Invalid connection state after new");
      state_ = new_state;
      break;

    case CONNECTION_STATE_CONNECTING:
      assert((new_state == CONNECTION_STATE_CONNECTED ||
              new_state == CONNECTION_STATE_CLOSE ||
              new_state == CONNECTION_STATE_CLOSE_DEFUNCT) &&
             "Invalid connection state after connecting");
      state_ = new_state;
      break;

    case CONNECTION_STATE_CONNECTED:
      assert((new_state == CONNECTION_STATE_REGISTERING_EVENTS ||
              new_state == CONNECTION_STATE_READY ||
              new_state == CONNECTION_STATE_CLOSE ||
              new_state == CONNECTION_STATE_CLOSE_DEFUNCT) &&
             "Invalid connection state after connected");
      state_ = new_state;
      break;

    case CONNECTION_STATE_REGISTERING_EVENTS:
      assert((new_state == CONNECTION_STATE_READY ||
              new_state == CONNECTION_STATE_CLOSE ||
              new_state == CONNECTION_STATE_CLOSE_DEFUNCT) &&
             "Invalid connection state after registering for events");
      state_ = new_state;
      break;

    case CONNECTION_STATE_READY:
      assert((new_state == CONNECTION_STATE_OVERWHELMED ||
              new_state == CONNECTION_STATE_CLOSE ||
              new_state == CONNECTION_STATE_CLOSE_DEFUNCT) &&
             "Invalid connection state after ready");
      state_ = new_state;
      break;

    case CONNECTION_STATE_OVERWHELMED:
      assert((new_state == CONNECTION_STATE_READY ||
              new_state == CONNECTION_STATE_CLOSE ||
              new_state == CONNECTION_STATE_CLOSE_DEFUNCT) &&
             "Invalid connection state after being overwhelmed");
      state_ = new_state;
      break;

    case CONNECTION_STATE_CLOSE:
      assert(false && "No state change after close");
      break;

    case CONNECTION_STATE_CLOSE_DEFUNCT:
      assert(false && "No state change after close defunct");
      break;
  }

  // Only change the availability if the state changes to/from being ready
  if (new_state == CONNECTION_STATE_READY || old_state == CONNECTION_STATE_READY) {
    listener_->on_availability_change(this);
  }
}

void Connection::consume(char* input, size_t size) {
  char* buffer = input;
  size_t remaining = size;

  while (remaining != 0) {
    ssize_t consumed = response_->decode(buffer, remaining);
    if (consumed <= 0) {
      notify_error("Error consuming message");
      remaining = 0;
      continue;
    }

    if (response_->is_body_ready()) {
      ScopedPtr<ResponseMessage> response(response_.release());
      response_.reset(new ResponseMessage());

      LOG_TRACE("Consumed message type %s with stream %d, input %u, remaining %u on host %s",
                opcode_to_string(response->opcode()).c_str(),
                static_cast<int>(response->stream()),
                static_cast<unsigned int>(size),
                static_cast<unsigned int>(remaining),
                addr_string_.c_str());

      if (response->stream() < 0) {
        if (response->opcode() == CQL_OPCODE_EVENT) {
          listener_->on_event(static_cast<EventResponse*>(response->response_body().get()));
        } else {
          notify_error("Invalid response opcode for event stream: " +
                       opcode_to_string(response->opcode()));
        }
      } else {
        Handler* handler = NULL;
        if (stream_manager_.get_pending_and_release(response->stream(), handler)) {
          switch (handler->state()) {
            case Handler::REQUEST_STATE_READING:
              maybe_set_keyspace(response.get());
              pending_reads_.remove(handler);
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
              pending_reads_.remove(handler);
              handler->set_state(Handler::REQUEST_STATE_DONE);
              handler->dec_ref();
              break;

            case Handler::REQUEST_STATE_TIMEOUT_WRITE_OUTSTANDING:
              // We must wait for the write callback before we can do the cleanup
              handler->set_state(Handler::REQUEST_STATE_READ_BEFORE_WRITE);
              break;

            default:
              assert(false && "Invalid request state after receiving response");
              break;
          }
        } else {
          notify_error("Invalid stream ID");
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
      keyspace_ = result->keyspace().to_string();
    }
  }
}

void Connection::on_connect(Connector* connector) {
  Connection* connection = static_cast<Connection*>(connector->data());

  if (!connection->connect_timer_.is_running()) {
    return; // Timed out
  }

  if (connector->status() == 0) {
    LOG_DEBUG("Connected to host %s", connection->addr_string_.c_str());

    if (connection->ssl_session_) {
      uv_read_start(copy_cast<uv_tcp_t*, uv_stream_t*>(&connection->socket_),
                    Connection::alloc_buffer_ssl, Connection::on_read_ssl);
    } else {
      uv_read_start(copy_cast<uv_tcp_t*, uv_stream_t*>(&connection->socket_),
                    Connection::alloc_buffer, Connection::on_read);
    }

    connection->set_state(CONNECTION_STATE_CONNECTED);

    if (connection->ssl_session_) {
      connection->ssl_handshake();
    } else {
      connection->on_connected();
    }
  } else {
    LOG_ERROR("Connect error '%s' on host %s",
              UV_ERRSTR(connector->status(),
                        connection->loop_),
              connection->addr_string_.c_str() );
    connection->notify_error("Unable to connect");
  }
}

void Connection::on_connect_timeout(Timer* timer) {
  Connection* connection = static_cast<Connection*>(timer->data());
  connection->notify_error("Connection timeout", CONNECTION_ERROR_TIMEOUT);
  connection->metrics_->connection_timeouts.inc();
}

void Connection::on_close(uv_handle_t* handle) {
  Connection* connection = static_cast<Connection*>(handle->data);

  LOG_DEBUG("Connection to host %s closed",
            connection->addr_string_.c_str());

  cleanup_pending_handlers(&connection->pending_reads_);

  while (!connection->pending_writes_.is_empty()) {
    PendingWriteBase* pending_write
        = connection->pending_writes_.front();
    connection->pending_writes_.remove(pending_write);
    delete pending_write;
  }

  while (!connection->pending_schema_agreements_.is_empty()) {
    PendingSchemaAgreement* pending_schema_aggreement
        = connection->pending_schema_agreements_.front();
    connection->pending_schema_agreements_.remove(pending_schema_aggreement);
    pending_schema_aggreement->stop_timer();
    pending_schema_aggreement->handler->on_closing();
    delete pending_schema_aggreement;
  }

  connection->listener_->on_close(connection);

  delete connection;
}

uv_buf_t Connection::internal_alloc_buffer(size_t suggested_size) {
  if (suggested_size <= BUFFER_REUSE_SIZE) {
    if (!buffer_reuse_list_.empty()) {
      uv_buf_t ret = buffer_reuse_list_.top();
      buffer_reuse_list_.pop();
      return ret;
    }
    return uv_buf_init(new char[BUFFER_REUSE_SIZE], BUFFER_REUSE_SIZE);
  }
  return uv_buf_init(new char[suggested_size], suggested_size);
}

void Connection::internal_reuse_buffer(uv_buf_t buf) {
  if (buf.len == BUFFER_REUSE_SIZE && buffer_reuse_list_.size() < MAX_BUFFER_REUSE_NO) {
    buffer_reuse_list_.push(buf);
    return;
  }
  delete[] buf.base;
}

#if UV_VERSION_MAJOR == 0
uv_buf_t Connection::alloc_buffer(uv_handle_t* handle, size_t suggested_size) {
  Connection* connection = static_cast<Connection*>(handle->data);
  return connection->internal_alloc_buffer(suggested_size);
}
#else
void Connection::alloc_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  Connection* connection = static_cast<Connection*>(handle->data);
  *buf =  connection->internal_alloc_buffer(suggested_size);
}
#endif

#if UV_VERSION_MAJOR == 0
void Connection::on_read(uv_stream_t* client, ssize_t nread, uv_buf_t buf) {
#else
void Connection::on_read(uv_stream_t* client, ssize_t nread, const uv_buf_t* buf) {
#endif
  Connection* connection = static_cast<Connection*>(client->data);

  if (nread < 0) {
#if UV_VERSION_MAJOR == 0
    if (uv_last_error(connection->loop_).code != UV_EOF) {
#else
    if (nread != UV_EOF) {
#endif
      LOG_ERROR("Read error '%s' on host %s",
                UV_ERRSTR(nread, connection->loop_),
                connection->addr_string_.c_str());
    }
    connection->defunct();

#if UV_VERSION_MAJOR == 0
    connection->internal_reuse_buffer(buf);
#else
    connection->internal_reuse_buffer(*buf);
#endif
    return;
  }

#if UV_VERSION_MAJOR == 0
  connection->consume(buf.base, nread);
  connection->internal_reuse_buffer(buf);
#else
  connection->consume(buf->base, nread);
  connection->internal_reuse_buffer(*buf);
#endif
}

#if UV_VERSION_MAJOR == 0
uv_buf_t Connection::alloc_buffer_ssl(uv_handle_t* handle, size_t suggested_size) {
  Connection* connection = static_cast<Connection*>(handle->data);
  char* base = connection->ssl_session_->incoming().peek_writable(&suggested_size);
  return uv_buf_init(base, suggested_size);
}
#else
void Connection::alloc_buffer_ssl(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  Connection* connection = static_cast<Connection*>(handle->data);
  buf->base = connection->ssl_session_->incoming().peek_writable(&suggested_size);
  buf->len = suggested_size;
}
#endif

#if UV_VERSION_MAJOR == 0
void Connection::on_read_ssl(uv_stream_t* client, ssize_t nread, uv_buf_t buf) {
#else
void Connection::on_read_ssl(uv_stream_t* client, ssize_t nread, const uv_buf_t* buf) {
#endif
  Connection* connection = static_cast<Connection*>(client->data);

  SslSession* ssl_session = connection->ssl_session_.get();
  assert(ssl_session != NULL);

  if (nread < 0) {
#if UV_VERSION_MAJOR == 0
    if (uv_last_error(connection->loop_).code != UV_EOF) {
#else
    if (nread != UV_EOF) {
#endif
      LOG_ERROR("Read error '%s' on host %s",
                UV_ERRSTR(nread, connection->loop_),
                connection->addr_string_.c_str());
    }
    connection->defunct();
    return;
  }

  ssl_session->incoming().commit(nread);

  if (ssl_session->is_handshake_done()) {
    char buf[SSL_READ_SIZE];
    int rc =  0;
    while ((rc = ssl_session->decrypt(buf, sizeof(buf))) > 0) {
      connection->consume(buf, rc);
    }
    if (rc <= 0 && ssl_session->has_error()) {
      connection->notify_error("Unable to decrypt data: " + ssl_session->error_message(),
                               CONNECTION_ERROR_SSL);
    }
  } else {
    connection->ssl_handshake();
  }
}

void Connection::on_timeout(Timer* timer) {
  Handler* handler = static_cast<Handler*>(timer->data());
  Connection* connection = handler->connection();
  LOG_INFO("Request timed out to host %s", connection->addr_string_.c_str());
  // TODO (mpenick): We need to handle the case where we have too many
  // timeout requests and we run out of stream ids. The java-driver
  // uses a threshold to defunct the connection.
  handler->set_state(Handler::REQUEST_STATE_TIMEOUT);
  handler->on_timeout();

  connection->metrics_->request_timeouts.inc();
}

void Connection::on_connected() {
  write(new StartupHandler(this, new OptionsRequest()));
}

void Connection::on_authenticate() {
  if (protocol_version_ == 1) {
    send_credentials();
  } else {
    send_initial_auth_response();
  }
}

void Connection::on_auth_challenge(const AuthResponseRequest* request,
                                   const std::string& token) {
  AuthResponseRequest* auth_response
      = new AuthResponseRequest(request->auth()->evaluate_challenge(token),
                                request->auth());
  write(new StartupHandler(this, auth_response));
}

void Connection::on_auth_success(const AuthResponseRequest* request,
                                 const std::string& token) {
  request->auth()->on_authenticate_success(token);
  on_ready();
}

void Connection::on_ready() {
  if (state_ == CONNECTION_STATE_CONNECTED && listener_->event_types() != 0) {
    set_state(CONNECTION_STATE_REGISTERING_EVENTS);
    write(new StartupHandler(this, new RegisterRequest(listener_->event_types())));
    return;
  }

  if (keyspace_.empty()) {
    notify_ready();
  } else {
    write(new StartupHandler(this, new QueryRequest("USE \"" + keyspace_ + "\"")));
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

  write(new StartupHandler(this, new StartupRequest()));
}

void Connection::on_pending_schema_agreement(Timer* timer) {
  PendingSchemaAgreement* pending_schema_agreement
      = static_cast<PendingSchemaAgreement*>(timer->data());
  Connection* connection = pending_schema_agreement->handler->connection();
  connection->pending_schema_agreements_.remove(pending_schema_agreement);
  pending_schema_agreement->handler->execute();
  delete pending_schema_agreement;
}

void Connection::notify_ready() {
  connect_timer_.stop();
  restart_heartbeat_timer();
  set_state(CONNECTION_STATE_READY);
  listener_->on_ready(this);
}

void Connection::notify_error(const std::string& message, ConnectionError code) {
  error_message_ = message;
  error_code_ = code;

  switch (code) {
    case CONNECTION_OK:
      assert(false && "Notified error without an error");
      return;

    case CONNECTION_ERROR_INVALID_PROTOCOL:
      LOG_WARN("Host %s received invalid protocol response %s",
               addr_string_.c_str(), message.c_str());
      break;

    case CONNECTION_ERROR_SSL:
      ssl_error_code_ = ssl_session_->error_code();
      log_error(message);
      break;

    default:
      log_error(message);
      break;
  }

  defunct();
}

void Connection::log_error(const std::string& error) {
  if (state_ == CONNECTION_STATE_READY) {
    LOG_ERROR("Host %s had the following error: %s",
              addr_string_.c_str(), error.c_str());
  } else {
    LOG_ERROR("Host %s had the following error on startup: %s",
              addr_string_.c_str(), error.c_str());
  }
  defunct();
}

void Connection::ssl_handshake() {
  if (!ssl_session_->is_handshake_done()) {
    ssl_session_->do_handshake();
    if (ssl_session_->has_error()) {
      notify_error("Error during SSL handshake: " + ssl_session_->error_message(), CONNECTION_ERROR_SSL);
      return;
    }
  }

  char buf[SslHandshakeWriter::MAX_BUFFER_SIZE];
  size_t size = ssl_session_->outgoing().read(buf, sizeof(buf));
  if (size > 0) {
    if (!SslHandshakeWriter::write(this, buf, size)) {
      notify_error("Error writing data during SSL handshake");
      return;
    }
  }

  if (ssl_session_->is_handshake_done()) {
    ssl_session_->verify();
    if (ssl_session_->has_error()) {
      notify_error("Error verifying peer certificate: " + ssl_session_->error_message(), CONNECTION_ERROR_SSL);
      return;
    }
    on_connected();
  }
}

void Connection::send_credentials() {
  ScopedPtr<V1Authenticator> v1_auth(config_.auth_provider()->new_authenticator_v1(address_));
  if (v1_auth) {
    V1Authenticator::Credentials credentials;
    v1_auth->get_credentials(&credentials);
    write(new StartupHandler(this, new CredentialsRequest(credentials)));
  } else {
    send_initial_auth_response();
  }
}

void Connection::send_initial_auth_response() {
  SharedRefPtr<Authenticator> auth(
        config_.auth_provider()->new_authenticator(address_));
  if (!auth) {
    notify_error("Authentication required but no auth provider set", CONNECTION_ERROR_AUTH);
  } else {
    AuthResponseRequest* auth_response
        = new AuthResponseRequest(auth->initial_response(), auth);
    write(new StartupHandler(this, auth_response));
  }
}

void Connection::restart_heartbeat_timer() {
  if (config_.connection_heartbeat_interval_secs() > 0) {
    heartbeat_timer_.start(loop_,
                           1000 * config_.connection_heartbeat_interval_secs(),
                           this, on_heartbeat);
  }
}

void Connection::on_heartbeat(Timer* timer) {
  Connection* connection = static_cast<Connection*>(timer->data());

  if (connection->idle_start_time_ms_ == 0) {
    connection->idle_start_time_ms_ = get_time_since_epoch_ms();
  } else if ((get_time_since_epoch_ms() - connection->idle_start_time_ms_) / 1000 >
             connection->config().connection_idle_timeout_secs()){
    connection->notify_error("Failed to send a heartbeat within connection idle interval. "
                             "Terminating connection...",
                 CONNECTION_ERROR_TIMEOUT);
    return;
  }

  if (!connection->heartbeat_outstanding_) {
    if (!connection->internal_write(new HeartbeatHandler(connection), true, false)) {
      // Recycling only this connection with a timeout error. This is unlikely and
      // it means the connection ran out of stream IDs as a result of requests
      // that never returned and as a result timed out.
      connection->notify_error("No streams IDs available for heartbeat request. "
                               "Terminating connection...",
                               CONNECTION_ERROR_TIMEOUT);
      return;
    }
    connection->heartbeat_outstanding_ = true;
  }

  connection->restart_heartbeat_timer();
}

void Connection::PendingSchemaAgreement::stop_timer() {
  timer.stop();
}

Connection::PendingWriteBase::~PendingWriteBase() {
  cleanup_pending_handlers(&handlers_);
}

int32_t Connection::PendingWriteBase::write(Handler* handler) {
  size_t last_buffer_size = buffers_.size();
  int32_t request_size = handler->encode(connection_->protocol_version_, 0x00, &buffers_);
  if (request_size < 0) {
    buffers_.resize(last_buffer_size); // rollback
    return request_size;
  }

  size_ += request_size;
  handlers_.add_to_back(handler);

  return request_size;
}

void Connection::PendingWriteBase::on_write(uv_write_t* req, int status) {
  PendingWrite* pending_write = static_cast<PendingWrite*>(req->data);

  Connection* connection = static_cast<Connection*>(pending_write->connection_);

  while (!pending_write->handlers_.is_empty()) {
    Handler* handler = pending_write->handlers_.front();

    pending_write->handlers_.remove(handler);

    switch (handler->state()) {
      case Handler::REQUEST_STATE_WRITING:
        if (status == 0) {
          handler->set_state(Handler::REQUEST_STATE_READING);
          connection->pending_reads_.add_to_back(handler);
        } else {
          if (!connection->is_closing()) {
            connection->notify_error("Write error '" +
                                     std::string(UV_ERRSTR(status, connection->loop_)) +
                                     "'");
            connection->defunct();
          }

          connection->stream_manager_.release(handler->stream());
          handler->stop_timer();
          handler->set_state(Handler::REQUEST_STATE_DONE);
          handler->on_error(CASS_ERROR_LIB_WRITE_ERROR,
                            "Unable to write to socket");
          handler->dec_ref();
        }
        break;

      case Handler::REQUEST_STATE_TIMEOUT_WRITE_OUTSTANDING:
        // The read may still come back, handle cleanup there
        handler->set_state(Handler::REQUEST_STATE_TIMEOUT);
        connection->pending_reads_.add_to_back(handler);
        break;

      case Handler::REQUEST_STATE_READ_BEFORE_WRITE:
        // The read callback happened before the write callback
        // returned. This is now responsible for cleanup.
        handler->stop_timer();
        handler->set_state(Handler::REQUEST_STATE_DONE);
        handler->dec_ref();
        break;

      case Handler::REQUEST_STATE_RETRY_WRITE_OUTSTANDING:
        handler->stop_timer();
        handler->retry();
        handler->dec_ref();
        break;

      default:
        assert(false && "Invalid request state after write finished");
        break;
    }
  }

  connection->pending_writes_size_ -= pending_write->size();
  if (connection->pending_writes_size_ <
      connection->config_.write_bytes_low_water_mark() &&
      connection->state_ == CONNECTION_STATE_OVERWHELMED) {
    connection->set_state(CONNECTION_STATE_READY);
  }

  connection->pending_writes_.remove(pending_write);
  delete pending_write;

  connection->flush();
}

void Connection::PendingWrite::flush() {
  if (!is_flushed_ && !buffers_.empty()) {
    UvBufVec bufs;

    bufs.reserve(buffers_.size());

    for (BufferVec::const_iterator it = buffers_.begin(),
         end = buffers_.end(); it != end; ++it) {
      bufs.push_back(uv_buf_init(const_cast<char*>(it->data()), it->size()));
    }

    is_flushed_ = true;
    uv_stream_t* sock_stream = copy_cast<uv_tcp_t*, uv_stream_t*>(&connection_->socket_);
    uv_write(&req_, sock_stream, bufs.data(), bufs.size(), PendingWrite::on_write);
  }
}

void Connection::PendingWriteSsl::encrypt() {
  char buf[SSL_WRITE_SIZE];

  size_t copied = 0;
  size_t offset = 0;
  size_t total = 0;

  SslSession* ssl_session = connection_->ssl_session_.get();

  BufferVec::const_iterator it = buffers_.begin(),
      end = buffers_.end();

  LOG_TRACE("Copying %u bufs", static_cast<unsigned int>(buffers_.size()));

  bool is_done = (it == end);

  while (!is_done) {
    assert(it->size() > 0);
    size_t size = it->size();

    size_t to_copy = size - offset;
    size_t available = SSL_WRITE_SIZE - copied;
    if (available < to_copy) {
      to_copy = available;
    }

    memcpy(buf + copied, it->data() + offset, to_copy);

    copied += to_copy;
    offset += to_copy;
    total += to_copy;

    if (offset == size) {
      ++it;
      offset = 0;
    }

    is_done = (it == end);

    if (is_done || copied == SSL_WRITE_SIZE) {
      int rc = ssl_session->encrypt(buf, copied);
      if (rc <= 0 && ssl_session->has_error()) {
        connection_->notify_error("Unable to encrypt data: " + ssl_session->error_message(), CONNECTION_ERROR_SSL);
        return;
      }
      copied = 0;
    }
  }

  LOG_TRACE("Copied %u bytes for encryption", static_cast<unsigned int>(total));
}

void Connection::PendingWriteSsl::flush() {
  if (!is_flushed_ && !buffers_.empty()) {
    SslSession* ssl_session = connection_->ssl_session_.get();

    rb::RingBuffer::Position prev_pos = ssl_session->outgoing().write_position();

    encrypt();

    FixedVector<uv_buf_t, SSL_ENCRYPTED_BUFS_COUNT> bufs;
    encrypted_size_ = ssl_session->outgoing().peek_multiple(prev_pos, &bufs);

    LOG_TRACE("Sending %u encrypted bytes", static_cast<unsigned int>(encrypted_size_));

    uv_stream_t* sock_stream = copy_cast<uv_tcp_t*, uv_stream_t*>(&connection_->socket_);
    uv_write(&req_, sock_stream, bufs.data(), bufs.size(), PendingWriteSsl::on_write);

    is_flushed_ = true;
  }
}

void Connection::PendingWriteSsl::on_write(uv_write_t* req, int status) {
  if (status == 0) {
    PendingWriteSsl* pending_write = static_cast<PendingWriteSsl*>(req->data);
    pending_write->connection_->ssl_session_->outgoing().read(NULL, pending_write->encrypted_size_);
  }
  PendingWriteBase::on_write(req, status);
}

bool Connection::SslHandshakeWriter::write(Connection* connection, char* buf, size_t buf_size) {
  SslHandshakeWriter* writer = new SslHandshakeWriter(connection, buf, buf_size);
  uv_stream_t* stream = copy_cast<uv_tcp_t*, uv_stream_t*>(&connection->socket_);

  int rc = uv_write(&writer->req_, stream, &writer->uv_buf_, 1, SslHandshakeWriter::on_write);
  if (rc != 0) {
    delete writer;
    return false;
  }

  return true;
}

Connection::SslHandshakeWriter::SslHandshakeWriter(Connection* connection, char* buf, size_t buf_size)
  : connection_(connection)
  , uv_buf_(uv_buf_init(buf, buf_size)) {
  memcpy(buf_, buf, buf_size);
  req_.data = this;
}

void Connection::SslHandshakeWriter::on_write(uv_write_t* req, int status) {
  SslHandshakeWriter* writer = static_cast<SslHandshakeWriter*>(req->data);
  if (status != 0) {
    writer->connection_->notify_error("Write error '" +
                                      std::string(UV_ERRSTR(status, writer->connection_->loop_)) +
                                      "'");
  }
  delete writer;
}

} // namespace cass
