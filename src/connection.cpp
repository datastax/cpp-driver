/*
  Copyright (c) DataStax, Inc.

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
#include "cassconfig.hpp"
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

#ifdef HAVE_NOSIGPIPE
#include <sys/socket.h>
#include <sys/types.h>
#endif

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

static void cleanup_pending_callbacks(List<RequestCallback>* pending) {
  while (!pending->is_empty()) {
    RequestCallback::Ptr callback(pending->front());

    pending->remove(callback.get());

    switch (callback->state()) {
      case RequestCallback::REQUEST_STATE_NEW:
      case RequestCallback::REQUEST_STATE_FINISHED:
      case RequestCallback::REQUEST_STATE_CANCELLED:
        assert(false && "Request state is invalid in cleanup");
        break;

      case RequestCallback::REQUEST_STATE_READ_BEFORE_WRITE:
        callback->set_state(RequestCallback::REQUEST_STATE_FINISHED);
        // Use the response saved in the read callback
        callback->on_set(callback->read_before_write_response());
        break;

      case RequestCallback::REQUEST_STATE_WRITING:
      case RequestCallback::REQUEST_STATE_READING:
        callback->set_state(RequestCallback::REQUEST_STATE_FINISHED);
        if (callback->request()->is_idempotent()) {
          callback->on_retry_next_host();
        } else {
          callback->on_error(CASS_ERROR_LIB_REQUEST_TIMED_OUT,
                             "Request timed out");
        }
        break;

      case RequestCallback::REQUEST_STATE_CANCELLED_WRITING:
      case RequestCallback::REQUEST_STATE_CANCELLED_READING:
      case RequestCallback::REQUEST_STATE_CANCELLED_READ_BEFORE_WRITE:
        callback->set_state(RequestCallback::REQUEST_STATE_CANCELLED);
        callback->on_cancel();
        break;
    }

    callback->dec_ref();
  }
}

Connection::StartupCallback::StartupCallback(const Request::ConstPtr& request)
  : SimpleRequestCallback(request) { }

void Connection::StartupCallback::on_internal_set(ResponseMessage* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_SUPPORTED:
      connection()->on_supported(response);
      break;

    case CQL_OPCODE_ERROR: {
      ErrorResponse* error
          = static_cast<ErrorResponse*>(response->response_body().get());
      ConnectionError error_code = CONNECTION_ERROR_GENERIC;
      if (error->code() == CQL_ERROR_PROTOCOL_ERROR &&
          error->message().find("Invalid or unsupported protocol version") != StringRef::npos) {
        error_code = CONNECTION_ERROR_INVALID_PROTOCOL;
      } else if (error->code() == CQL_ERROR_BAD_CREDENTIALS) {
        error_code = CONNECTION_ERROR_AUTH;
      } else if (error->code() == CQL_ERROR_INVALID_QUERY &&
                 error->message().find("Keyspace") == 0 &&
                 error->message().find("does not exist") != StringRef::npos) {
        error_code = CONNECTION_ERROR_KEYSPACE;
      }
      connection()->notify_error("Received error response " + error->error_message(), error_code);
      break;
    }

    case CQL_OPCODE_AUTHENTICATE: {
      AuthenticateResponse* auth
          = static_cast<AuthenticateResponse*>(response->response_body().get());
      connection()->on_authenticate(auth->class_name());
      break;
    }

    case CQL_OPCODE_AUTH_CHALLENGE:
      connection()->on_auth_challenge(
            static_cast<const AuthResponseRequest*>(request()),
            static_cast<AuthChallengeResponse*>(response->response_body().get())->token());
      break;

    case CQL_OPCODE_AUTH_SUCCESS:
      connection()->on_auth_success(
            static_cast<const AuthResponseRequest*>(request()),
            static_cast<AuthSuccessResponse*>(response->response_body().get())->token());
      break;

    case CQL_OPCODE_READY:
      connection()->on_ready();
      break;

    case CQL_OPCODE_RESULT:
      on_result_response(response);
      break;

    default:
      connection()->notify_error("Invalid opcode");
      break;
  }
}

void Connection::StartupCallback::on_internal_error(CassError code,
                                                    const std::string& message) {
  std::ostringstream ss;
  ss << "Error: '" << message
     << "' (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0') << code << ")";
  connection()->notify_error(ss.str());
}

void Connection::StartupCallback::on_internal_timeout() {
  if (!connection()->is_closing()) {
    connection()->notify_error("Timed out", CONNECTION_ERROR_TIMEOUT);
  }
}

void Connection::StartupCallback::on_result_response(ResponseMessage* response) {
  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());
  switch (result->kind()) {
    case CASS_RESULT_KIND_SET_KEYSPACE:
      connection()->on_set_keyspace();
      break;
    default:
      connection()->notify_error("Invalid result response. Expected set keyspace.");
      break;
  }
}

Connection::HeartbeatCallback::HeartbeatCallback()
  : SimpleRequestCallback(Request::ConstPtr(new OptionsRequest())) { }

void Connection::HeartbeatCallback::on_internal_set(ResponseMessage* response) {
  LOG_TRACE("Heartbeat completed on host %s",
            connection()->address_string().c_str());
  connection()->heartbeat_outstanding_ = false;
}

void Connection::HeartbeatCallback::on_internal_error(CassError code, const std::string& message) {
  LOG_WARN("An error occurred on host %s during a heartbeat request: %s",
           connection()->address_string().c_str(),
           message.c_str());
  connection()->heartbeat_outstanding_ = false;
}

void Connection::HeartbeatCallback::on_internal_timeout() {
  LOG_WARN("Heartbeat request timed out on host %s",
           connection()->address_string().c_str());
  connection()->heartbeat_outstanding_ = false;
}

Connection::Connection(uv_loop_t* loop,
                       const Config& config,
                       Metrics* metrics,
                       const Host::ConstPtr& host,
                       const std::string& keyspace,
                       int protocol_version,
                       Listener* listener)
    : state_(CONNECTION_STATE_NEW)
    , error_code_(CONNECTION_OK)
    , ssl_error_code_(CASS_OK)
    , loop_(loop)
    , config_(config)
    , metrics_(metrics)
    , host_(host)
    , keyspace_(keyspace)
    , protocol_version_(protocol_version)
    , listener_(listener)
    , response_(new ResponseMessage())
    , stream_manager_(protocol_version)
    , ssl_session_(NULL)
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
    ssl_session_.reset(ssl_context->create_session(host));
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
    Connector::connect(&socket_, host_->address(), this, on_connect);
  }
}

bool Connection::write(const RequestCallback::Ptr& callback, bool flush_immediately) {
  int32_t result = internal_write(callback, flush_immediately);
  if (result > 0) {
    restart_heartbeat_timer();
  }
  return result != Request::REQUEST_ERROR_NO_AVAILABLE_STREAM_IDS;
}

int32_t Connection::internal_write(const RequestCallback::Ptr& callback, bool flush_immediately) {
  if (callback->state() == RequestCallback::REQUEST_STATE_CANCELLED) {
    return Request::REQUEST_ERROR_CANCELLED;
  }

  int stream = stream_manager_.acquire(callback.get());
  if (stream < 0) {
    return Request::REQUEST_ERROR_NO_AVAILABLE_STREAM_IDS;
  }

  callback->inc_ref(); // Connection reference
  callback->start(this, stream);

  if (pending_writes_.is_empty() || pending_writes_.back()->is_flushed()) {
    if (ssl_session_) {
      pending_writes_.add_to_back(new PendingWriteSsl(this));
    } else {
      pending_writes_.add_to_back(new PendingWrite(this));
    }
  }

  PendingWriteBase *pending_write = pending_writes_.back();

  int32_t request_size = pending_write->write(callback.get());
  if (request_size < 0) {
    stream_manager_.release(stream);

    switch (request_size) {
      case Request::REQUEST_ERROR_BATCH_WITH_NAMED_VALUES:
      case Request::REQUEST_ERROR_PARAMETER_UNSET:
        // Already handled
        break;

      default:
        callback->on_error(CASS_ERROR_LIB_MESSAGE_ENCODE,
                           "Operation unsupported by this protocol version");
        break;
    }

    callback->dec_ref();
    return request_size;
  }

  LOG_TRACE("Sending message type %s with stream %d on host %s",
            opcode_to_string(callback->request()->opcode()).c_str(),
            stream,
            address_string().c_str());

  callback->set_state(RequestCallback::REQUEST_STATE_WRITING);
  if (flush_immediately) {
    pending_write->flush();
  }

  return request_size;
}

void Connection::flush() {
  if (pending_writes_.is_empty()) return;

  pending_writes_.back()->flush();
}

void Connection::schedule_schema_agreement(const SchemaChangeCallback::Ptr& callback, uint64_t wait) {
  PendingSchemaAgreement* pending_schema_agreement = new PendingSchemaAgreement(callback);
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
    uv_handle_t* handle = reinterpret_cast<uv_handle_t*>(&socket_);
    if (!uv_is_closing(handle)) {
      heartbeat_timer_.stop();
      terminate_timer_.stop();
      connect_timer_.stop();
      set_state(close_state);
      uv_close(handle, on_close);
    }
  }
}

void Connection::set_state(ConnectionState new_state) {
  // Only update if the state changed
  if (new_state == state_) return;

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
      assert((new_state == CONNECTION_STATE_CLOSE ||
              new_state == CONNECTION_STATE_CLOSE_DEFUNCT) &&
             "Invalid connection state after ready");
      state_ = new_state;
      break;

    case CONNECTION_STATE_CLOSE:
      assert(false && "No state change after close");
      break;

    case CONNECTION_STATE_CLOSE_DEFUNCT:
      assert(false && "No state change after close defunct");
      break;
  }
}

void Connection::consume(char* input, size_t size) {
  char* buffer = input;
  size_t remaining = size;

  // A successful read means the connection is still responsive
  restart_terminate_timer();

  while (remaining != 0 && !is_closing()) {
    ssize_t consumed = response_->decode(buffer, remaining);
    if (consumed <= 0) {
      notify_error("Error consuming message");
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
                host_->address_string().c_str());

      if (response->stream() < 0) {
        if (response->opcode() == CQL_OPCODE_EVENT) {
          listener_->on_event(static_cast<EventResponse*>(response->response_body().get()));
        } else {
          notify_error("Invalid response opcode for event stream: " +
                       opcode_to_string(response->opcode()));
          continue;
        }
      } else {
        RequestCallback* temp = NULL;

        if (stream_manager_.get_pending_and_release(response->stream(), temp)) {
          RequestCallback::Ptr callback(temp);

          switch (callback->state()) {
            case RequestCallback::REQUEST_STATE_READING:
              pending_reads_.remove(callback.get());
              callback->set_state(RequestCallback::REQUEST_STATE_FINISHED);
              maybe_set_keyspace(response.get());
              callback->on_set(response.get());
              callback->dec_ref();
              break;

            case RequestCallback::REQUEST_STATE_WRITING:
              // There are cases when the read callback will happen
              // before the write callback. If this happens we have
              // to allow the write callback to finish the request.
              callback->set_state(RequestCallback::REQUEST_STATE_READ_BEFORE_WRITE);
              // Save the response for the write callback
              callback->set_read_before_write_response(response.release()); // Transfer ownership
              break;

            case RequestCallback::REQUEST_STATE_CANCELLED_READING:
              pending_reads_.remove(callback.get());
              callback->set_state(RequestCallback::REQUEST_STATE_CANCELLED);
              callback->on_cancel();
              callback->dec_ref();
              break;

            case RequestCallback::REQUEST_STATE_CANCELLED_WRITING:
              // There are cases when the read callback will happen
              // before the write callback. If this happens we have
              // to allow the write callback to finish the request.
              callback->set_state(RequestCallback::REQUEST_STATE_CANCELLED_READ_BEFORE_WRITE);
              break;

            default:
              assert(false && "Invalid request state after receiving response");
              break;
          }
        } else {
          notify_error("Invalid stream ID");
          continue;
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
    LOG_DEBUG("Connected to host %s on connection(%p)",
              connection->host_->address_string().c_str(),
              static_cast<void*>(connection));

#if defined(HAVE_NOSIGPIPE) && UV_VERSION_MAJOR >= 1
    // This must be done after connection for the socket file descriptor to be
    // valid.
    uv_os_fd_t fd = 0;
    int enabled = 1;
    if (uv_fileno(reinterpret_cast<uv_handle_t*>(&connection->socket_), &fd) != 0 ||
        setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, (void *)&enabled, sizeof(int)) != 0) {
      LOG_WARN("Unable to set socket option SO_NOSIGPIPE for host %s",
               connection->host_->address_string().c_str());
    }
#endif

    if (connection->ssl_session_) {
      uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->socket_),
                    Connection::alloc_buffer_ssl, Connection::on_read_ssl);
    } else {
      uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->socket_),
                    Connection::alloc_buffer, Connection::on_read);
    }

    connection->set_state(CONNECTION_STATE_CONNECTED);

    if (connection->ssl_session_) {
      connection->ssl_handshake();
    } else {
      connection->on_connected();
    }
  } else {
    connection->notify_error("Connect error '" +
                             std::string(UV_ERRSTR(connector->status(), connection->loop_)) +
                             "'");
  }
}

void Connection::on_connect_timeout(Timer* timer) {
  Connection* connection = static_cast<Connection*>(timer->data());
  connection->notify_error("Connection timeout", CONNECTION_ERROR_TIMEOUT);
  connection->metrics_->connection_timeouts.inc();
}

void Connection::on_close(uv_handle_t* handle) {
  Connection* connection = static_cast<Connection*>(handle->data);

  LOG_DEBUG("Connection(%p) to host %s closed",
            static_cast<void*>(connection),
            connection->host_->address_string().c_str());

  cleanup_pending_callbacks(&connection->pending_reads_);

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
    pending_schema_aggreement->callback->on_closing();
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
      connection->notify_error("Read error '" +
                               std::string(UV_ERRSTR(nread, connection->loop_)) +
                               "'");
    } else {
      connection->defunct();
    }

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
      connection->notify_error("Read error '" +
                               std::string(UV_ERRSTR(nread, connection->loop_)) +
                               "'");
    } else {
      connection->defunct();
    }
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
                               CONNECTION_ERROR_SSL_DECRYPT);
    }
  } else {
    connection->ssl_handshake();
  }
}

void Connection::on_connected() {
  internal_write(RequestCallback::Ptr(
                   new StartupCallback(Request::ConstPtr(
                                         new OptionsRequest()))));
}

void Connection::on_authenticate(const std::string& class_name) {
  if (protocol_version_ == 1) {
    send_credentials(class_name);
  } else {
    send_initial_auth_response(class_name);
  }
}

void Connection::on_auth_challenge(const AuthResponseRequest* request,
                                   const std::string& token) {
  std::string response;
  if (!request->auth()->evaluate_challenge(token, &response)) {
    notify_error("Failed evaluating challenge token: " + request->auth()->error(), CONNECTION_ERROR_AUTH);
    return;
  }
  internal_write(RequestCallback::Ptr(
                   new StartupCallback(Request::ConstPtr(
                                         new AuthResponseRequest(response, request->auth())))));
}

void Connection::on_auth_success(const AuthResponseRequest* request,
                                 const std::string& token) {
  if (!request->auth()->success(token)) {
    notify_error("Failed evaluating success token: " + request->auth()->error(), CONNECTION_ERROR_AUTH);
    return;
  }
  on_ready();
}

void Connection::on_ready() {
  if (state_ == CONNECTION_STATE_CONNECTED && listener_->event_types() != 0) {
    set_state(CONNECTION_STATE_REGISTERING_EVENTS);
    internal_write(RequestCallback::Ptr(
                     new StartupCallback(Request::ConstPtr(
                                           new RegisterRequest(listener_->event_types())))));
    return;
  }

  if (keyspace_.empty()) {
    notify_ready();
  } else {
    internal_write(RequestCallback::Ptr(
                     new StartupCallback(Request::ConstPtr(
                                           new QueryRequest("USE \"" + keyspace_ + "\"")))));
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

  internal_write(RequestCallback::Ptr(
                   new StartupCallback(Request::ConstPtr(
                                         new StartupRequest(config().no_compact())))));
}

void Connection::on_pending_schema_agreement(Timer* timer) {
  PendingSchemaAgreement* pending_schema_agreement
      = static_cast<PendingSchemaAgreement*>(timer->data());
  Connection* connection = pending_schema_agreement->callback->connection();
  connection->pending_schema_agreements_.remove(pending_schema_agreement);
  pending_schema_agreement->callback->execute();
  delete pending_schema_agreement;
}

void Connection::notify_ready() {
  connect_timer_.stop();
  restart_heartbeat_timer();
  restart_terminate_timer();
  set_state(CONNECTION_STATE_READY);
  listener_->on_ready(this);
}

void Connection::notify_error(const std::string& message, ConnectionError code) {
  assert(code != CONNECTION_OK && "Notified error without an error");
  LOG_DEBUG("Lost connection(%p) to host %s with the following error: %s",
            static_cast<void*>(this),
            host_->address_string().c_str(),
            message.c_str());
  error_message_ = message;
  error_code_ = code;
  if (is_ssl_error()) {
    ssl_error_code_ = ssl_session_->error_code();
  }
  defunct();
}

void Connection::ssl_handshake() {
  if (!ssl_session_->is_handshake_done()) {
    ssl_session_->do_handshake();
    if (ssl_session_->has_error()) {
      notify_error("Error during SSL handshake: " + ssl_session_->error_message(),
                   CONNECTION_ERROR_SSL_HANDSHAKE);
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
      notify_error("Error verifying peer certificate: " + ssl_session_->error_message(),
                   CONNECTION_ERROR_SSL_VERIFY);
      return;
    }
    on_connected();
  }
}

void Connection::send_credentials(const std::string& class_name) {
  ScopedPtr<V1Authenticator> v1_auth(config_.auth_provider()->new_authenticator_v1(host_, class_name));
  if (v1_auth) {
    V1Authenticator::Credentials credentials;
    v1_auth->get_credentials(&credentials);
    internal_write(RequestCallback::Ptr(
                     new StartupCallback(Request::ConstPtr(
                                           new CredentialsRequest(credentials)))));
  } else {
    send_initial_auth_response(class_name);
  }
}

void Connection::send_initial_auth_response(const std::string& class_name) {
  Authenticator::Ptr auth(config_.auth_provider()->new_authenticator(host_, class_name));
  if (!auth) {
    notify_error("Authentication required but no auth provider set", CONNECTION_ERROR_AUTH);
  } else {
    std::string response;
    if (!auth->initial_response(&response)) {
      notify_error("Failed creating initial response token: " + auth->error(), CONNECTION_ERROR_AUTH);
      return;
    }
    internal_write(RequestCallback::Ptr(
                     new StartupCallback(Request::ConstPtr(
                                           new AuthResponseRequest(response, auth)))));
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

  if (!connection->heartbeat_outstanding_) {
    if (!connection->internal_write(RequestCallback::Ptr(new HeartbeatCallback()))) {
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

void Connection::restart_terminate_timer() {
  // The terminate timer shouldn't be started without having heartbeats enabled,
  // otherwise connections would be terminated in periods of request inactivity.
  if (config_.connection_heartbeat_interval_secs() > 0 &&
      config_.connection_idle_timeout_secs() > 0) {
    terminate_timer_.start(loop_,
                      1000 * config_.connection_idle_timeout_secs(),
                      this, on_terminate);
  }
}

void Connection::on_terminate(Timer* timer) {
  Connection* connection = static_cast<Connection*>(timer->data());
  connection->notify_error("Failed to send a heartbeat within connection idle interval. "
                           "Terminating connection...",
                           CONNECTION_ERROR_TIMEOUT);
}

void Connection::PendingSchemaAgreement::stop_timer() {
  timer.stop();
}

Connection::PendingWriteBase::~PendingWriteBase() {
  cleanup_pending_callbacks(&callbacks_);
}

int32_t Connection::PendingWriteBase::write(RequestCallback* callback) {
  size_t last_buffer_size = buffers_.size();
  int32_t request_size = callback->encode(connection_->protocol_version_, 0x00, &buffers_);
  if (request_size < 0) {
    buffers_.resize(last_buffer_size); // rollback
    return request_size;
  }

  size_ += request_size;
  callbacks_.add_to_back(callback);

  return request_size;
}

void Connection::PendingWriteBase::on_write(uv_write_t* req, int status) {
  PendingWrite* pending_write = static_cast<PendingWrite*>(req->data);

  Connection* connection = static_cast<Connection*>(pending_write->connection_);

  while (!pending_write->callbacks_.is_empty()) {
    RequestCallback::Ptr callback(pending_write->callbacks_.front());

    pending_write->callbacks_.remove(callback.get());

    switch (callback->state()) {
      case RequestCallback::REQUEST_STATE_WRITING:
        if (status == 0) {
          callback->set_state(RequestCallback::REQUEST_STATE_READING);
          connection->pending_reads_.add_to_back(callback.get());
        } else {
          if (!connection->is_closing()) {
            connection->notify_error("Write error '" +
                                     std::string(UV_ERRSTR(status, connection->loop_)) +
                                     "'");
            connection->defunct();
          }

          connection->stream_manager_.release(callback->stream());
          callback->set_state(RequestCallback::REQUEST_STATE_FINISHED);
          callback->on_error(CASS_ERROR_LIB_WRITE_ERROR,
                             "Unable to write to socket");
          callback->dec_ref();
        }
        break;

      case RequestCallback::REQUEST_STATE_READ_BEFORE_WRITE:
        // The read callback happened before the write callback
        // returned. This is now responsible for finishing the request.
        callback->set_state(RequestCallback::REQUEST_STATE_FINISHED);
        // Use the response saved in the read callback
        connection->maybe_set_keyspace(callback->read_before_write_response());
        callback->on_set(callback->read_before_write_response());
        callback->dec_ref();
        break;

      case RequestCallback::REQUEST_STATE_CANCELLED_WRITING:
        callback->set_state(RequestCallback::REQUEST_STATE_CANCELLED_READING);
        connection->pending_reads_.add_to_back(callback.get());
        break;

      case RequestCallback::REQUEST_STATE_CANCELLED_READ_BEFORE_WRITE:
        // The read callback happened before the write callback
        // returned. This is now responsible for cleanup.
        callback->set_state(RequestCallback::REQUEST_STATE_CANCELLED);
        callback->on_cancel();
        callback->dec_ref();
        break;

      default:
        assert(false && "Invalid request state after write finished");
        break;
    }
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
    uv_stream_t* sock_stream = reinterpret_cast<uv_stream_t*>(&connection_->socket_);
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
        connection_->notify_error("Unable to encrypt data: " + ssl_session->error_message(),
                                  CONNECTION_ERROR_SSL_ENCRYPT);
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

    SmallVector<uv_buf_t, SSL_ENCRYPTED_BUFS_COUNT> bufs;
    encrypted_size_ = ssl_session->outgoing().peek_multiple(prev_pos, &bufs);

    LOG_TRACE("Sending %u encrypted bytes", static_cast<unsigned int>(encrypted_size_));

    uv_stream_t* sock_stream = reinterpret_cast<uv_stream_t*>(&connection_->socket_);
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
  uv_stream_t* stream = reinterpret_cast<uv_stream_t*>(&connection->socket_);

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
