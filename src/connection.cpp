#include "connection.hpp"
#include "auth.hpp"
#include "auth_requests.hpp"
#include "auth_responses.hpp"
#include "constants.hpp"
#include "response.hpp"
#include "connecter.hpp"
#include "timer.hpp"
#include "config.hpp"
#include "result_response.hpp"
#include "supported_response.hpp"
#include "startup_request.hpp"
#include "query_request.hpp"
#include "options_request.hpp"
#include "error_response.hpp"
#include "logger.hpp"
#include "cassandra.h"

#include <sstream>
#include <iomanip>

namespace cass {

Connection::StartupHandler::StartupHandler(Connection* connection,
                                           Request* request)
    : connection_(connection)
    , request_(request) {
}

const Request* Connection::StartupHandler::request() const {
  return request_.get();
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
          error->message().find("Invalid or unsupported protocol version") != std::string::npos) {
        connection_->is_invalid_protocol_ = true;
        connection_->logger_->warn(
              "Protocol version %d unsupported. Trying protocol version %d...",
              connection_->protocol_version_, connection_->protocol_version_ - 1);
      } else {
        std::ostringstream ss;
        ss << "Error response during startup: '" << error->message()
           << "' (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0')
           << CASS_ERROR(CASS_ERROR_SOURCE_SERVER, error->code()) << ")";
        connection_->notify_error(ss.str());
      }
    }
      break;

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
      connection_->notify_error("Invalid opcode during startup");
      break;
  }
}

void Connection::StartupHandler::on_error(CassError code,
                                          const std::string& message) {
  std::ostringstream ss;
  ss << "Error during startup: '" << message
     << "' (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0') << code << ")";
  connection_->notify_error(ss.str());
}

void Connection::StartupHandler::on_timeout() {
  if (!connection_->is_closing()) {
    connection_->notify_error("Timed out during startup");
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
          "Invalid result during startup. Expected set keyspace.");
      break;
  }
}

Connection::InternalRequest::InternalRequest(Connection* connection)
    : connection(connection)
    , stream_(0)
    , timer_(NULL)
    , state_(REQUEST_STATE_NEW) {
}

void Connection::InternalRequest::on_set(ResponseMessage* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_RESULT:
      on_result_response(response);
      break;
  }
  response_callback_->on_set(response);
}

void Connection::InternalRequest::on_error(CassError code, const std::string& message) {
  response_callback_->on_error(code, message);
  connection->stream_manager_.release_stream(stream_);
}

void Connection::InternalRequest::on_timeout() {
  response_callback_->on_timeout();
}

void Connection::InternalRequest::change_state(Connection::InternalRequest::State next_state) {
  switch (state_) {
    case REQUEST_STATE_NEW:
      assert(next_state == REQUEST_STATE_WRITING &&
             "Invalid request state after new");
      state_ = REQUEST_STATE_WRITING;
      timer_ =
          Timer::start(connection->loop_, connection->config_.write_timeout(),
                       this, on_request_timeout);
      break;

    case REQUEST_STATE_WRITING:
      if (next_state == REQUEST_STATE_READING) { // Success
        stop_timer();
        state_ = next_state;
        timer_ =
            Timer::start(connection->loop_, connection->config_.read_timeout(),
                         this, on_request_timeout);
      } else if (next_state == REQUEST_STATE_READ_BEFORE_WRITE ||
                 next_state == REQUEST_STATE_DONE) {
        stop_timer();
        state_ = next_state;
      } else if (next_state == REQUEST_STATE_WRITE_TIMEOUT) {
        connection->timed_out_request_count_++;
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after writing");
      }
      break;

    case REQUEST_STATE_READING:
      if (next_state == REQUEST_STATE_DONE) { // Success
        stop_timer();
        state_ = next_state;
      } else if (next_state == REQUEST_STATE_READ_TIMEOUT) {
        connection->timed_out_request_count_++;
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after reading");
      }
      break;

    case REQUEST_STATE_WRITE_TIMEOUT:
      assert((next_state == REQUEST_STATE_WRITE_TIMEOUT_BEFORE_READ ||
              next_state == REQUEST_STATE_READ_BEFORE_WRITE) &&
             "Invalid request state after write timeout");
      state_ = next_state;
      break;

    case REQUEST_STATE_READ_TIMEOUT:
      assert(next_state == REQUEST_STATE_DONE &&
             "Invalid request state after read timeout");
      connection->timed_out_request_count_--;
      state_ = next_state;
      break;

    case REQUEST_STATE_READ_BEFORE_WRITE:
      assert(next_state == REQUEST_STATE_DONE &&
             "Invalid request state after read before write");
      state_ = next_state;
      break;

    case REQUEST_STATE_WRITE_TIMEOUT_BEFORE_READ:
      assert(next_state == REQUEST_STATE_DONE &&
             "Invalid request state after write timeout before read");
      connection->timed_out_request_count_--;
      state_ = next_state;
      break;

    case REQUEST_STATE_DONE:
      assert(false && "Invalid request state after done");
      break;

    default:
      assert(false && "Invalid request state");
      break;
  }
}

void Connection::InternalRequest::stop_timer() {
  assert(timer_ != NULL);
  Timer::stop(timer_);
  timer_ = NULL;
}

void Connection::InternalRequest::on_result_response(ResponseMessage* response) {
  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());
  switch (result->kind()) {
    case CASS_RESULT_KIND_SET_KEYSPACE:
      connection->keyspace_ = result->keyspace();
      break;
  }
}

void Connection::InternalRequest::on_request_timeout(Timer* timer) {
  InternalRequest* request = static_cast<InternalRequest*>(timer->data());
  request->connection->logger_->info("Request timed out to '%s'",
                                     request->connection->host_string_.c_str());
  request->timer_ = NULL;
  if (request->state_ == REQUEST_STATE_READING) {
    request->change_state(REQUEST_STATE_READ_TIMEOUT);
  } else if (request->state_ == REQUEST_STATE_WRITING) {
    request->change_state(REQUEST_STATE_WRITE_TIMEOUT);
  } else {
    assert(false && "Invalid request state for timeout");
  }
  request->on_timeout();
}

Connection::Connection(uv_loop_t* loop,
                       const Host& host, Logger* logger, const Config& config,
                       const std::string& keyspace, int protocol_version)
    : state_(CONNECTION_STATE_NEW)
    , is_defunct_(false)
    , is_invalid_protocol_(false)
    , timed_out_request_count_(0)
    , loop_(loop)
    , response_(new ResponseMessage())
    , host_(host)
    , host_string_(host.address.to_string())
    , ssl_handshake_done_(false)
    , version_("3.0.0")
    , protocol_version_(protocol_version)
    , logger_(logger)
    , config_(config)
    , keyspace_(keyspace)
    , connect_timer_(NULL) {
  socket_.data = this;
  uv_tcp_init(loop_, &socket_);
}

void Connection::connect() {
  if (state_ == CONNECTION_STATE_NEW) {
    state_ = CONNECTION_STATE_CONNECTING;
    connect_timer_ = Timer::start(loop_, config_.connect_timeout(), this,
                                  on_connect_timeout);
    Connecter::connect(&socket_, host_.address, this, on_connect);
  }
}

bool Connection::execute(ResponseCallback* response_callback) {
  ScopedPtr<InternalRequest> internal_request(new InternalRequest(this));

  const Request* request = response_callback->request();

  int8_t stream = stream_manager_.acquire_stream(internal_request.get());
  if (stream < 0) {
    return false;
  }

  internal_request->set_stream(stream);
  internal_request->set_response_callback(response_callback);

  ScopedPtr<BufferVec> bufs(request->encode(protocol_version_, 0x00, stream));
  if (!bufs) {
    internal_request->on_error(CASS_ERROR_LIB_MESSAGE_ENCODE,
                               "Operation unsupported by this protocol version");
    return true; // Don't retry
  }

  logger_->debug("Sending message type %s with %d",
                 opcode_to_string(request->opcode()).c_str(), stream);

  pending_requests_.add_to_back(internal_request.get());

  internal_request->change_state(InternalRequest::REQUEST_STATE_WRITING);
  write(bufs.release(), internal_request.release());

  return true;
}

void Connection::close() {
  if (state_ != CONNECTION_STATE_CLOSING) {
    if (!uv_is_closing(reinterpret_cast<uv_handle_t*>(&socket_))) {
      if (state_ == CONNECTION_STATE_CONNECTED ||
          state_ == CONNECTION_STATE_READY) {
        uv_read_stop(reinterpret_cast<uv_stream_t*>(&socket_));
      }
      state_ = CONNECTION_STATE_CLOSING;
      uv_close(reinterpret_cast<uv_handle_t*>(&socket_), on_close);
    }
  }
}

void Connection::defunct() {
  is_defunct_ = true;
  close();
}

void Connection::write(BufferVec* bufs, Connection::InternalRequest* request) {
  uv_stream_t* stream = reinterpret_cast<uv_stream_t*>(&socket_);
  Writer::write(stream, bufs, request, on_write);
}

void Connection::consume(char* input, size_t size) {
  char* buffer = input;
  int remaining = size;

  while (remaining != 0) {
    int consumed = response_->decode(protocol_version_, buffer, remaining);
    if (consumed < 0) {
      // TODO(mstump) probably means connection closed/failed
      // Can this even happen right now?
      logger_->error("Error consuming message on '%s'", host_string_.c_str());
    }

    if (response_->is_body_ready()) {
      ScopedPtr<ResponseMessage> response(response_.release());
      response_.reset(new ResponseMessage());

      logger_->debug(
          "Consumed message type %s with stream %d, input %lu, remaining %d on '%s'",
          opcode_to_string(response->opcode()).c_str(), static_cast<int>(response->stream()),
          size, remaining, host_string_.c_str());

      if (response->stream() < 0) {
        // TODO(mstump) system events
        assert(false);
      } else {
        InternalRequest* request = NULL;
        if (stream_manager_.get_item(response->stream(), request)) {
          switch (request->state()) {
            case InternalRequest::REQUEST_STATE_READING:
              request->on_set(response.get());
              request->change_state(InternalRequest::REQUEST_STATE_DONE);
              break;

            case InternalRequest::REQUEST_STATE_WRITING:
              request->on_set(response.get());
              request->change_state(InternalRequest::REQUEST_STATE_READ_BEFORE_WRITE);
              break;

            case InternalRequest::REQUEST_STATE_WRITE_TIMEOUT:
              request->change_state(InternalRequest::REQUEST_STATE_READ_BEFORE_WRITE);
              break;

            case InternalRequest::REQUEST_STATE_READ_TIMEOUT:
              request->change_state(InternalRequest::REQUEST_STATE_DONE);
              break;

            case InternalRequest::REQUEST_STATE_WRITE_TIMEOUT_BEFORE_READ:
              request->change_state(InternalRequest::REQUEST_STATE_DONE);
              break;

            default:
              assert(false && "Invalid request state after receiving response");
              break;
          }

          if (request->state() == InternalRequest::REQUEST_STATE_DONE) {
            pending_requests_.remove(request);
            delete request;
          }
        } else {
          logger_->error("Invalid stream returnd from server on '%s'",
                         host_string_.c_str());
          defunct();
        }
      }
    }
    remaining -= consumed;
    buffer += consumed;
  }
}

void Connection::on_connect(Connecter* connecter) {
  Connection* connection = reinterpret_cast<Connection*>(connecter->data());

  if (connection->is_defunct()) {
    return; // Timed out
  }

  Timer::stop(connection->connect_timer_);
  connection->connect_timer_ = NULL;

  if (connecter->status() == Connecter::SUCCESS) {
    connection->logger_->debug("Connected to '%s'",
                               connection->host_string_.c_str());
    uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->socket_),
                  alloc_buffer, on_read);
    connection->state_ = CONNECTION_STATE_CONNECTED;
    connection->on_connected();
  } else {
    connection->logger_->info("Connect error '%s' on '%s'",
                              connection->host_string_.c_str(),
                              uv_err_name(uv_last_error(connection->loop_)));
    connection->notify_error("Unable to connect");
  }
}

void Connection::on_connect_timeout(Timer* timer) {
  Connection* connection = reinterpret_cast<Connection*>(timer->data());
  connection->connect_timer_ = NULL;
  connection->notify_error("Connection timeout");
}

void Connection::on_close(uv_handle_t* handle) {
  Connection* connection = reinterpret_cast<Connection*>(handle->data);

  connection->logger_->debug("Connection to '%s' closed",
                             connection->host_string_.c_str());

  while (!connection->pending_requests_.is_empty()) {
    InternalRequest* request = connection->pending_requests_.front();
    if (request->state() == InternalRequest::REQUEST_STATE_WRITING ||
        request->state() == InternalRequest::REQUEST_STATE_READING) {
      request->on_timeout();
      request->stop_timer();
    }
    connection->pending_requests_.remove(request);
    delete request;
  }

  if (connection->closed_callback_) {
    connection->closed_callback_(connection);
  }

  delete connection;
}

void Connection::on_read(uv_stream_t* client, ssize_t nread, uv_buf_t buf) {
  Connection* connection = reinterpret_cast<Connection*>(client->data);

  if (nread == -1) {
    if (uv_last_error(connection->loop_).code != UV_EOF) {
      connection->logger_->info("Read error '%s' on '%s'",
                                connection->host_string_.c_str(),
                                uv_err_name(uv_last_error(connection->loop_)));
    }
    connection->defunct();
    free_buffer(buf);
    return;
  }
  connection->consume(buf.base, nread);
  free_buffer(buf);
}

void Connection::on_write(Writer* writer) {
  InternalRequest* request = static_cast<InternalRequest*>(writer->data());
  Connection* connection = request->connection;

  switch (request->state()) {
    case InternalRequest::REQUEST_STATE_WRITING:
      if (writer->status() == Writer::SUCCESS) {
        request->change_state(InternalRequest::REQUEST_STATE_READING);
      } else {
        if (!connection->is_closing()) {
          connection->logger_->info(
              "Write error '%s' on '%s'", connection->host_string_.c_str(),
              uv_err_name(uv_last_error(connection->loop_)));
          connection->defunct();
        }
        request->on_error(CASS_ERROR_LIB_WRITE_ERROR,
                          "Unable to write to socket");
        request->change_state(InternalRequest::REQUEST_STATE_DONE);
      }
      break;

    case InternalRequest::REQUEST_STATE_WRITE_TIMEOUT:
      request->change_state(InternalRequest::REQUEST_STATE_WRITE_TIMEOUT_BEFORE_READ);
      break;

    case InternalRequest::REQUEST_STATE_READ_BEFORE_WRITE:
      request->change_state(InternalRequest::REQUEST_STATE_DONE);
      break;

    default:
      assert(false && "Invalid request state after write finished");
      break;
  }

  if (request->state() == InternalRequest::REQUEST_STATE_DONE) {
    connection->pending_requests_.remove(request);
    delete request;
  }
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

void Connection::notify_ready() {
  state_ = CONNECTION_STATE_READY;
  if (ready_callback_) {
    ready_callback_(this);
  }
}

void Connection::notify_error(const std::string& error) {
  logger_->error("'%s' error on startup for '%s'", error.c_str(),
                 host_string_.c_str());
  defunct();
}

void Connection::send_credentials() {
  ScopedPtr<V1Authenticator> v1_auth(config_.auth_provider()->new_authenticator_v1(host_.address));
  if (v1_auth) {
    V1Authenticator::Credentials credentials;
    v1_auth->get_credentials(&credentials);
    execute(new StartupHandler(this, new CredentialsRequest(credentials)));
  } else {
    send_initial_auth_response();
  }
}

void Connection::send_initial_auth_response() {
  Authenticator* auth = config_.auth_provider()->new_authenticator(host_.address);
  if (auth == NULL) {
    notify_error("Authenticaion required but no auth provider given");
  } else {
    AuthResponseRequest* auth_response
        = new AuthResponseRequest(auth->initial_response(), auth);
    execute(new StartupHandler(this, auth_response));
  }
}

} // namespace cass
