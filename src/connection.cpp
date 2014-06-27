#include "connection.hpp"
#include "constants.hpp"
#include "message.hpp"
#include "ssl_context.hpp"
#include "ssl_session.hpp"
#include "connecter.hpp"
#include "timer.hpp"
#include "config.hpp"
#include "result_response.hpp"
#include "supported_response.hpp"
#include "startup_request.hpp"
#include "query_request.hpp"
#include "options_request.hpp"
#include "logger.hpp"
#include "writer.hpp"

namespace cass {

Connection::StartupHandler::StartupHandler(Connection* connection,
                                           cass::Request* request)
    : connection_(connection)
    , request_(request) {
}

Request* Connection::StartupHandler::request() const {
  return request_.get();
}

void Connection::StartupHandler::on_set(Message* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_SUPPORTED:
      connection_->on_supported(response);
      break;
    case CQL_OPCODE_ERROR:
      connection_->notify_error(
          "Error during startup"); // TODO(mpenick): Better error
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
  connection_->notify_error("Error during startup");
}

void Connection::StartupHandler::on_timeout() {
  connection_->notify_error("Timed out during startup");
}

void Connection::StartupHandler::on_result_response(Message* response) {
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

Connection::Request::Request(Connection* connection,
                             ResponseCallback* response_callback)
    : connection(connection)
    , stream(0)
    , response_callback_(response_callback)
    , timer_(NULL)
    , state_(REQUEST_STATE_NEW) {
}

void Connection::Request::on_set(Message* response) {
  switch (response->opcode()) {
    case CQL_OPCODE_RESULT:
      on_result_response(response);
      break;
  }
  response_callback_->on_set(response);
}

void Connection::Request::on_error(CassError code, const std::string& message) {
  response_callback_->on_error(code, message);
  connection->stream_manager_.release_stream(stream);
}

void Connection::Request::on_timeout() {
  response_callback_->on_timeout();
}

void Connection::Request::change_state(Connection::Request::State next_state) {
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
      } else if (next_state == REQUEST_STATE_READ_BEFORE_WRITE) {
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

void Connection::Request::stop_timer() {
  assert(timer_ != NULL);
  Timer::stop(timer_);
  timer_ = NULL;
}

void Connection::Request::on_result_response(Message* response) {
  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());
  switch (result->kind()) {
    case CASS_RESULT_KIND_SET_KEYSPACE:
      connection->keyspace_ = result->keyspace();
      break;
  }
}

void Connection::Request::on_request_timeout(Timer* timer) {
  Request* request = static_cast<Request*>(timer->data());
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

Connection::Connection(uv_loop_t* loop, SSLSession* ssl_session,
                       const Host& host, Logger* logger, const Config& config,
                       const std::string& keyspace)
    : state_(CLIENT_STATE_NEW)
    , is_defunct_(false)
    , timed_out_request_count_(0)
    , loop_(loop)
    , incoming_(new Message())
    , host_(host)
    , host_string_(host.address.to_string())
    , ssl_(ssl_session)
    , ssl_handshake_done_(false)
    , version_("3.0.0")
    , logger_(logger)
    , config_(config)
    , keyspace_(keyspace)
    , connect_timer_(NULL) {
  socket_.data = this;
  uv_tcp_init(loop_, &socket_);
  if (ssl_) {
    ssl_->init();
    ssl_->handshake(true);
  }
}

void Connection::connect() {
  if (state_ == CLIENT_STATE_NEW) {
    state_ = CLIENT_STATE_CONNECTING;
    connect_timer_ = Timer::start(loop_, config_.connect_timeout(), this,
                                  on_connect_timeout);
    Connecter::connect(&socket_, host_.address, this, on_connect);
  }
}

bool Connection::execute(ResponseCallback* response_callback) {
  ScopedPtr<Request> request(new Request(this, response_callback));

  cass::Request* message = response_callback->request();

  int8_t stream = stream_manager_.acquire_stream(request.get());
  if (stream < 0) {
    return false;
  }

  request->stream = stream;

  ScopedPtr<BufferVec> bufs(new BufferVec());
  if(message->encode(0x02, 0x00, stream, bufs.get())) {
    request->on_error(CASS_ERROR_LIB_MESSAGE_PREPARE,
                      "Unable to build request");
    return true;
  }

  logger_->debug("Sending message type %s with %d",
                 opcode_to_string(message->opcode()).c_str(), stream);

  pending_requests_.add_to_back(request.get());

  request->change_state(Request::REQUEST_STATE_WRITING);
  write(bufs.release(), request.release());

  return true;
}

void Connection::close() {
  if (state_ != CLIENT_STATE_CLOSING && state_ != CLIENT_STATE_CLOSED) {
    if (!uv_is_closing(reinterpret_cast<uv_handle_t*>(&socket_))) {
      if (state_ >= CLIENT_STATE_CONNECTED) {
        uv_read_stop(reinterpret_cast<uv_stream_t*>(&socket_));
      }
      state_ = CLIENT_STATE_CLOSING;
      uv_close(reinterpret_cast<uv_handle_t*>(&socket_), on_close);
    }
  }
}

void Connection::defunct() {
  is_defunct_ = true;
  close();
}

void Connection::write(BufferVec* bufs, Connection::Request* request) {
  uv_stream_t* stream = reinterpret_cast<uv_stream_t*>(&socket_);
  Writer::write(stream, bufs, request, on_write);
}

void Connection::event_received() {
  switch (state_) {
    case CLIENT_STATE_CONNECTED:
      ssl_handshake();
      break;
    case CLIENT_STATE_HANDSHAKE:
      send_options();
      break;
    case CLIENT_STATE_SUPPORTED:
      send_startup();
      break;
    case CLIENT_STATE_READY:
      notify_ready();
      break;
    case CLIENT_STATE_SET_KEYSPACE:
      send_use_keyspace();
      break;
    case CLIENT_STATE_CLOSED:
      break;
    default:
      assert(false);
  }
}

void Connection::consume(char* input, size_t size) {
  char* buffer = input;
  int remaining = size;

  while (remaining != 0) {
    int consumed = incoming_->decode(buffer, remaining);
    if (consumed < 0) {
      // TODO(mstump) probably means connection closed/failed
      // Can this even happen right now?
      logger_->error("Error consuming message on '%s'", host_string_.c_str());
    }

    if (incoming_->body_ready()) {
      ScopedPtr<Message> response(incoming_.release());
      incoming_.reset(new Message());

      logger_->debug(
          "Consumed message type %s with stream %d, input %zd, remaining %d on "
          "'%s'",
          opcode_to_string(response->opcode()).c_str(), response->stream(),
          size, remaining, host_string_.c_str());

      if (response->stream() < 0) {
        // TODO(mstump) system events
        assert(false);
      } else {
        Request* request = NULL;
        if (stream_manager_.get_item(response->stream(), request)) {
          switch (request->state()) {
            case Request::REQUEST_STATE_READING:
              request->on_set(response.get());
              request->change_state(Request::REQUEST_STATE_DONE);
              break;

            case Request::REQUEST_STATE_WRITING:
              request->on_set(response.get());
              request->change_state(Request::REQUEST_STATE_READ_BEFORE_WRITE);
              break;

            case Request::REQUEST_STATE_WRITE_TIMEOUT:
              request->change_state(Request::REQUEST_STATE_READ_BEFORE_WRITE);
              break;

            case Request::REQUEST_STATE_READ_TIMEOUT:
              request->change_state(Request::REQUEST_STATE_DONE);
              break;

            case Request::REQUEST_STATE_WRITE_TIMEOUT_BEFORE_READ:
              request->change_state(Request::REQUEST_STATE_DONE);
              break;

            default:
              assert(false && "Invalid request state after receiving response");
              break;
          }

          if (request->state() == Request::REQUEST_STATE_DONE) {
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
    connection->state_ = CLIENT_STATE_CONNECTED;
    connection->event_received();
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

  connection->state_ = CLIENT_STATE_CLOSED;
  connection->event_received();

  while (!connection->pending_requests_.is_empty()) {
    Request* request = connection->pending_requests_.front();
    if (request->state() == Request::REQUEST_STATE_WRITING ||
        request->state() == Request::REQUEST_STATE_READING) {
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

  if (connection->ssl_) {
//    char* read_input = buf.base;
//    size_t read_input_size = nread;

//    for (;;) {
//      size_t read_size = 0;
//      char* read_output = NULL;
//      size_t read_output_size = 0;
//      char* write_output = NULL;
//      size_t write_output_size = 0;

//      // TODO(mstump) error handling for SSL decryption
//      std::string error;
//      connection->ssl_->read_write(read_input, read_input_size, read_size,
//                                   &read_output, read_output_size, NULL, 0,
//                                   &write_output, write_output_size, &error);

//      if (read_output && read_output_size) {
//        // TODO(mstump) error handling
//        connection->consume(read_output, read_output_size);
//        delete read_output;
//      }

//      if (write_output && write_output_size) {
//        Request* request = new Request(connection, NULL);
//        connection->write(uv_buf_init(write_output, write_output_size),
//                          request);
//        // delete of write_output will be handled by on_write
//      }

//      if (read_size < read_input_size) {
//        read_input += read_size;
//        read_input_size -= read_size;
//      } else {
//        break;
//      }

//      if (!connection->ssl_handshake_done_) {
//        if (connection->ssl_->handshake_done()) {
//          connection->state_ = CLIENT_STATE_HANDSHAKE;
//          connection->event_received();
//        }
//      }
//    }
  } else {
    connection->consume(buf.base, nread);
  }
  free_buffer(buf);
}

void Connection::on_write(Writer* writer) {
  Request* request = static_cast<Request*>(writer->data());
  Connection* connection = request->connection;

  switch (request->state()) {
    case Request::REQUEST_STATE_WRITING:
      if (writer->status() == Writer::SUCCESS) {
        request->change_state(Request::REQUEST_STATE_READING);
      } else {
        if (!connection->is_closing()) {
          connection->logger_->info(
              "Write error '%s' on '%s'", connection->host_string_.c_str(),
              uv_err_name(uv_last_error(connection->loop_)));
          connection->defunct();
        }
        request->on_error(CASS_ERROR_LIB_WRITE_ERROR,
                          "Unable to write to socket");
        request->change_state(Request::REQUEST_STATE_DONE);
      }
      break;

    case Request::REQUEST_STATE_WRITE_TIMEOUT:
      request->change_state(Request::REQUEST_STATE_WRITE_TIMEOUT_BEFORE_READ);
      break;

    case Request::REQUEST_STATE_READ_BEFORE_WRITE:
      request->change_state(Request::REQUEST_STATE_DONE);
      break;

    default:
      assert(false && "Invalid request state after write finished");
      break;
  }

  if (request->state() == Request::REQUEST_STATE_DONE) {
    connection->pending_requests_.remove(request);
    delete request;
  }
}

void Connection::ssl_handshake() {
  if (ssl_) {
    // calling read on a handshaked initiated ssl_ pipe
    // will gives us the first message to send to the server
    on_read(reinterpret_cast<uv_stream_t*>(&socket_), 0, alloc_buffer(0));
  } else {
    state_ = CLIENT_STATE_HANDSHAKE;
    event_received();
  }
}

void Connection::on_ready() {
  if (keyspace_.empty()) {
    state_ = CLIENT_STATE_READY;
  } else {
    state_ = CLIENT_STATE_SET_KEYSPACE;
  }
  event_received();
}

void Connection::on_set_keyspace() {
  state_ = CLIENT_STATE_READY;
  event_received();
}

void Connection::on_supported(Message* response) {
  SupportedResponse* supported =
      static_cast<SupportedResponse*>(response->response_body().get());

  // TODO(mstump) do something with the supported info
  (void)supported;

  state_ = CLIENT_STATE_SUPPORTED;
  event_received();
}

void Connection::notify_ready() {
  if (ready_callback_) {
    ready_callback_(this);
  }
}

void Connection::notify_error(const std::string& error) {
  logger_->error("'%s' error on startup for '%s'", error.c_str(),
                 host_string_.c_str());
  defunct();
}

void Connection::send_options() {
  execute(new StartupHandler(this, new OptionsRequest()));
}

void Connection::send_startup() {
  StartupRequest* startup = new StartupRequest();
  startup->set_version(version_);
  execute(new StartupHandler(this, startup));
}

void Connection::send_use_keyspace() {
  QueryRequest* query = new QueryRequest();
  query->statement("use \"" + keyspace_ + "\"");
  query->set_consistency(CASS_CONSISTENCY_ONE);
  execute(new StartupHandler(this, query));
}

} // namespace cass
