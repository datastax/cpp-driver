/*
  Copyright 2014 DataStax

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

#ifndef __CASS_CONNECTION_HPP_INCLUDED__
#define __CASS_CONNECTION_HPP_INCLUDED__

#include <uv.h>

#include "common.hpp"
#include "host.hpp"
#include "message.hpp"
#include "session.hpp"
#include "ssl_context.hpp"
#include "ssl_session.hpp"
#include "stream_manager.hpp"
#include "connecter.hpp"
#include "writer.hpp"
#include "timer.hpp"
#include "list.hpp"

namespace cass {

class Connection {
  public:
    class StartupHandler : public ResponseCallback {
      public:
        StartupHandler(Connection* connection, Message* request)
          : connection_(connection)
          , request_(request) { }

        virtual Message* request() const {
          return request_.get();
        }

        virtual void on_set(Message* response) {
          switch(response->opcode) {
            case CQL_OPCODE_SUPPORTED:
              connection_->on_supported(response);
              break;
            case CQL_OPCODE_ERROR:
              connection_->notify_error("Error during startup"); // TODO(mpenick): Better error
              connection_->defunct();
              break;
            case CQL_OPCODE_READY:
              connection_->on_ready();
              break;
            case CQL_OPCODE_RESULT:
              on_result_response(response);
              break;
            default:
              connection_->notify_error("Invalid opcode during startup");
              connection_->defunct();
              break;
          }
        }

        virtual void on_error(CassError code, const std::string& message) {
          connection_->notify_error("Error during startup");
          connection_->defunct();
        }

        virtual void on_timeout() {
          connection_->notify_error("Timed out during startup");
          connection_->defunct();
        }

      private:
        void on_result_response(Message* response) {
          ResultResponse* result = static_cast<ResultResponse*>(response->body.get());
          switch(result->kind) {
            case CASS_RESULT_KIND_SET_KEYSPACE:
              connection_->on_set_keyspace();
              break;
            default:
              connection_->notify_error("Invalid result during startup. Expected set keyspace.");
              connection_->defunct();
              break;
          }
        }

        Connection* connection_;
        std::unique_ptr<Message> request_;
    };

    struct Request : public List<Request>::Node {
        enum State {
          REQUEST_STATE_NEW,
          REQUEST_STATE_WRITING,
          REQUEST_STATE_READING,
          REQUEST_STATE_WRITE_TIMEOUT,
          REQUEST_STATE_READ_TIMEOUT,
          REQUEST_STATE_READ_BEFORE_WRITE,
          REQUEST_STATE_WRITE_TIMEOUT_BEFORE_READ,
          REQUEST_STATE_DONE,
        };

        Request(Connection* connection,
                ResponseCallback* response_callback)
          : connection(connection)
          , stream(0)
          , response_callback_(response_callback)
          , timer_(nullptr)
          , state_(REQUEST_STATE_NEW) { }

        void on_set(Message* response) {
          switch(response->opcode) {
            case CQL_OPCODE_RESULT:
              on_result_response(response);
              break;
          }
          response_callback_->on_set(response);
        }

        void on_error(CassError code, const std::string& message) {
          response_callback_->on_error(code, message);
          connection->stream_manager_.release_stream(stream);
        }

        void on_timeout() {
          response_callback_->on_timeout();
        }

        State state() const { return state_; }

        void change_state(State next_state) {
          switch(state_) {
            case REQUEST_STATE_NEW:
              assert(next_state == REQUEST_STATE_WRITING && "Invalid request state after new");
              state_ = REQUEST_STATE_WRITING;
              timer_ = Timer::start(connection->loop_, connection->config_.write_timeout(), this, on_request_timeout);
              break;

            case REQUEST_STATE_WRITING:
              if(next_state == REQUEST_STATE_READING) { // Success
                stop_timer();
                state_ = next_state;
                timer_ = Timer::start(connection->loop_, connection->config_.read_timeout(), this, on_request_timeout);
              } else if(next_state == REQUEST_STATE_READ_BEFORE_WRITE) {
                stop_timer();
                state_ = next_state;
              } else if(next_state == REQUEST_STATE_WRITE_TIMEOUT) {
                connection->timed_out_request_count_++;
                state_ = next_state;
              } else {
                assert(false && "Invalid request state after writing");
              }
              break;

            case REQUEST_STATE_READING:
              if(next_state == REQUEST_STATE_DONE) { // Success
                stop_timer();
                state_ = next_state;
                cleanup();
              } else if(next_state == REQUEST_STATE_READ_TIMEOUT) {
               connection-> timed_out_request_count_++;
                state_ = next_state;
              } else {
                assert(false && "Invalid request state after reading");
              }
              break;

            case REQUEST_STATE_WRITE_TIMEOUT:
              assert((next_state == REQUEST_STATE_WRITE_TIMEOUT_BEFORE_READ || next_state == REQUEST_STATE_READ_BEFORE_WRITE)
                     && "Invalid request state after write timeout");
              state_ = next_state;
              break;

            case REQUEST_STATE_READ_TIMEOUT:
              assert(next_state == REQUEST_STATE_DONE && "Invalid request state after read timeout");
              connection->timed_out_request_count_--;
              state_ = next_state;
              cleanup();
              break;

            case REQUEST_STATE_READ_BEFORE_WRITE:
              assert(next_state == REQUEST_STATE_DONE && "Invalid request state after read before write");
              state_ = next_state;
              cleanup();
              break;

            case REQUEST_STATE_WRITE_TIMEOUT_BEFORE_READ:
              assert(next_state == REQUEST_STATE_DONE && "Invalid request state after write timeout before read");
              connection->timed_out_request_count_--;
              state_ = next_state;
              cleanup();
              break;

            case REQUEST_STATE_DONE:
              assert(false && "Invalid request state after done");
              break;

            default:
              assert(false && "Invalid request state");
              break;
          }
        }

        void stop_timer() {
          assert(timer_ != nullptr);
          Timer::stop(timer_);
          timer_ = nullptr;
        }

        Connection* connection;
        int8_t stream;

      private:
        void cleanup() {
          connection->pending_requests_.remove(this);
          connection->maybe_close();
          delete this;
        }

        void on_result_response(Message* response) {
          ResultResponse* result = static_cast<ResultResponse*>(response->body.get());
          switch(result->kind) {
            case CASS_RESULT_KIND_SET_KEYSPACE:
              connection->keyspace_.assign(result->keyspace, result->keyspace_size);
              break;
          }
        }

        static void on_request_timeout(Timer* timer) {
          Request *request = static_cast<Request*>(timer->data());
          request->connection->logger_->info("Request timed out to '%s'",
                                             request->connection->host_string_.c_str());
          request->timer_ = nullptr;
          if(request->state_ == REQUEST_STATE_READING) {
            request->change_state(REQUEST_STATE_READ_TIMEOUT);
          } else if(request->state_ == REQUEST_STATE_WRITING) {
            request->change_state(REQUEST_STATE_WRITE_TIMEOUT);
          } else {
            assert(false && "Invalid request state for timeout");
          }
          request->on_timeout();
        }

      private:
        std::unique_ptr<ResponseCallback> response_callback_;
        Timer* timer_;
        State state_;
    };

    typedef std::function<void(Connection*)> ConnectCallback;
    typedef std::function<void(Connection*)> CloseCallback;

  public:
    enum ClientConnectionState {
      CLIENT_STATE_NEW,
      CLIENT_STATE_CONNECTED,
      CLIENT_STATE_HANDSHAKE,
      CLIENT_STATE_SUPPORTED,
      CLIENT_STATE_SET_KEYSPACE,
      CLIENT_STATE_READY,
      CLIENT_STATE_CLOSING,
      CLIENT_STATE_CLOSED
    };

    enum Compression {
      CLIENT_COMPRESSION_NONE,
      CLIENT_COMPRESSION_SNAPPY,
      CLIENT_COMPRESSION_LZ4
    };

    enum SchemaEventType {
      CLIENT_EVENT_SCHEMA_CREATED,
      CLIENT_EVENT_SCHEMA_UPDATED,
      CLIENT_EVENT_SCHEMA_DROPPED
    };

    Connection(uv_loop_t* loop,
               SSLSession* ssl_session,
               const Host& host,
               Logger* logger,
               const Config& config,
               const std::string& keyspace,
               ConnectCallback connect_callback,
               CloseCallback close_callback)
      : state_(CLIENT_STATE_NEW)
      , is_defunct_(false)
      , timed_out_request_count_(0)
      , loop_(loop)
      , incoming_(new Message())
      , connect_callback_(connect_callback)
      , close_callback_(close_callback)
      , host_(host)
      , host_string_(host.address.to_string())
      , ssl_(ssl_session)
      , ssl_handshake_done_(false)
      , version_("3.0.0")
      , logger_(logger)
      , config_(config)
      , keyspace_(keyspace)
      , connect_timer_(nullptr) {
      socket_.data = this;
      uv_tcp_init(loop_, &socket_);
      if (ssl_) {
        ssl_->init();
        ssl_->handshake(true);
      }
    }

    ~Connection() {
    }

    void connect() {
      if(state_ == CLIENT_STATE_NEW) {
        connect_timer_ = Timer::start(loop_, config_.connect_timeout(), this, on_connect_timeout);
        Connecter::connect(&socket_, host_.address, this, on_connect);
      }
    }

    bool execute(ResponseCallback* response_callback) {
      std::unique_ptr<Request> request(new Request(this, response_callback));

      Message* message = response_callback->request();

      int8_t stream = stream_manager_.acquire_stream(request.get());
      if(request->stream < 0) {
        return false;
      }

      request->stream = stream;
      message->stream = stream;

      uv_buf_t buf;
      if (!message->prepare(&buf.base, buf.len)) {
        request->on_error(CASS_ERROR_LIB_MESSAGE_PREPARE, "Unable to build request");
        return true;
      }

      logger_->debug("Sending message type %s with %d, size %zd",
                     opcode_to_string(message->opcode).c_str(), message->stream, buf.len);

      pending_requests_.add_to_back(request.get());

      request->change_state(Request::REQUEST_STATE_WRITING);
      write(buf, request.release());

      return true;
    }

    const std::string& keyspace() {
      return keyspace_;
    }

    void close() {
      state_ = CLIENT_STATE_CLOSING;
      maybe_close();
    }

    bool is_closing() const {
      return state_ == CLIENT_STATE_CLOSING;
    }

    void maybe_close() {
      if(state_ == CLIENT_STATE_CLOSING && !has_requests_pending()) {
        actually_close();
      }
    }

    void defunct() {
      if(!is_defunct_) {
        is_defunct_ = true;
        actually_close();
      }
    }

    bool is_defunct() const {
      return is_defunct_;
    }


    bool is_ready() {
      return state_ == CLIENT_STATE_READY;
    }

    inline size_t available_streams() {
      return stream_manager_.available_streams();
    }

    inline bool has_requests_pending() {
      return pending_requests_.size() - timed_out_request_count_ > 0;
    }

  private:
    void actually_close() {
      if(!uv_is_closing(reinterpret_cast<uv_handle_t*>(&socket_))) {
        if(state_ >= CLIENT_STATE_CONNECTED) {
          uv_read_stop(reinterpret_cast<uv_stream_t*>(&socket_));
        }
        state_ = CLIENT_STATE_CLOSING;
        uv_close(reinterpret_cast<uv_handle_t*>(&socket_), on_close);
      }
    }

    void write(uv_buf_t buf, Request* request) {
      Writer::Bufs* bufs = new Writer::Bufs({ buf });
      Writer::write(reinterpret_cast<uv_stream_t*>(&socket_), bufs, request, on_write);
    }

    void event_received() {
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

    void consume(char* input, size_t size) {
      char* buffer    = input;
      int   remaining = size;

      while (remaining != 0) {
        int consumed = incoming_->consume(buffer, remaining);
        if (consumed < 0) {
          // TODO(mstump) probably means connection closed/failed
          // Can this even happen right now?
          logger_->error("Error consuming message on '%s'",
                         host_string_.c_str());
        }

        if (incoming_->body_ready) {
          std::unique_ptr<Message> response(std::move(incoming_));
          incoming_.reset(new Message());

          logger_->debug("Consumed message type %s with stream %d, input %zd, remaining %d on '%s'",
                         opcode_to_string(response->opcode).c_str(), response->stream,
                         size, remaining, host_string_.c_str());

          if (response->stream < 0) {
            // TODO(mstump) system events
            assert(false);
          } else {
            Request* request = nullptr;
            if(stream_manager_.get_item(response->stream, request)) {
              switch(request->state()) {
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
            } else {
              logger_->error("Invalid stream returnd from server on '%s'",
                             host_string_.c_str());
              defunct();
            }
          }
        }
        remaining -= consumed;
        buffer    += consumed;
      }
    }

    static void on_read(uv_stream_t* client, ssize_t nread, uv_buf_t buf) {
      Connection* connection =
          reinterpret_cast<Connection*>(client->data);

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
        char*  read_input        = buf.base;
        size_t read_input_size   = nread;

        for (;;) {
          size_t read_size         = 0;
          char*  read_output       = nullptr;
          size_t read_output_size  = 0;
          char*  write_output      = nullptr;
          size_t write_output_size = 0;

          // TODO(mstump) error handling for SSL decryption
          std::string error;
          connection->ssl_->read_write(read_input,
                                       read_input_size,
                                       read_size,
                                       &read_output,
                                       read_output_size,
                                       nullptr,
                                       0,
                                       &write_output,
                                       write_output_size,
                                       &error);

          if (read_output && read_output_size) {
            // TODO(mstump) error handling
            connection->consume(read_output, read_output_size);
            delete read_output;
          }

          if (write_output && write_output_size) {
            Request* request = new Request(connection, nullptr);
            connection->write(uv_buf_init(write_output, write_output_size), request);
            // delete of write_output will be handled by on_write
          }

          if (read_size < read_input_size) {
            read_input += read_size;
            read_input_size -= read_size;
          } else {
            break;
          }

          if (!connection->ssl_handshake_done_) {
            if (connection->ssl_->handshake_done()) {
              connection->state_ = CLIENT_STATE_HANDSHAKE;
              connection->event_received();
            }
          }
        }
      } else {
        connection->consume(buf.base, nread);
      }
      free_buffer(buf);
    }

    static void on_connect(Connecter* connecter) {
      Connection* connection
          = reinterpret_cast<Connection*>(connecter->data());

      if(connection->is_defunct()) {
        return; // Timed out
      }

      Timer::stop(connection->connect_timer_);
      connection->connect_timer_ = nullptr;

      if(connection->is_closing()) {
        connection->notify_error("Closing");
        return;
      }

      if(connecter->status() == Connecter::SUCCESS) {
        connection->logger_->debug("Connected to '%s'",
                                   connection->host_string_.c_str());
        uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->socket_),
                      alloc_buffer,
                      on_read);
        connection->state_ = CLIENT_STATE_CONNECTED;
        connection->event_received();
      } else {
        connection->logger_->info("Connect error '%s' on '%s'",
                                  connection->host_string_.c_str(),
                                  uv_err_name(uv_last_error(connection->loop_)));
        connection->defunct();
        connection->notify_error("Unable to connect");
      }
    }

    static void on_connect_timeout(Timer* timer) {
      Connection* connection
          = reinterpret_cast<Connection*>(timer->data());
      connection->connect_timer_ = nullptr;
      connection->notify_error("Connection timeout");
      connection->defunct();
    }

    static void on_close(uv_handle_t *handle) {
      Connection* connection
          = reinterpret_cast<Connection*>(handle->data);

      connection->logger_->debug("Connection to '%s' closed",
                                 connection->host_string_.c_str());

      connection->state_ = CLIENT_STATE_CLOSED;
      connection->event_received();

      while(!connection->pending_requests_.is_empty()) {
        Request* request = connection->pending_requests_.front();
        if(request->state() == Request::REQUEST_STATE_WRITING
           || request->state() == Request::REQUEST_STATE_READING) {
          request->on_timeout();
          request->stop_timer();
        }
        connection->pending_requests_.remove(request);
        delete request;
      }

    connection->close_callback_(connection);

      delete connection;
    }

    void ssl_handshake() {
      if (ssl_) {
        // calling read on a handshaked initiated ssl_ pipe
        // will gives us the first message to send to the server
        on_read(
              reinterpret_cast<uv_stream_t*>(&socket_),
              0,
              alloc_buffer(0));
      } else {
        state_ = CLIENT_STATE_HANDSHAKE;
        event_received();
      }
    }

    void on_ready() {
      if(keyspace_.empty()) {
        state_ = CLIENT_STATE_READY;
      } else {
        state_ = CLIENT_STATE_SET_KEYSPACE;
      }
      event_received();
    }

    void on_set_keyspace() {
      state_ = CLIENT_STATE_READY;
      event_received();
    }

    void on_supported(Message* response) {
      SupportedResponse* supported
          = static_cast<SupportedResponse*>(response->body.get());

      // TODO(mstump) do something with the supported info
      (void) supported;

      state_ = CLIENT_STATE_SUPPORTED;
      event_received();
    }

    void notify_ready() {
      connect_callback_(this);
    }

    void notify_error(const std::string& error) {
      logger_->error("'%s' error on startup for '%s'",
                     error.c_str(),
                     host_string_.c_str());
      connect_callback_(this);
    }

    void send_options() {
      execute(new StartupHandler(this, new Message(CQL_OPCODE_OPTIONS)));
    }

    void send_startup() {
      Message* message = new Message(CQL_OPCODE_STARTUP);
      StartupRequest* startup = static_cast<StartupRequest*>(message->body.get());
      startup->version = version_;
      execute(new StartupHandler(this, message));
    }

    void send_use_keyspace() {
      Message* message = new Message(CQL_OPCODE_QUERY);
      QueryRequest* query = static_cast<QueryRequest*>(message->body.get());
      query->statement("use \"" + keyspace_ + "\"");
      query->consistency(CASS_CONSISTENCY_ONE);
      execute(new StartupHandler(this, message));
    }

    static void on_write(Writer* writer) {
      Request* request = static_cast<Request*>(writer->data());
      Connection* connection = request->connection;

      switch(request->state()) {
        case Request::REQUEST_STATE_WRITING:
          if(writer->status() == Writer::SUCCESS) {
            request->change_state(Request::REQUEST_STATE_READING);
          } else {
            if(!connection->is_closing()) {
              connection->logger_->info("Write error '%s' on '%s'",
                                        connection->host_string_.c_str(),
                                        uv_err_name(uv_last_error(connection->loop_)));
              connection->defunct();
            }
            request->on_error(CASS_ERROR_LIB_WRITE_ERROR, "Unable to write to socket");
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
    }

  private:
    ClientConnectionState       state_;
    bool is_defunct_;

    List<Request> pending_requests_;
    int timed_out_request_count_;

    uv_loop_t*                  loop_;
    std::unique_ptr<Message>    incoming_;
    StreamManager<Request*>     stream_manager_;
    ConnectCallback             connect_callback_;
    CloseCallback               close_callback_;
    // DNS and hostname stuff
    Host                        host_;
    std::string                 host_string_;
    // the actual connection
    uv_tcp_t                    socket_;
    // ssl stuff
    SSLSession*                 ssl_;
    bool                        ssl_handshake_done_;
    // supported stuff sent in start up message
    std::string                 compression_;
    std::string                 version_;

    Logger* logger_;
    const Config& config_;
    std::string keyspace_;
    Timer* connect_timer_;

    DISALLOW_COPY_AND_ASSIGN(Connection);
};

} // namespace cass

#endif
