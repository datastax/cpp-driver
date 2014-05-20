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

#ifndef __CASS_CLIENT_CONNECTION_HPP_INCLUDED__
#define __CASS_CLIENT_CONNECTION_HPP_INCLUDED__

#include "common.hpp"
#include "host.hpp"
#include "message.hpp"
#include "session.hpp"
#include "ssl_context.hpp"
#include "ssl_session.hpp"
#include "stream_manager.hpp"
#include "connecter.hpp"
#include "writer.hpp"


namespace cass {

class ClientConnection {
  private:

//    typedef std::function<void(ClientConnection*,
//                               const char*, size_t)> KeyspaceCallback;

//    typedef std::function<void(ClientConnection*,
//                               SchemaEventType,
//                               const char*, size_t,
//                               const char*, size_t)> SchemaCallback;

//    typedef std::function<void(ClientConnection*,
//                               Error*,
//                               const char*, size_t,
//                               const char*, size_t)> PrepareCallback;

    typedef std::function<void(ClientConnection*, Message* message)> RequestCallback;
    struct Request {
        enum State {
          REQUEST_STATE_WRITING,
          REQUEST_STATE_READING,
          REQUEST_STATE_TIMED_OUT,
        };

        Request(ClientConnection* connection,
                RequestCallback cb)
          : connection(connection)
          , cb(cb)
          , future(nullptr)
          , message(nullptr)
          , stream(0)
          , timer(nullptr)
          , state(REQUEST_STATE_WRITING) { }

        void stop_timer() {
          if(timer) {
            Timer::stop(timer);
            timer = nullptr;
          }
        }

        ClientConnection* connection;
        RequestCallback cb;
        RequestFuture* future;
        Message* message;
        int8_t stream;
        Timer* timer;
        State state;
    };

    typedef std::function<void(ClientConnection*, Error*)> ConnectCallback;
    typedef std::function<void(ClientConnection*)> CloseCallback;

  public:
    enum ClientConnectionState {
      CLIENT_STATE_NEW,
      CLIENT_STATE_CONNECTED,
      CLIENT_STATE_HANDSHAKE,
      CLIENT_STATE_SUPPORTED,
      CLIENT_STATE_READY,
      CLIENT_STATE_DEFUNCT,
      CLIENT_STATE_DISCONNECTING,
      CLIENT_STATE_DISCONNECTED
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

    ClientConnection(uv_loop_t* loop,
                     SSLSession* ssl_session,
                     const Host& host,
                     ConnectCallback connect_callback,
                     CloseCallback close_callback)
      : state_(CLIENT_STATE_NEW)
      , request_count_(0)
      , loop_(loop)
      , incoming_(new Message())
      , connect_callback_(connect_callback)
      , close_callback_(close_callback)
      , log_callback_(nullptr)
      , host_(host)
      , ssl_(ssl_session)
      , ssl_handshake_done_(false)
      , version_("3.0.0")
      , connect_timeout_(1000)
      , write_timeout_(1000)
      , read_timeout_(1000)
      , connect_timer_(nullptr) {
      socket_.data = this;
      uv_tcp_init(loop_, &socket_);
      if (ssl_) {
        ssl_->init();
        ssl_->handshake(true);
      }
    }

    ~ClientConnection() {
      for(auto request : timed_out_requests_) {
        delete request;
      }
    }

    void connect() {
      if(state_ == CLIENT_STATE_NEW) {
        log(CASS_LOG_DEBUG, "connect");
        connect_timer_ = Timer::start(loop_, connect_timeout_, this, on_connect_timeout);
        Connecter::connect(&socket_, host_.address, this, on_connect);
      }
    }

    bool execute(Message* message,
                 RequestFuture* request_future = nullptr,
                 RequestCallback request_callback = nullptr) {
      std::unique_ptr<Request> request(new Request(this, request_callback));
      request->future = request_future;
      if(request_future == nullptr) {
        request->message = message;
      }
      int8_t stream = stream_manager_.acquire_stream(request.get());
      if(request->stream < 0) {
        return false;
      }

      request->stream = stream;
      message->stream = stream;

      uv_buf_t buf;
      if (!message->prepare(&buf.base, buf.len)) {
        request_future->set_error(CASS_ERROR(CASS_ERROR_SOURCE_LIBRARY,
                                     CASS_ERROR_LIB_MESSAGE_PREPARE,
                                     "error preparing message"));
        return true;
      }

      char log_message[512];
      snprintf(
            log_message,
            sizeof(log_message),
            "sending message type %s with stream %d, size %zd",
            opcode_to_string(message->opcode).c_str(),
            message->stream,
            buf.len);

      log(CASS_LOG_DEBUG, log_message);

      request_count_++;
      request->timer = Timer::start(loop_, write_timeout_, request.get(), on_timeout);
      write(buf, request.release());

      return true;
    }

    void close() {
      uv_read_stop(reinterpret_cast<uv_stream_t*>(&socket_));
      uv_close(reinterpret_cast<uv_handle_t*>(&socket_), on_close);
    }

    void shutdown() {
      state_ = CLIENT_STATE_DISCONNECTING;
    }

    void defunct() {
      state_ = CLIENT_STATE_DEFUNCT;
      maybe_close();
    }

    bool is_disconnecting() {
      return state_ == CLIENT_STATE_DISCONNECTING;
    }

    bool is_defunct() {
      return state_ == CLIENT_STATE_DEFUNCT;
    }

    bool is_ready() {
      return state_ == CLIENT_STATE_READY;
    }

    void maybe_close() {
      if((is_disconnecting() || is_defunct()) && outstanding_request_count() == 0) {
        close();
      }
    }

    inline size_t available_streams() {
      return stream_manager_.available_streams();
    }

    inline int outstanding_request_count() {
      return request_count_ - timed_out_requests_.size();
    }

  private:
    inline void log(int level, const char* message, size_t size) {
      if (log_callback_) {
        log_callback_(level, message, size);
      }
    }

    inline void log(int level, const char* message) {
      if (log_callback_) {
        log_callback_(level, message, strlen(message));
      }
    }

    void write(uv_buf_t buf, Request* request) {
      Writer::Bufs* bufs = new Writer::Bufs({ buf });
      Writer::write(reinterpret_cast<uv_stream_t*>(&socket_), bufs, request, on_write);
    }

    void event_received() {
      log(CASS_LOG_DEBUG, "event received");

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
        case CLIENT_STATE_DISCONNECTED:
          break;
        case CLIENT_STATE_DEFUNCT:
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
          fprintf(stderr, "consume error\n");
        }

        if (incoming_->body_ready) {
          std::unique_ptr<Message> message(std::move(incoming_));
          incoming_.reset(new Message());

          char log_message[512];
          snprintf(
                log_message,
                sizeof(log_message),
                "consumed message type %s with stream %d, input %zd, remaining %d",
                opcode_to_string(message->opcode).c_str(),
                message->stream,
                size,
                remaining);

          log(CASS_LOG_DEBUG, log_message);
          if (message->stream < 0) {
            // TODO(mstump) system events
            assert(false);
          } else {
            Request* request = nullptr;
            if(stream_manager_.get_item(message->stream, request)) {
              if(request->state == Request::REQUEST_STATE_TIMED_OUT) {
                timed_out_requests_.remove(request);
              } else {
                request->stop_timer();

                switch (message->opcode) {
                  case CQL_OPCODE_SUPPORTED:
                    on_supported(message.get());
                    break;
                  case CQL_OPCODE_ERROR:
                    on_error(message.get(), request);
                    break;
                  case CQL_OPCODE_READY:
                    on_ready(message.get());
                    break;
                  case CQL_OPCODE_RESULT:
                    on_result(message.get(), request);
                    break;
                  default:
                    // TODO(mpenick): Log this
                    defunct();
                    break;
                }
              }
              request_count_--;
              delete request;
              maybe_close();
            } else {
              // TODO(mpenick): Log this
              defunct();
            }
          }
        }
        remaining -= consumed;
        buffer    += consumed;
      }
    }

    static void on_read(uv_stream_t* client, ssize_t nread, uv_buf_t buf) {
      ClientConnection* connection =
          reinterpret_cast<ClientConnection*>(client->data);

      connection->log(CASS_LOG_DEBUG, "on_read");
      if (nread == -1) {
        if (uv_last_error(connection->loop_).code != UV_EOF) {
          fprintf(stderr,
                  "Read error %s\n",
                  uv_err_name(uv_last_error(connection->loop_)));
        }
        connection->defunct();
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
          connection->ssl_->read_write(
                read_input,
                read_input_size,
                read_size,
                &read_output,
                read_output_size,
                nullptr,
                0,
                &write_output,
                write_output_size);

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
      ClientConnection* connection
          = reinterpret_cast<ClientConnection*>(connecter->data());

      connection->log(CASS_LOG_DEBUG, "on_connect");

      if(connection->is_defunct()) { // Timed out
        return;
      }

      Timer::stop(connection->connect_timer_);
      connection->connect_timer_ = nullptr;

      if(connecter->status() == Connecter::SUCCESS) {
        uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->socket_),
                      alloc_buffer,
                      on_read);

        connection->state_ = CLIENT_STATE_CONNECTED;
        connection->event_received();
      } else {
        connection->notify_error(CASS_ERROR(CASS_ERROR_SOURCE_LIBRARY,
                                            CASS_ERROR_LIB_BAD_PARAMS,
                                            "Unable to connect"));
        connection->defunct();
      }
    }

    static void on_connect_timeout(Timer* timer) {
      ClientConnection* connection
          = reinterpret_cast<ClientConnection*>(timer->data());
      connection->notify_error(CASS_ERROR(CASS_ERROR_SOURCE_LIBRARY,
                                          CASS_ERROR_LIB_BAD_PARAMS,
                                          "Connection timeout"));
      connection->connect_timer_ = nullptr;
      connection->defunct();
    }

    static void on_close(uv_handle_t *handle) {
      ClientConnection* connection
          = reinterpret_cast<ClientConnection*>(handle->data);

      connection->log(CASS_LOG_DEBUG, "on_close");
      connection->state_ = CLIENT_STATE_DISCONNECTED;
      connection->event_received();

      connection->close_callback_(connection);
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

    void on_result(Message* response, Request* request) {
      log(CASS_LOG_DEBUG, "on_result");

      if(request->cb) {
        request->cb(this, response);
      }

      if(request->future != nullptr) {
        request->future->set_result(response->body.release());
      }
    }

    void on_error(Message* response, Request* request) {
      log(CASS_LOG_DEBUG, "on_error");
      BodyError* error = static_cast<BodyError*>(response->body.get());

      if (state_ < CLIENT_STATE_READY) {
        notify_error(
              new Error(
                CASS_ERROR_SOURCE_SERVER,
                CASS_OK, // TODO(mpenick): Need valid error
                error->message,
                __FILE__,
                __LINE__));
      } else if(request->future != nullptr) {
        request->future->set_error(CASS_ERROR(CASS_ERROR_SOURCE_SERVER, (cass_code_t)error->code, error->message));
      }
    }

    void on_ready(Message* response) {
      log(CASS_LOG_DEBUG, "on_ready");
      state_ = CLIENT_STATE_READY;
      event_received();
    }

    void on_supported(Message* response) {
      log(CASS_LOG_DEBUG, "on_supported");
      BodySupported* supported
          = static_cast<BodySupported*>(response->body.get());

      // TODO(mstump) do something with the supported info
      (void) supported;

      state_ = CLIENT_STATE_SUPPORTED;
      event_received();
    }

    void set_keyspace(const std::string& keyspace) {
      Message message(CQL_OPCODE_QUERY);
      Query* query = static_cast<Query*>(message.body.get());
      query->statement("USE " + keyspace);
      execute(&message, nullptr);
    }

    void notify_ready() {
      log(CASS_LOG_DEBUG, "notify_ready");
      connect_callback_(this, nullptr);
    }

    void notify_error(Error* err) {
      log(CASS_LOG_DEBUG, "notify_error");
      connect_callback_(this, err);
    }

    void send_options() {
      log(CASS_LOG_DEBUG, "send_options");
      Message message(CQL_OPCODE_OPTIONS);
      execute(&message, nullptr);
    }

    void send_startup() {
      log(CASS_LOG_DEBUG, "send_startup");
      Message      message(CQL_OPCODE_STARTUP);
      BodyStartup* startup = static_cast<BodyStartup*>(message.body.get());
      startup->version = version_;
      execute(&message, nullptr);
    }

    static void on_write(Writer* writer) {
      Request* request = static_cast<Request*>(writer->data());
      ClientConnection* connection = request->connection;

      connection->log(CASS_LOG_DEBUG, "on_write");

      if(request->state == Request::REQUEST_STATE_TIMED_OUT) {
        return;
      }

      request->stop_timer();

      if(writer->status() == Writer::SUCCESS) {
        request->state = Request::REQUEST_STATE_READING;
        request->timer = Timer::start(connection->loop_, connection->read_timeout_, request, on_timeout);
      } else {
        connection->defunct();
        connection->stream_manager_.release_stream(request->stream);
        if(request->future) {
          request->future->set_error(CASS_ERROR(CASS_ERROR_SOURCE_LIBRARY,
                                                CASS_ERROR_LIB_BAD_PARAMS, // TODO(mpenick): valid error
                                                "Write failed"));
        }
        connection->request_count_--;
        delete request;
        fprintf(stderr,
                "Write error %s\n",
                uv_err_name(uv_last_error(connection->loop_)));
      }
    }

    static void on_timeout(Timer* timer) {
      Request* request = static_cast<Request*>(timer->data());
      ClientConnection* connection = request->connection;

      if(request->future) {
        if(request->state == Request::REQUEST_STATE_WRITING) {
        request->future->set_error(CASS_ERROR(CASS_ERROR_SOURCE_LIBRARY,
                                              CASS_ERROR_LIB_BAD_PARAMS, // TODO(mpenick): valid error
                                              "Write timeout"));
        } else if(request->state == Request::REQUEST_STATE_READING) {
          request->future->set_error(CASS_ERROR(CASS_ERROR_SOURCE_LIBRARY,
                                                CASS_ERROR_LIB_BAD_PARAMS, // TODO(mpenick): valid error
                                                "Read timeout"));
        } else {
          assert(false);
        }
      } else {
        connection->notify_error(CASS_ERROR(CASS_ERROR_SOURCE_LIBRARY,
                                            CASS_ERROR_LIB_BAD_PARAMS,
                                            "Timed out during handshake"));
        connection->defunct();
      }

      request->state = Request::REQUEST_STATE_TIMED_OUT;
      request->timer = nullptr;

      // The request can't be deleted here because it could come back
      connection->timed_out_requests_.push_back(request);
      connection->maybe_close();
    }

  private:
    ClientConnectionState       state_;
    int request_count_;

    uv_loop_t*                  loop_;
    std::unique_ptr<Message>    incoming_;
    StreamManager<Request*>     stream_manager_;
    ConnectCallback             connect_callback_;
    CloseCallback               close_callback_;
    LogCallback                 log_callback_;
    // DNS and hostname stuff
    Host                        host_;
    // the actual connection
    uv_tcp_t                    socket_;
    // ssl stuff
    SSLSession*                 ssl_;
    bool                        ssl_handshake_done_;
    // supported stuff sent in start up message
    std::string                 compression_;
    std::string                 version_;


    std::list<Request*> timed_out_requests_;


    uint64_t connect_timeout_;
    uint64_t write_timeout_;
    uint64_t read_timeout_;
    Timer* connect_timer_;

    DISALLOW_COPY_AND_ASSIGN(ClientConnection);
};

} // namespace cass

#endif
