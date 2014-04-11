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

#ifndef __CQL_CLIENT_CONNECTION_HPP_INCLUDED__
#define __CQL_CLIENT_CONNECTION_HPP_INCLUDED__

#include "cql_common.hpp"
#include "cql_message.hpp"
#include "cql_request.hpp"
#include "cql_session.hpp"
#include "cql_ssl_context.hpp"
#include "cql_ssl_session.hpp"
#include "cql_stream_storage.hpp"

#define CQL_ADDRESS_MAX_LENGTH 46
#define CQL_STREAM_ID_MAX      127

namespace cql {

struct ClientConnection {
  enum ClientConnectionState {
    CLIENT_STATE_NEW,
    CLIENT_STATE_RESOLVED,
    CLIENT_STATE_CONNECTED,
    CLIENT_STATE_HANDSHAKE,
    CLIENT_STATE_SUPPORTED,
    CLIENT_STATE_READY,
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

  typedef int8_t Stream;

  typedef std::function<void(ClientConnection*,
                             cql::Error*)> ConnectionCallback;

  typedef std::function<void(ClientConnection*,
                             const char*, size_t)> KeyspaceCallback;

  typedef std::function<void(ClientConnection*,
                             SchemaEventType,
                             const char*, size_t,
                             const char*, size_t)> SchemaCallback;

  typedef std::function<void(ClientConnection*,
                             cql::Error*,
                             const char*, size_t,
                             const char*, size_t)> PrepareCallback;

  typedef cql::StreamStorage<
    Stream,
    CallerRequest*,
    CQL_STREAM_ID_MAX> StreamStorageCollection;

  struct WriteRequestData {
    uv_buf_t buf;
    ClientConnection* connection;
  };

  ClientConnectionState         state_;
  uv_loop_t*                    loop_;
  std::unique_ptr<cql::Message> incomming_;
  StreamStorageCollection       stream_storage_;
  ConnectionCallback            connect_callback_;
  KeyspaceCallback              keyspace_callback_;
  PrepareCallback               prepare_callback_;
  LogCallback                   log_callback_;

  // DNS and hostname stuff
  struct sockaddr_in       address_;
  char*                    address_string_[CQL_ADDRESS_MAX_LENGTH];
  int                      address_family_;
  std::string              hostname_;
  std::string              port_;
  uv_getaddrinfo_t         resolver_;
  struct addrinfo          resolver_hints_;

  // the actual connection
  uv_connect_t             connect_request_;
  uv_tcp_t                 socket_;
  // ssl stuff
  SSLSession*              ssl_;
  bool                     ssl_handshake_done_;

  // supported stuff sent in start up message
  std::string              compression_;
  std::string              cql_version_;


  explicit
  ClientConnection(
      uv_loop_t* loop,
      cql::SSLSession* ssl_session) :
      state_(CLIENT_STATE_NEW),
      loop_(loop),
      incomming_(new Message()),
      connect_callback_(nullptr),
      keyspace_callback_(nullptr),
      prepare_callback_(nullptr),
      log_callback_(nullptr),
      address_family_(PF_INET),         // use ipv4 by default
      hostname_("localhost"),
      port_("9042"),
      ssl_(ssl_session),
      ssl_handshake_done_(false),
      cql_version_("3.0.0") {
    resolver_.data = this;
    connect_request_.data = this;
    socket_.data = this;

    resolver_hints_.ai_family = address_family_;
    resolver_hints_.ai_socktype = SOCK_STREAM;
    resolver_hints_.ai_protocol = IPPROTO_TCP;
    resolver_hints_.ai_flags = 0;
    memset(address_string_, 0, sizeof(address_string_));
    if (ssl_) {
      ssl_->init();
      ssl_->handshake(true);
    }
  }

  inline void
  log(
      int         level,
      const char* message,
      size_t      size) {
    if (log_callback_) {
      log_callback_(level, message, size);
    }
  }

  inline void
  log(
      int         level,
      const char* message) {
    if (log_callback_) {
      log_callback_(level, message, strlen(message));
    }
  }

  inline size_t
  available_streams() {
    return stream_storage_.available_streams();
  }

  void
  event_received() {
    log(CQL_LOG_DEBUG, "event received");

    switch (state_) {
      case CLIENT_STATE_NEW:
        resolve();
        break;
      case CLIENT_STATE_RESOLVED:
        connect();
        break;
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
      default:
        assert(false);
    }
  }

  void
  consume(
      char*  input,
      size_t size) {
    char* buffer    = input;
    int   remaining = size;

    while (remaining != 0) {
      int consumed = incomming_->consume(buffer, remaining);
      if (consumed < 0) {
        // TODO(mstump)
        fprintf(stderr, "consume error\n");
      }

      if (incomming_->body_ready) {
        Message* message = incomming_.release();
        incomming_.reset(new Message());

        char log_message[512];
        snprintf(
            log_message,
            sizeof(log_message),
            "consumed message type %s with stream %d, input %zd, remaining %d",
            opcode_to_string(message->opcode).c_str(),
            message->stream,
            size,
            remaining);

        log(CQL_LOG_DEBUG, log_message);
        if (message->stream < 0) {
          // system event
          // TODO(mstump)
          assert(false);
        } else {
          switch (message->opcode) {
            case CQL_OPCODE_SUPPORTED:
              on_supported(message);
              break;
            case CQL_OPCODE_ERROR:
              on_error(message);
              break;
            case CQL_OPCODE_READY:
              on_ready(message);
              break;
            case CQL_OPCODE_RESULT:
              on_result(message);
              break;
            default:
              assert(false);
              break;
          }
        }
      }
      remaining -= consumed;
      buffer    += consumed;
    }
  }

  static void
  on_close(
      uv_handle_t* client) {
    ClientConnection* connection
        = reinterpret_cast<ClientConnection*>(client->data);

    connection->log(CQL_LOG_DEBUG, "on_close");
    connection->state_ = CLIENT_STATE_DISCONNECTED;
    connection->event_received();
  }

  static void
  on_read(
      uv_stream_t* client,
      ssize_t      nread,
      uv_buf_t     buf) {
    ClientConnection* connection =
        reinterpret_cast<ClientConnection*>(client->data);

    connection->log(CQL_LOG_DEBUG, "on_read");
    if (nread == -1) {
      if (uv_last_error(connection->loop_).code != UV_EOF) {
        fprintf(stderr,
                "Read error %s\n",
                uv_err_name(uv_last_error(connection->loop_)));
      }
      connection->close();
      return;
    }

    if (connection->ssl_) {
      char*  read_input        = buf.base;
      size_t read_input_size   = nread;

      for (;;) {
        size_t read_size         = 0;
        char*  read_output       = NULL;
        size_t read_output_size  = 0;
        char*  write_output      = NULL;
        size_t write_output_size = 0;

        // TODO(mstump) error handling
        connection->ssl_->read_write(
            read_input,
            read_input_size,
            read_size,
            &read_output,
            read_output_size,
            NULL,
            0,
            &write_output,
            write_output_size);

        if (read_output && read_output_size) {
          // TODO(mstump) error handling
          connection->consume(read_output, read_output_size);
          delete read_output;
        }

        if (write_output && write_output_size) {
          connection->send_data(write_output, write_output_size);
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

  Error*
  send_data(
      char*  input,
      size_t size) {
    return send_data(uv_buf_init(input, size));
  }

  Error*
  send_data(
      uv_buf_t buf) {
    uv_write_t        *req  = new uv_write_t;
    WriteRequestData*  data = new WriteRequestData;
    data->buf               = buf;
    data->connection        = this;
    req->data               = data;
    uv_write(
        req,
        reinterpret_cast<uv_stream_t*>(&socket_),
        &buf,
        1,
        ClientConnection::on_write);
    return CQL_ERROR_NO_ERROR;
  }

  void
  close() {
    log(CQL_LOG_DEBUG, "close");
    state_ = CLIENT_STATE_DISCONNECTING;
    uv_close(
        reinterpret_cast<uv_handle_t*>(&socket_),
        ClientConnection::on_close);
  }

  static void
  on_connect(
      uv_connect_t*     request,
      int               status) {
    ClientConnection* connection
        = reinterpret_cast<ClientConnection*>(request->data);

    connection->log(CQL_LOG_DEBUG, "on_connect");
    if (status == -1) {
      // TODO(mstump)
      fprintf(
          stderr,
          "connect failed error %s\n",
          uv_err_name(uv_last_error(connection->loop_)));
      return;
    }

    uv_read_start(
        reinterpret_cast<uv_stream_t*>(&connection->socket_),
        alloc_buffer,
        on_read);

    connection->state_ = CLIENT_STATE_CONNECTED;
    connection->event_received();
  }

  void
  connect() {
    log(CQL_LOG_DEBUG, "connect");
    // connect to the resolved host
    uv_tcp_init(loop_, &socket_);
    uv_tcp_connect(
        &connect_request_,
        &socket_,
        address_,
        ClientConnection::on_connect);
  }

  void
  ssl_handshake() {
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

  void
  on_result(
      Message* response) {
    log(CQL_LOG_DEBUG, "on_result");

    Error*         err     = NULL;
    CallerRequest* request = NULL;
    BodyResult*    result  = static_cast<BodyResult*>(response->body.get());

    switch (result->kind) {
      case CQL_RESULT_KIND_SET_KEYSPACE:
        if (keyspace_callback_) {
          keyspace_callback_(this, result->keyspace, result->keyspace_size);
        }
        break;

      case CQL_RESULT_KIND_PREPARED:
        err = stream_storage_.get_stream(response->stream, request);
        if (prepare_callback_) {
          if (!err) {
            prepare_callback_(
                this,
                NULL,
                request->data.c_str(),
                request->data.size(),
                result->prepared,
                result->prepared_size);
          } else {
            prepare_callback_(
                this,
                err,
                request->data.c_str(),
                request->data.size(),
                result->prepared,
                result->prepared_size);
          }
        }

        if (!err) {
          request->result = response;
          request->notify(loop_);
        } else {
          delete err;
        }
        break;

      default:
        err = stream_storage_.get_stream(response->stream, request);
        if (!err) {
          request->result = response;
          request->notify(loop_);
        } else {
          if (connect_callback_) {
            connect_callback_(this, err);
          }
        }
        break;
    }
  }

  void
  on_error(
      Message* response) {
    log(CQL_LOG_DEBUG, "on_error");
    BodyError* error = static_cast<BodyError*>(response->body.get());

    if (state_ < CLIENT_STATE_READY) {
      notify_error(
          new Error(
              CQL_ERROR_SOURCE_SERVER,
              0,
              error->message,
              __FILE__,
              __LINE__));
    }
    delete response;
  }

  void
  on_ready(
      Message* response) {
    log(CQL_LOG_DEBUG, "on_ready");
    delete response;
    state_ = CLIENT_STATE_READY;
    event_received();
  }

  void
  on_supported(
      Message* response) {
    log(CQL_LOG_DEBUG, "on_supported");
    BodySupported* supported
        = static_cast<BodySupported*>(response->body.get());

    // TODO(mstump) do something with the supported info
    (void) supported;

    delete response;
    state_ = CLIENT_STATE_SUPPORTED;
    event_received();
  }

  void
  set_keyspace(
      const std::string& keyspace) {
    Message    message(CQL_OPCODE_QUERY);
    BodyQuery* query = static_cast<BodyQuery*>(message.body.get());
    query->query_string("USE " + keyspace);
    send_message(&message, NULL);
  }

  void
  notify_ready() {
    log(CQL_LOG_DEBUG, "notify_ready");
    if (connect_callback_) {
      connect_callback_(this, NULL);
    }
  }

  void
  notify_error(
      Error* err) {
    log(CQL_LOG_DEBUG, "notify_error");
    if (connect_callback_) {
      connect_callback_(this, err);
    }
  }

  void
  send_options() {
    log(CQL_LOG_DEBUG, "send_options");
    Message message(CQL_OPCODE_OPTIONS);
    send_message(&message, NULL);
  }

  void
  send_startup() {
    log(CQL_LOG_DEBUG, "send_startup");
    Message      message(CQL_OPCODE_STARTUP);
    BodyStartup* startup = static_cast<BodyStartup*>(message.body.get());
    startup->cql_version = cql_version_;
    send_message(&message, NULL);
  }

  static void
  on_write(
      uv_write_t* req,
      int         status) {
    WriteRequestData* data
        = reinterpret_cast<WriteRequestData*>(req->data);

    ClientConnection* connection = data->connection;
    connection->log(CQL_LOG_DEBUG, "on_write");
    if (status == -1) {
      fprintf(
          stderr,
          "Write error %s\n",
          uv_err_name(uv_last_error(connection->loop_)));
    }
    delete data->buf.base;
    delete data;
    delete req;
  }

  CallerRequest*
  prepare(
      const char*             statement,
      size_t                  size,
      CallerRequest::Callback callback = NULL) {
    CallerRequest* request = new CallerRequest();
    Message*       message = new Message(CQL_OPCODE_PREPARE);
    BodyPrepare*   prepare = static_cast<BodyPrepare*>(message->body.get());
    prepare->prepare_string(statement, size);

    request->callback = callback;
    request->data.assign(statement, size);

    Error* err = send_message(message, request);
    if (err) {
      request->error = err;
      request->notify(loop_);
    }
    return request;
  }

  CallerRequest*
  exec(
      Message*                message,
      CallerRequest::Callback callback = NULL) {
    CallerRequest* request = new CallerRequest();
    request->callback = callback;
    Error* err = send_message(message, request);
    if (err) {
      request->error = err;
      request->notify(loop_);
    }
    return request;
  }

  Error*
  send_message(
      Message* message,
      CallerRequest* request = NULL) {
    uv_buf_t   buf;
    Error*     err = stream_storage_.set_stream(request, message->stream);
    if (err) {
      return err;
    }
    message->prepare(&buf.base, buf.len);

    char log_message[512];
    snprintf(
        log_message,
        sizeof(log_message),
        "sending message type %s with stream %d, size %zd",
        opcode_to_string(message->opcode).c_str(),
        message->stream,
        buf.len);

    log(CQL_LOG_DEBUG, log_message);
    return send_data(buf);
  }

  static void
  on_resolve(
      uv_getaddrinfo_t* resolver_,
      int               status,
      struct addrinfo*  res) {
    ClientConnection* connection
        = reinterpret_cast<ClientConnection*>(resolver_->data);

    connection->log(CQL_LOG_DEBUG, "on_resolve");
    if (status == -1) {
      // TODO(mstump)
      fprintf(
          stderr,
          "getaddrinfo request error %s\n",
          uv_err_name(uv_last_error(connection->loop_)));
      return;
    }

    // store the human readable address_
    if (res->ai_family == AF_INET) {
      uv_ip4_name((struct sockaddr_in*) res->ai_addr,
                  reinterpret_cast<char*>(connection->address_string_),
                  CQL_ADDRESS_MAX_LENGTH);
    } else if (res->ai_family == AF_INET6) {
      uv_ip6_name((struct sockaddr_in6*) res->ai_addr,
                  reinterpret_cast<char*>(connection->address_string_),
                  CQL_ADDRESS_MAX_LENGTH);
    }
    connection->address_ = *(struct sockaddr_in*) res->ai_addr;
    uv_freeaddrinfo(res);

    connection->state_ = CLIENT_STATE_RESOLVED;
    connection->event_received();
  }

  void
  resolve() {
    log(CQL_LOG_DEBUG, "resolve");
    uv_getaddrinfo(
        loop_,
        &resolver_,
        ClientConnection::on_resolve,
        hostname_.c_str(),
        port_.c_str(),
        &resolver_hints_);
  }

  void
  init(
      ConnectionCallback connect     = NULL,
      KeyspaceCallback   keyspace    = NULL
      // SchemaCallback     schema   = NULL,
      // TopologyCallback   topology = NULL,
      // StatusCallback     status   = NULL
       ) {
    connect_callback_     = connect;
    keyspace_callback_    = keyspace;

    // schema_callback   = schema;
    // topology_callback = topology;
    // status_callback   = status;
    event_received();
  }

 private:
  ClientConnection(const ClientConnection&) {}
  void operator=(const ClientConnection&) {}
};
}
#endif
