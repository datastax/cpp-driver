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

#ifndef __CASS_CONNECTION_HPP_INCLUDED__
#define __CASS_CONNECTION_HPP_INCLUDED__

#include "buffer.hpp"
#include "cassandra.h"
#include "request_callback.hpp"
#include "hash.hpp"
#include "host.hpp"
#include "list.hpp"
#include "macros.hpp"
#include "metrics.hpp"
#include "ref_counted.hpp"
#include "request.hpp"
#include "response.hpp"
#include "schema_change_callback.hpp"
#include "scoped_ptr.hpp"
#include "ssl.hpp"
#include "stream_manager.hpp"
#include "timer.hpp"

#include <uv.h>

#include <stack>

namespace cass {

class AuthProvider;
class AuthResponseRequest;
class Config;
class Connector;
class EventResponse;
class Request;

class Connection {
public:
  enum ConnectionState {
    CONNECTION_STATE_NEW,
    CONNECTION_STATE_CONNECTING,
    CONNECTION_STATE_CONNECTED,
    CONNECTION_STATE_REGISTERING_EVENTS,
    CONNECTION_STATE_READY,
    CONNECTION_STATE_CLOSE,
    CONNECTION_STATE_CLOSE_DEFUNCT
  };

  enum ConnectionError {
    CONNECTION_OK,
    CONNECTION_ERROR_GENERIC,
    CONNECTION_ERROR_TIMEOUT,
    CONNECTION_ERROR_INVALID_PROTOCOL,
    CONNECTION_ERROR_AUTH,
    CONNECTION_ERROR_SSL_ENCRYPT,
    CONNECTION_ERROR_SSL_DECRYPT,
    CONNECTION_ERROR_SSL_HANDSHAKE,
    CONNECTION_ERROR_SSL_VERIFY,
    CONNECTION_ERROR_KEYSPACE
  };

  class Listener {
  public:
    Listener()
      : event_types_(0) {}

    virtual ~Listener() {}

    int event_types() const { return event_types_; }

    void set_event_types(int event_types) {
      event_types_ = event_types;
    }

    virtual void on_ready(Connection* connection) = 0;
    virtual void on_close(Connection* connection) = 0;

    virtual void on_event(EventResponse* response) = 0;

  private:
    int event_types_;
  };

  Connection(uv_loop_t* loop,
             const Config& config,
             Metrics* metrics,
             const Host::ConstPtr& host,
             const std::string& keyspace,
             int protocol_version,
             Listener* listener);
  ~Connection();

  void connect();

  bool write(const RequestCallback::Ptr& request, bool flush_immediately = true);
  void flush();

  void schedule_schema_agreement(const SchemaChangeCallback::Ptr& callback, uint64_t wait);

  uv_loop_t* loop() { return loop_; }
  const Config& config() const { return config_; }
  Metrics* metrics() { return metrics_; }
  const Address& address() const { return host_->address(); }
  const std::string& address_string() const { return host_->address_string(); }
  const std::string& keyspace() const { return keyspace_; }

  void close();
  void defunct();

  bool is_closing() const {
    return state_ == CONNECTION_STATE_CLOSE ||
        state_ == CONNECTION_STATE_CLOSE_DEFUNCT;
  }

  bool is_ready() const { return state_ == CONNECTION_STATE_READY; }
  bool is_available() const { return is_ready(); }
  bool is_defunct() const { return state_ == CONNECTION_STATE_CLOSE_DEFUNCT; }

  bool is_invalid_protocol() const { return error_code_ == CONNECTION_ERROR_INVALID_PROTOCOL; }
  bool is_auth_error() const { return error_code_ == CONNECTION_ERROR_AUTH; }
  bool is_ssl_error() const {
    return error_code_ == CONNECTION_ERROR_SSL_ENCRYPT ||
        error_code_ == CONNECTION_ERROR_SSL_DECRYPT ||
        error_code_ == CONNECTION_ERROR_SSL_HANDSHAKE ||
        error_code_ == CONNECTION_ERROR_SSL_VERIFY;
  }
  bool is_timeout_error() const { return error_code_ == CONNECTION_ERROR_TIMEOUT; }

  ConnectionError error_code() const { return error_code_; }
  const std::string& error_message() const { return error_message_; }

  CassError ssl_error_code() const { return ssl_error_code_; }

  int protocol_version() const { return protocol_version_; }

  size_t available_streams() const { return stream_manager_.available_streams(); }
  size_t pending_request_count() const { return stream_manager_.pending_streams(); }

  static void on_timeout(Timer* timer);

private:
  class SslHandshakeWriter {
  public:
    static const int MAX_BUFFER_SIZE = 16 * 1024 + 5;

    static bool write(Connection* connection, char* buf, size_t buf_size);

  private:
    SslHandshakeWriter(Connection* connection, char* buf, size_t buf_size);

    static void on_write(uv_write_t* req, int status);

  private:
    uv_write_t req_;
    Connection* connection_;
    uv_buf_t uv_buf_;
    char buf_[MAX_BUFFER_SIZE];
  };

  class StartupCallback : public SimpleRequestCallback {
  public:
    StartupCallback(const Request::ConstPtr& request);

  private:
    virtual void on_internal_set(ResponseMessage* response);
    virtual void on_internal_error(CassError code, const std::string& message);
    virtual void on_internal_timeout();

    void on_result_response(ResponseMessage* response);
  };

  class HeartbeatCallback : public SimpleRequestCallback {
  public:
    HeartbeatCallback();

  private:
    virtual void on_internal_set(ResponseMessage* response);
    virtual void on_internal_error(CassError code, const std::string& message);
    virtual void on_internal_timeout();
  };

  class PendingWriteBase : public List<PendingWriteBase>::Node {
  public:
    PendingWriteBase(Connection* connection)
      : connection_(connection)
      , is_flushed_(false)
      , size_(0) {
      req_.data = this;
    }

    virtual ~PendingWriteBase();

    bool is_flushed() const {
      return is_flushed_;
    }

    size_t size() const {
      return size_;
    }

    int32_t write(RequestCallback* callback);

    virtual void flush() = 0;

  protected:
    static void on_write(uv_write_t* req, int status);

    Connection* connection_;
    uv_write_t req_;
    bool is_flushed_;
    size_t size_;
    BufferVec buffers_;
    List<RequestCallback> callbacks_;
  };

  class PendingWrite : public PendingWriteBase {
  public:
    PendingWrite(Connection* connection)
       : PendingWriteBase(connection) {}

    virtual void flush();
  };

  class PendingWriteSsl : public PendingWriteBase {
  public:
    PendingWriteSsl(Connection* connection)
       : PendingWriteBase(connection)
       , encrypted_size_(0) {}

    void encrypt();
    virtual void flush();

  private:
    size_t encrypted_size_;
    static void on_write(uv_write_t* req, int status);
  };

  struct PendingSchemaAgreement
      : public List<PendingSchemaAgreement>::Node {
    PendingSchemaAgreement(const SchemaChangeCallback::Ptr& callback)
        : callback(callback) { }

    void stop_timer();

    SchemaChangeCallback::Ptr callback;
    Timer timer;
  };

  int32_t internal_write(const RequestCallback::Ptr& request, bool flush_immediately = true);
  void internal_close(ConnectionState close_state);
  void set_state(ConnectionState state);
  void consume(char* input, size_t size);
  void maybe_set_keyspace(ResponseMessage* response);

  static void on_connect(Connector* connecter);
  static void on_connect_timeout(Timer* timer);
  static void on_close(uv_handle_t* handle);

  uv_buf_t internal_alloc_buffer(size_t suggested_size);
  void internal_reuse_buffer(uv_buf_t buf);

#if UV_VERSION_MAJOR == 0
  static uv_buf_t alloc_buffer(uv_handle_t* handle, size_t suggested_size);
  static void on_read(uv_stream_t* client, ssize_t nread, uv_buf_t buf);
  static uv_buf_t alloc_buffer_ssl(uv_handle_t* handle, size_t suggested_size);
  static void on_read_ssl(uv_stream_t* client, ssize_t nread, uv_buf_t buf);
#else
  static void alloc_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
  static void on_read(uv_stream_t* client, ssize_t nread, const uv_buf_t* buf);
  static void alloc_buffer_ssl(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
  static void on_read_ssl(uv_stream_t* client, ssize_t nread, const uv_buf_t *buf);
#endif

  void on_connected();
  void on_authenticate(const std::string& class_name);
  void on_auth_challenge(const AuthResponseRequest* auth_response, const std::string& token);
  void on_auth_success(const AuthResponseRequest* auth_response, const std::string& token);
  void on_ready();
  void on_set_keyspace();
  void on_supported(ResponseMessage* response);
  static void on_pending_schema_agreement(Timer* timer);

  void notify_ready();
  void notify_error(const std::string& message, ConnectionError code = CONNECTION_ERROR_GENERIC);

  void ssl_handshake();

  void send_credentials(const std::string& class_name);
  void send_initial_auth_response(const std::string& class_name);

  void restart_heartbeat_timer();
  static void on_heartbeat(Timer* timer);
  void restart_terminate_timer();
  static void on_terminate(Timer* timer);

private:
  ConnectionState state_;
  ConnectionError error_code_;
  std::string error_message_;
  CassError ssl_error_code_;

  List<PendingWriteBase> pending_writes_;
  List<RequestCallback> pending_reads_;
  List<PendingSchemaAgreement> pending_schema_agreements_;

  uv_loop_t* loop_;
  const Config& config_;
  Metrics* metrics_;
  Host::ConstPtr host_;
  std::string keyspace_;
  const int protocol_version_;
  Listener* listener_;

  ScopedPtr<ResponseMessage> response_;
  StreamManager<RequestCallback*> stream_manager_;

  uv_tcp_t socket_;
  Timer connect_timer_;
  ScopedPtr<SslSession> ssl_session_;

  bool heartbeat_outstanding_;
  Timer heartbeat_timer_;
  Timer terminate_timer_;

  // buffer reuse for libuv
  std::stack<uv_buf_t> buffer_reuse_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(Connection);
};

} // namespace cass

#endif
