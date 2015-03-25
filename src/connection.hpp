/*
  Copyright (c) 2014-2015 DataStax

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

#include "address.hpp"
#include "buffer.hpp"
#include "cassandra.h"
#include "handler.hpp"
#include "list.hpp"
#include "macros.hpp"
#include "metrics.hpp"
#include "ref_counted.hpp"
#include "request.hpp"
#include "response.hpp"
#include "schema_change_handler.hpp"
#include "scoped_ptr.hpp"
#include "ssl.hpp"
#include "stream_manager.hpp"

#include <uv.h>

namespace cass {

class AuthProvider;
class AuthResponseRequest;
class Config;
class Connector;
class EventResponse;
class Request;
class Timer;

class Connection {
public:
  enum ConnectionState {
    CONNECTION_STATE_NEW,
    CONNECTION_STATE_CONNECTING,
    CONNECTION_STATE_CONNECTED,
    CONNECTION_STATE_READY,
    CONNECTION_STATE_CLOSING,
    CONNECTION_STATE_CLOSED
  };

  class Listener {
  public:
    Listener(int event_types = 0)
      : event_types_(event_types) {}

    virtual ~Listener() {}

    int event_types() const { return event_types_; }

    virtual void on_ready(Connection* connection) = 0;
    virtual void on_close(Connection* connection) = 0;
    virtual void on_availability_change(Connection* connection) = 0;

    virtual void on_event(EventResponse* response) = 0;

  private:
    const int event_types_;
  };

  Connection(uv_loop_t* loop,
             const Config& config,
             Metrics* metrics,
             const Address& address,
             const std::string& keyspace,
             int protocol_version,
             Listener* listener);

  void connect();

  bool write(Handler* request, bool flush_immediately = true);
  void flush();

  void schedule_schema_agreement(const SharedRefPtr<SchemaChangeHandler>& handler, uint64_t wait);

  const Config& config() const { return config_; }
  Metrics* metrics() { return metrics_; }
  const Address& address() { return address_; }
  const std::string& address_string() { return addr_string_; }
  const std::string& keyspace() { return keyspace_; }

  void close();
  void defunct();

  bool is_closing() const { return state_ == CONNECTION_STATE_CLOSING; }
  bool is_ready() const { return state_ == CONNECTION_STATE_READY; }
  bool is_available() const { return is_available_; }
  bool is_defunct() const { return is_defunct_; }
  bool is_invalid_protocol() const { return is_invalid_protocol_; }
  bool is_critical_failure() const {
    return is_invalid_protocol_ || !auth_error_.empty() || !ssl_error_.empty();
  }

  const std::string& auth_error() const { return auth_error_; }
  const std::string& ssl_error() const { return ssl_error_; }
  CassError ssl_error_code() const { return ssl_error_code_; }

  int protocol_version() const { return protocol_version_; }

  size_t available_streams() const { return stream_manager_.available_streams(); }
  size_t pending_request_count() const { return stream_manager_.pending_streams(); }

  static void on_timeout(RequestTimer* timer);

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

  class StartupHandler : public Handler {
  public:
    StartupHandler(Connection* connection, Request* request)
        : connection_(connection)
        , request_(request) {}

    const Request* request() const {
      return request_.get();
    }

    virtual void on_set(ResponseMessage* response);
    virtual void on_error(CassError code, const std::string& message);
    virtual void on_timeout();

  private:
    void on_result_response(ResponseMessage* response);

    Connection* connection_;
    ScopedRefPtr<Request> request_;
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

    int32_t write(Handler* handler);

    virtual void flush() = 0;

  protected:
    static void on_write(uv_write_t* req, int status);

    Connection* connection_;
    uv_write_t req_;
    bool is_flushed_;
    size_t size_;
    BufferVec buffers_;
    UvBufVec uv_bufs_;
    List<Handler> handlers_;
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
    PendingSchemaAgreement(const SharedRefPtr<SchemaChangeHandler>& handler)
        : handler(handler)
        , timer(NULL) {}

    void stop_timer();

    SharedRefPtr<SchemaChangeHandler> handler;
    Timer* timer;
  };

  void set_is_available(bool is_available);
  void actually_close();
  void consume(char* input, size_t size);
  void maybe_set_keyspace(ResponseMessage* response);

  static void on_connect(Connector* connecter);
  static void on_connect_timeout(Timer* timer);
  static void on_close(uv_handle_t* handle);

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
  void on_authenticate();
  void on_auth_challenge(AuthResponseRequest* auth_response, const std::string& token);
  void on_auth_success(AuthResponseRequest* auth_response, const std::string& token);
  void on_ready();
  void on_set_keyspace();
  void on_supported(ResponseMessage* response);
  static void on_pending_schema_agreement(Timer* timer);

  void stop_connect_timer();
  void notify_ready();
  void notify_error(const std::string& error);
  void notify_error_ssl(const std::string& error);

  void ssl_handshake();

  void send_credentials();
  void send_initial_auth_response();

private:
  ConnectionState state_;
  bool is_defunct_;
  bool is_invalid_protocol_;
  bool is_registered_for_events_;
  bool is_available_;

  std::string auth_error_;
  std::string ssl_error_;
  CassError ssl_error_code_;

  size_t pending_writes_size_;
  List<PendingWriteBase> pending_writes_;
  List<Handler> pending_reads_;
  List<PendingSchemaAgreement> pending_schema_agreements_;

  uv_loop_t* loop_;
  const Config& config_;
  Metrics* metrics_;
  Address address_;
  std::string addr_string_;
  std::string keyspace_;
  const int protocol_version_;
  Listener* listener_;

  ScopedPtr<ResponseMessage> response_;
  StreamManager<Handler*> stream_manager_;

  // the actual connection
  uv_tcp_t socket_;
  // supported stuff sent in start up message
  std::string compression_;
  std::string version_;

  Timer* connect_timer_;
  ScopedPtr<SslSession> ssl_session_;

private:
  DISALLOW_COPY_AND_ASSIGN(Connection);
};

} // namespace cass

#endif
