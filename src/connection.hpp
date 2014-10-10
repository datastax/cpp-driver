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

#include "address.hpp"
#include "buffer.hpp"
#include "cassandra.h"
#include "handler.hpp"
#include "list.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"
#include "request.hpp"
#include "response.hpp"
#include "schema_change_handler.hpp"
#include "scoped_ptr.hpp"
#include "stream_manager.hpp"

#include "third_party/boost/boost/cstdint.hpp"
#include "third_party/boost/boost/function.hpp"

#include <uv.h>

namespace cass {

class AuthProvider;
class AuthResponseRequest;
class Config;
class Connecter;
class EventResponse;
class Logger;
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

  typedef boost::function1<void, EventResponse*> EventCallback;
  typedef boost::function1<void, Connection*> Callback;

  Connection(uv_loop_t* loop, Logger* logger, const Config& config,
             const Address& address, const std::string& keyspace,
             int protocol_version);

  void connect();

  bool write(Handler* request, bool flush_immediately = true);
  void flush();

  void schedule_schema_agreement(const SharedRefPtr<SchemaChangeHandler>& handler, uint64_t wait);

  Logger* logger() const { return logger_; }
  const Config& config() const { return config_; }
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
  bool is_critical_failure() const { return is_invalid_protocol_ || !auth_error_.empty(); }

  const std::string& auth_error() { return auth_error_; }

  int protocol_version() const { return protocol_version_; }

  void set_ready_callback(Callback callback) { ready_callback_ = callback; }
  void set_close_callback(Callback callback) { closed_callback_ = callback; }
  void set_availability_changed_callback(Callback callback) {
    availability_changed_callback_ = callback;
  }

  void set_event_callback(int types, EventCallback callback) {
    event_types_ = types;
    event_callback_ = callback;
  }

  size_t available_streams() const { return stream_manager_.available_streams(); }
  size_t pending_request_count() const { return stream_manager_.pending_streams(); }

  void on_timeout(RequestTimer* timer);

private:
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

  class PendingWriteBuffer : public List<PendingWriteBuffer>::Node {
  public:
    PendingWriteBuffer(Connection* connection)
       : connection_(connection)
       , is_flushed_(false)
       , size_(0) {
      req_.data = this;
    }

    ~PendingWriteBuffer();

    bool is_flushed() const {
      return is_flushed_;
    }

    size_t size() const {
      return size_;
    }

    int32_t write(Handler* handler);
    void flush();

  private:
    static void on_write(uv_write_t* req, int status);

    Connection* connection_;
    uv_write_t req_;
    bool is_flushed_;
    size_t size_;
    BufferVec buffers_;
    UvBufVec uv_bufs_;
    List<Handler> handlers_;
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
  void maybe_write_buffers();


  static void on_connect(Connecter* connecter);
  static void on_connect_timeout(Timer* timer);
  static void on_close(uv_handle_t* handle);
  static void on_read(uv_stream_t* client, ssize_t nread, uv_buf_t buf);

  void on_connected();
  void on_authenticate();
  void on_auth_challenge(AuthResponseRequest* auth_response, const std::string& token);
  void on_auth_success(AuthResponseRequest* auth_response, const std::string& token);
  void on_ready();
  void on_set_keyspace();
  void on_supported(ResponseMessage* response);
  void on_pending_schema_agreement(Timer* timer);

  void notify_ready();
  void notify_error(const std::string& error);

  void send_credentials();
  void send_initial_auth_response();

private:
  ConnectionState state_;
  bool is_defunct_;
  bool is_invalid_protocol_;
  std::string auth_error_;
  bool is_registered_for_events_;
  bool is_available_;

  size_t pending_writes_size_;
  List<PendingWriteBuffer> pending_writes_;
  List<Handler> pending_reads_;
  List<PendingSchemaAgreement> pending_schema_aggreements_;

  uv_loop_t* loop_;
  Logger* logger_;
  const Config& config_;
  Address address_;
  std::string addr_string_;
  std::string keyspace_;
  const int protocol_version_;

  ScopedPtr<ResponseMessage> response_;
  StreamManager<Handler*> stream_manager_;

  Callback ready_callback_;
  Callback closed_callback_;
  Callback availability_changed_callback_;
  EventCallback event_callback_;

  // the actual connection
  uv_tcp_t socket_;
  // ssl stuff
  bool ssl_handshake_done_;
  // supported stuff sent in start up message
  std::string compression_;
  std::string version_;
  int event_types_;

  Timer* connect_timer_;

private:
  DISALLOW_COPY_AND_ASSIGN(Connection);
};

} // namespace cass

#endif
