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

#include "cassandra.h"
#include "buffer.hpp"
#include "handler.hpp"
#include "macros.hpp"
#include "host.hpp"
#include "stream_manager.hpp"
#include "list.hpp"
#include "scoped_ptr.hpp"
#include "ref_counted.hpp"
#include "response.hpp"
#include "request.hpp"

#include "third_party/boost/boost/function.hpp"

#include <uv.h>

namespace cass {

class AuthProvider;
class AuthResponseRequest;
class Connecter;
class Timer;
class Config;
class Logger;
class Request;

class Connection {
public:
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

  typedef boost::function1<void, Connection*> Callback;

public:
  enum ConnectionState {
    CONNECTION_STATE_NEW,
    CONNECTION_STATE_CONNECTING,
    CONNECTION_STATE_CONNECTED,
    CONNECTION_STATE_READY,
    CONNECTION_STATE_CLOSING,
    CONNECTION_STATE_CLOSED
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

  Connection(uv_loop_t* loop, const Host& host,
             Logger* logger, const Config& config, const std::string& keyspace,
             int protocol_version);

  void connect();

  bool execute(Handler* request);

  const Config& config() const { return config_; }

  const std::string& keyspace() { return keyspace_; }

  void close();
  void defunct();

  bool is_closing() const { return state_ == CONNECTION_STATE_CLOSING; }
  bool is_ready() const { return state_ == CONNECTION_STATE_READY; }
  bool is_defunct() const { return is_defunct_; }
  bool is_invalid_protocol() const { return is_invalid_protocol_; }

  int protocol_version() const { return protocol_version_; }

  void set_ready_callback(Callback callback) { ready_callback_ = callback; }
  void set_close_callback(Callback callback) { closed_callback_ = callback; }

  size_t available_streams() { return stream_manager_.available_streams(); }
  size_t pending_request_count() { return pending_requests_.size(); }

  void on_write(RequestWriter* writer);
  void on_timeout(RequestTimer* timer);

private:
  void actually_close();
  void consume(char* input, size_t size);
  void maybe_set_keyspace(ResponseMessage* response);

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

  void notify_ready();
  void notify_error(const std::string& error);

  void send_credentials();
  void send_initial_auth_response();

private:
  ConnectionState state_;
  bool is_defunct_;
  bool is_invalid_protocol_;

  List<Handler> pending_requests_;

  uv_loop_t* loop_;
  ScopedPtr<ResponseMessage> response_;
  StreamManager<Handler*> stream_manager_;

  Callback ready_callback_;
  Callback closed_callback_;

  // DNS and hostname stuff
  Host host_;
  std::string host_string_;
  // the actual connection
  uv_tcp_t socket_;
  // ssl stuff
  bool ssl_handshake_done_;
  // supported stuff sent in start up message
  std::string compression_;
  std::string version_;
  const int protocol_version_;

  Logger* logger_;
  const Config& config_;
  std::string keyspace_;
  Timer* connect_timer_;

private:
  DISALLOW_COPY_AND_ASSIGN(Connection);
};

} // namespace cass

#endif
