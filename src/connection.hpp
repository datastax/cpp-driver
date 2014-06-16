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
    StartupHandler(Connection* connection, Message* request);

    virtual Message* request() const;
    virtual void on_set(Message* response);
    virtual void on_error(CassError code, const std::string& message);
    virtual void on_timeout();

  private:
    void on_result_response(Message* response);

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
      REQUEST_STATE_DONE
    };

    Request(Connection* connection, ResponseCallback* response_callback);

    void on_set(Message* response);
    void on_error(CassError code, const std::string& message);
    void on_timeout();

    State state() const { return state_; }

    void change_state(State next_state);

    void stop_timer() {
      assert(timer_ != nullptr);
      Timer::stop(timer_);
      timer_ = nullptr;
    }

    Connection* connection;
    int8_t stream;

  private:
    void cleanup();
    void on_result_response(Message* response);
    static void on_request_timeout(Timer* timer);

  private:
    std::unique_ptr<ResponseCallback> response_callback_;
    Timer* timer_;
    State state_;
  };

  typedef std::function<void(Connection*)> Callback;

public:
  enum ConnectionState {
    CLIENT_STATE_NEW,
    CLIENT_STATE_CONNECTING,
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

  Connection(uv_loop_t* loop, SSLSession* ssl_session, const Host& host,
             Logger* logger, const Config& config, const std::string& keyspace);

  void connect();

  bool execute(ResponseCallback* response_callback);

  const std::string& keyspace() { return keyspace_; }

  void close();
  void defunct();

  bool is_closing() const { return state_ == CLIENT_STATE_CLOSING; }

  bool is_defunct() const { return is_defunct_; }

  bool is_ready() { return state_ == CLIENT_STATE_READY; }

  void set_ready_callback(Callback callback) { ready_callback_ = callback; }

  void set_close_callback(Callback callback) { closed_callback_ = callback; }

  size_t available_streams() {
    return stream_manager_.available_streams();
  }

  bool has_requests_pending() {
    return pending_requests_.size() - timed_out_request_count_ > 0;
  }

private:
  void actually_close();
  void write(uv_buf_t buf, Request* request);
  void event_received();
  void consume(char* input, size_t size);

  static void on_connect(Connecter* connecter);
  static void on_connect_timeout(Timer* timer);
  static void on_close(uv_handle_t* handle);
  static void on_read(uv_stream_t* client, ssize_t nread, uv_buf_t buf);
  static void on_write(Writer* writer);

  void ssl_handshake();

  void on_ready();
  void on_set_keyspace();
  void on_supported(Message* response);

  void notify_ready();
  void notify_error(const std::string& error);

  void send_options();
  void send_startup();
  void send_use_keyspace();

private:
  ConnectionState state_;
  bool is_defunct_;

  List<Request> pending_requests_;
  int timed_out_request_count_;

  uv_loop_t* loop_;
  std::unique_ptr<Message> incoming_;
  StreamManager<Request*> stream_manager_;

  Callback ready_callback_;
  Callback closed_callback_;

  // DNS and hostname stuff
  Host host_;
  std::string host_string_;
  // the actual connection
  uv_tcp_t socket_;
  // ssl stuff
  SSLSession* ssl_;
  bool ssl_handshake_done_;
  // supported stuff sent in start up message
  std::string compression_;
  std::string version_;

  Logger* logger_;
  const Config& config_;
  std::string keyspace_;
  Timer* connect_timer_;

  DISALLOW_COPY_AND_ASSIGN(Connection);
};

} // namespace cass

#endif
