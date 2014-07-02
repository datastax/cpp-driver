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

#ifndef __CASS_POOL_HPP_INCLUDED__
#define __CASS_POOL_HPP_INCLUDED__

#include "cassandra.h"
#include "response_callback.hpp"
#include "scoped_ptr.hpp"
#include "request_handler.hpp"

#include "third_party/boost/boost/function.hpp"

#include <vector>
#include <set>
#include <string>
#include <algorithm>
#include <functional>

namespace cass {

class Connection;
class SSLContext;
class Logger;
class RequestHandler;
class Config;

class Pool {
public:
  typedef boost::function1<void, Pool*> Callback;
  typedef boost::function1<void, const std::string&> KeyspaceCallback;

  class PoolHandler : public ResponseCallback {
  public:
    PoolHandler(Pool* pool, Connection* connection,
                RequestHandler* request_handler);

    virtual Request* request() const { return request_handler_->request(); }

    virtual void on_set(ResponseMessage* response);
    virtual void on_error(CassError code, const std::string& message);
    virtual void on_timeout();

  private:
    void finish_request();

    void on_result_response(ResponseMessage* response);
    void on_error_response(ResponseMessage* response);

    Pool* pool_;
    Connection* connection_;
    ScopedPtr<RequestHandler> request_handler_;
  };

  enum PoolState {
    POOL_STATE_NEW,
    POOL_STATE_CONNECTING,
    POOL_STATE_READY,
    POOL_STATE_CLOSING,
    POOL_STATE_CLOSED
  };

  Pool(const Host& host, uv_loop_t* loop, SSLContext* ssl_context,
       Logger* logger, const Config& config);

  ~Pool();

  void connect(const std::string& keyspace);
  void close();

  bool execute(Connection* connection, RequestHandler* request_handler);
  bool wait_for_connection(RequestHandler* request_handler);
  Connection* borrow_connection(const std::string& keyspace);

  Host host() const { return host_; }
  bool is_defunct() { return is_defunct_; }

  void set_ready_callback(Callback callback) { ready_callback_ = callback; }

  void set_closed_callback(Callback callback) { closed_callback_ = callback; }

  void set_keyspace_callback(KeyspaceCallback callback) {
    keyspace_callback_ = callback;
  }

private:
  void defunct();
  void maybe_notify_ready(Connection* connection);
  void maybe_close();
  void spawn_connection(const std::string& keyspace);
  void maybe_spawn_connection(const std::string& keyspace);

  void on_connection_ready(Connection* connection);
  void on_connection_closed(Connection* connection);
  void on_timeout(Timer* timer);

  Connection* find_least_busy();

  void execute_pending_request(Connection* connection);

private:
  typedef std::set<Connection*> ConnectionSet;
  typedef std::vector<Connection*> ConnectionVec;
  typedef std::list<RequestHandler*> RequestHandlerList;

  PoolState state_;
  Host host_;
  uv_loop_t* loop_;
  SSLContext* ssl_context_;
  Logger* logger_;
  const Config& config_;
  int protocol_version_;
  ConnectionVec connections_;
  ConnectionSet connections_pending_;
  RequestHandlerList pending_request_queue_;
  bool is_defunct_;

  Callback ready_callback_;
  Callback closed_callback_;
  KeyspaceCallback keyspace_callback_;
};

} // namespace cass

#endif
