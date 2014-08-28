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
#include "request.hpp"
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
class Logger;
class RequestHandler;
class Config;

class Pool {
public:
  typedef boost::function1<void, Pool*> Callback;
  typedef boost::function1<void, const std::string&> KeyspaceCallback;

  enum PoolState {
    POOL_STATE_NEW,
    POOL_STATE_CONNECTING,
    POOL_STATE_READY,
    POOL_STATE_CLOSING,
    POOL_STATE_CLOSED
  };

  Pool(const Address& address, uv_loop_t* loop,
       Logger* logger, const Config& config);

  ~Pool();

  void connect(const std::string& keyspace);
  void close();

  bool execute(Connection* connection, RequestHandler* request_handler);
  bool wait_for_connection(RequestHandler* request_handler);
  Connection* borrow_connection(const std::string& keyspace);

  const Address& address() const { return address_; }

  bool is_defunct() const { return is_defunct_; }
  bool is_critical_failure() const { return is_critical_failure_; }

  void set_ready_callback(Callback callback) { ready_callback_ = callback; }
  void set_closed_callback(Callback callback) { closed_callback_ = callback; }
  void set_keyspace_callback(KeyspaceCallback callback) {
    keyspace_callback_ = callback;
  }

private:
  friend class RequestHandler;

  void defunct();
  void maybe_notify_ready(Connection* connection);
  void maybe_close();
  void spawn_connection(const std::string& keyspace);
  void maybe_spawn_connection(const std::string& keyspace);

  void on_connection_ready(Connection* connection);
  void on_connection_closed(Connection* connection);
  void on_timeout(void* data);

  Connection* find_least_busy();

  void return_connection(Connection* connection);

private:
  typedef std::set<Connection*> ConnectionSet;
  typedef std::vector<Connection*> ConnectionVec;
  typedef std::list<RequestHandler*> RequestHandlerList;

  PoolState state_;
  Address address_;
  uv_loop_t* loop_;
  Logger* logger_;
  const Config& config_;
  int protocol_version_;
  ConnectionVec connections_;
  ConnectionSet connections_pending_;
  RequestHandlerList pending_request_queue_;
  bool is_defunct_;
  bool is_critical_failure_;

  Callback ready_callback_;
  Callback closed_callback_;
  KeyspaceCallback keyspace_callback_;
};

} // namespace cass

#endif
