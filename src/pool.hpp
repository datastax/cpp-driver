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
#include "ref_counted.hpp"
#include "request.hpp"
#include "request_handler.hpp"
#include "scoped_ptr.hpp"

#include <algorithm>
#include <functional>
#include <set>
#include <string>

namespace cass {

class Connection;
class IOWorker;
class Logger;
class RequestHandler;
class Config;

class Pool : public RefCounted<Pool> {
public:
  enum PoolState {
    POOL_STATE_NEW,
    POOL_STATE_CONNECTING,
    POOL_STATE_READY,
    POOL_STATE_CLOSING,
    POOL_STATE_CLOSED
  };

  Pool(IOWorker* io_worker, const Address& address,
       bool is_initial_connection);
  ~Pool();

  void connect();
  void close();

  bool write(Connection* connection, RequestHandler* request_handler);
  void flush();

  void wait_for_connection(RequestHandler* request_handler);
  Connection* borrow_connection();

  const Address& address() const { return address_; }

  bool is_initial_connection() const { return is_initial_connection_; }
  bool is_ready() const { return state_ == POOL_STATE_READY; }
  bool is_defunct() const { return is_defunct_; }
  bool is_critical_failure() const { return is_critical_failure_; }
  bool is_pending_flush() const { return is_pending_flush_; }

  void return_connection(Connection* connection);

private:
  void add_pending_request(RequestHandler* request_handler);
  void remove_pending_request(RequestHandler* request_handler);
  void set_is_available(bool is_available);

  void defunct();
  void maybe_notify_ready();
  void maybe_close();
  void spawn_connection();
  void maybe_spawn_connection();

  void on_connection_ready(Connection* connection);
  void on_connection_closed(Connection* connection);
  void on_connection_availability_changed(Connection* connection);
  void on_pending_request_timeout(RequestTimer* timer);

  Connection* find_least_busy();

private:
  typedef std::set<Connection*> ConnectionSet;
  typedef std::vector<Connection*> ConnectionVec;

  IOWorker* io_worker_;
  Address address_;
  uv_loop_t* loop_;
  Logger* logger_;
  const Config& config_;

  PoolState state_;
  ConnectionVec connections_;
  ConnectionSet connections_pending_;
  List<Handler> pending_requests_;
  int available_connection_count_;
  bool is_available_;
  bool is_initial_connection_;
  bool is_defunct_;
  bool is_critical_failure_;
  bool is_pending_flush_;
};

} // namespace cass

#endif
