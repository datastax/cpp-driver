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

#ifndef __CASS_POOL_HPP_INCLUDED__
#define __CASS_POOL_HPP_INCLUDED__

#include "cassandra.h"
#include "connection.hpp"
#include "metrics.hpp"
#include "ref_counted.hpp"
#include "request.hpp"
#include "request_handler.hpp"
#include "scoped_ptr.hpp"

#include <algorithm>
#include <functional>
#include <set>
#include <string>

namespace cass {

class IOWorker;
class RequestHandler;
class Config;

class Pool : public RefCounted<Pool>
           , public Connection::Listener {
public:
  enum PoolState {
    POOL_STATE_NEW,
    POOL_STATE_CONNECTING,
    POOL_STATE_READY,
    POOL_STATE_CLOSING,
    POOL_STATE_CLOSED
  };

  Pool(IOWorker* io_worker,
       const Address& address,
       bool is_initial_connection);
  virtual ~Pool();

  void connect();
  void close(bool cancel_reconnect = false);

  bool write(Connection* connection, RequestHandler* request_handler);
  void flush();

  void wait_for_connection(RequestHandler* request_handler);
  Connection* borrow_connection();

  const Address& address() const { return address_; }

  bool is_initial_connection() const { return is_initial_connection_; }
  bool is_ready() const { return state_ == POOL_STATE_READY; }
  bool is_critical_failure() const { return is_critical_failure_; }
  bool cancel_reconnect() const { return cancel_reconnect_; }

  void return_connection(Connection* connection);

private:
  void add_pending_request(RequestHandler* request_handler);
  void remove_pending_request(RequestHandler* request_handler);
  void set_is_available(bool is_available);

  void maybe_notify_ready();
  void maybe_close();
  void spawn_connection();
  void maybe_spawn_connection();

  // Connection listener methods
  virtual void on_ready(Connection* connection);
  virtual void on_close(Connection* connection);
  virtual void on_availability_change(Connection* connection);
  virtual void on_event(EventResponse* response) {}

  static void on_pending_request_timeout(RequestTimer* timer);

  Connection* find_least_busy();

private:
  typedef std::set<Connection*> ConnectionSet;
  typedef std::vector<Connection*> ConnectionVec;

  IOWorker* io_worker_;
  Address address_;
  uv_loop_t* loop_;
  const Config& config_;
  Metrics* metrics_;

  PoolState state_;
  ConnectionVec connections_;
  ConnectionSet connections_pending_;
  List<Handler> pending_requests_;
  int available_connection_count_;
  bool is_available_;
  bool is_initial_connection_;
  bool is_critical_failure_;
  bool is_pending_flush_;
  bool cancel_reconnect_;
};

} // namespace cass

#endif
