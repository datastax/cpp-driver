/*
  Copyright (c) 2014-2016 DataStax

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
#include "host.hpp"
#include "metrics.hpp"
#include "ref_counted.hpp"
#include "request.hpp"
#include "request_callback.hpp"
#include "request_handler.hpp"
#include "scoped_ptr.hpp"
#include "timer.hpp"

namespace cass {

class IOWorker;
class Config;

class Pool : public RefCounted<Pool>
           , public Connection::Listener {
public:
  typedef SharedRefPtr<Pool> Ptr;

  enum PoolState {
    POOL_STATE_NEW,
    POOL_STATE_CONNECTING,
    POOL_STATE_WAITING_TO_CONNECT,
    POOL_STATE_READY,
    POOL_STATE_CLOSING,
    POOL_STATE_CLOSED
  };

  Pool(IOWorker* io_worker,
       const Host::ConstPtr& host,
       bool is_initial_connection);
  virtual ~Pool();

  void connect();
  void delayed_connect();
  void close(bool cancel_reconnect = false);

  bool write(Connection* connection, const SpeculativeExecution::Ptr& speculative_execution);
  void flush();

  void wait_for_connection(const SpeculativeExecution::Ptr& speculative_execution);
  Connection* borrow_connection();

  const Host::ConstPtr& host() const { return host_; }
  uv_loop_t* loop() { return loop_; }
  const Config& config() const { return config_; }

  bool is_initial_connection() const { return is_initial_connection_; }
  bool is_ready() const { return state_ == POOL_STATE_READY; }
  bool is_keyspace_error() const {
    return error_code_ == Connection::CONNECTION_ERROR_KEYSPACE;
  }

  bool is_critical_failure() const {
    return error_code_ == Connection::CONNECTION_ERROR_INVALID_PROTOCOL ||
        error_code_ == Connection::CONNECTION_ERROR_AUTH ||
        error_code_ == Connection::CONNECTION_ERROR_SSL_HANDSHAKE ||
        error_code_ == Connection::CONNECTION_ERROR_SSL_VERIFY;
  }

  bool cancel_reconnect() const { return cancel_reconnect_; }

  void return_connection(Connection* connection);

private:
  void remove_pending_request(SpeculativeExecution* speculative_execution);
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

  static void on_pending_request_timeout(Timer* timer);
  static void on_partial_reconnect(Timer* timer);
  static void on_wait_to_connect(Timer* timer);

  Connection* find_least_busy();

private:
  typedef std::vector<Connection*> ConnectionVec;

  IOWorker* io_worker_;
  Host::ConstPtr host_;
  uv_loop_t* loop_;
  const Config& config_;
  Metrics* metrics_;

  PoolState state_;
  Connection::ConnectionError error_code_;
  ConnectionVec connections_;
  ConnectionVec pending_connections_;
  List<RequestCallback> pending_requests_;
  int available_connection_count_;
  bool is_available_;
  bool is_initial_connection_;
  bool is_pending_flush_;
  bool cancel_reconnect_;

  Timer connect_timer;
};

} // namespace cass

#endif
