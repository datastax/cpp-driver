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

#ifndef __CASS_SESSION_HPP_INCLUDED__
#define __CASS_SESSION_HPP_INCLUDED__

#include "cluster_metadata.hpp"
#include "config.hpp"
#include "control_connection.hpp"
#include "event_thread.hpp"
#include "future.hpp"
#include "host.hpp"
#include "io_worker.hpp"
#include "load_balancing.hpp"
#include "metrics.hpp"
#include "mpmc_queue.hpp"
#include "ref_counted.hpp"
#include "row.hpp"
#include "schema_metadata.hpp"
#include "scoped_lock.hpp"
#include "scoped_ptr.hpp"

#include <list>
#include <memory>
#include <set>
#include <string>
#include <uv.h>
#include <vector>

namespace cass {

class RequestHandler;
class Future;
class IOWorker;
class Resolver;
class Request;

struct SessionEvent {
  enum Type {
    INVALID,
    CONNECT,
    NOTIFY_READY,
    NOTIFY_WORKER_CLOSED,
    NOTIFY_UP,
    NOTIFY_DOWN
  };

  SessionEvent()
    : type(INVALID) {}

  Type type;
  Address address;
};

class Session : public EventThread<SessionEvent> {
public:
  enum State {
    SESSION_STATE_CONNECTING,
    SESSION_STATE_CONNECTED,
    SESSION_STATE_CLOSING,
    SESSION_STATE_CLOSED
  };

  Session();
  ~Session();

  const Config& config() const { return config_; }
  Metrics* metrics() const { return metrics_.get(); }

  void set_load_balancing_policy(LoadBalancingPolicy* policy) {
    load_balancing_policy_.reset(policy);
  }

  void broadcast_keyspace_change(const std::string& keyspace,
                                 const IOWorker* calling_io_worker);

  SharedRefPtr<Host> get_host(const Address& address);

  bool notify_ready_async();
  bool notify_worker_closed_async();
  bool notify_up_async(const Address& address);
  bool notify_down_async(const Address& address);

  void connect_async(const Config& config, const std::string& keyspace, Future* future);
  void close_async(Future* future, bool force = false);

  Future* prepare(const char* statement, size_t length);
  Future* execute(const RoutableRequest* statement);

  const Schema* copy_schema() const { return cluster_meta_.copy_schema(); }

private:
  void clear(const Config& config);
  int init();

  void close_handles();

  void internal_connect();
  void internal_close();

  void notify_connected();
  void notify_connect_error(CassError code, const std::string& message);
  void notify_closed();

  void execute(RequestHandler* request_handler);

  virtual void on_run();
  virtual void on_after_run();
  virtual void on_event(const SessionEvent& event);

  static void on_resolve(Resolver* resolver);

#if UV_VERSION_MAJOR == 0
  static void on_execute(uv_async_t* data, int status);
#else
  static void on_execute(uv_async_t* data);
#endif

  QueryPlan* new_query_plan(const Request* request = NULL);

  void on_reconnect(Timer* timer);

private:
  // TODO(mpenick): Consider removing friend access to session
  friend class ControlConnection;

  SharedRefPtr<Host> add_host(const Address& address);
  void purge_hosts(bool is_initial_connection);

  ClusterMetadata& cluster_meta() {
    return cluster_meta_;
  }

  void on_control_connection_ready();
  void on_control_connection_error(CassError code, const std::string& message);

  void on_add(SharedRefPtr<Host> host, bool is_initial_connection);
  void on_remove(SharedRefPtr<Host> host);
  void on_up(SharedRefPtr<Host> host);
  void on_down(SharedRefPtr<Host> host);

private:
  typedef std::vector<SharedRefPtr<IOWorker> > IOWorkerVec;

  State state_;
  uv_mutex_t state_mutex_;

  Config config_;
  ScopedPtr<Metrics> metrics_;
  ScopedRefPtr<LoadBalancingPolicy> load_balancing_policy_;
  ScopedRefPtr<Future> connect_future_;
  ScopedRefPtr<Future> close_future_;

  HostMap hosts_;
  uv_mutex_t hosts_mutex_;

  IOWorkerVec io_workers_;
  ScopedPtr<AsyncQueue<MPMCQueue<RequestHandler*> > > request_queue_;
  ClusterMetadata cluster_meta_;
  ControlConnection control_connection_;
  bool current_host_mark_;
  int pending_resolve_count_;
  int pending_pool_count_;
  int pending_workers_count_;
  int current_io_worker_;
};

class SessionFuture : public Future {
public:
  SessionFuture()
      : Future(CASS_FUTURE_TYPE_SESSION) {}
};

} // namespace cass

#endif
