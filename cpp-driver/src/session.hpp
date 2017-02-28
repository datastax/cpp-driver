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

#ifndef __CASS_SESSION_HPP_INCLUDED__
#define __CASS_SESSION_HPP_INCLUDED__

#include "config.hpp"
#include "control_connection.hpp"
#include "event_thread.hpp"
#include "external.hpp"
#include "future.hpp"
#include "host.hpp"
#include "io_worker.hpp"
#include "load_balancing.hpp"
#include "metadata.hpp"
#include "metrics.hpp"
#include "mpmc_queue.hpp"
#include "random.hpp"
#include "ref_counted.hpp"
#include "request_handler.hpp"
#include "resolver.hpp"
#include "row.hpp"
#include "scoped_lock.hpp"
#include "scoped_ptr.hpp"
#include "speculative_execution.hpp"
#include "token_map.hpp"

#include <memory>
#include <set>
#include <string>
#include <uv.h>
#include <vector>

namespace cass {

class Future;
class IOWorker;
class Request;

struct SessionEvent {
  enum Type {
    INVALID,
    CONNECT,
    NOTIFY_READY,
    NOTIFY_KEYSPACE_ERROR,
    NOTIFY_WORKER_CLOSED,
    NOTIFY_UP,
    NOTIFY_DOWN
  };

  SessionEvent()
    : type(INVALID) { }

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

  Host::Ptr get_host(const Address& address);

  bool notify_ready_async();
  bool notify_keyspace_error_async();
  bool notify_worker_closed_async();
  bool notify_up_async(const Address& address);
  bool notify_down_async(const Address& address);

  void connect_async(const Config& config, const std::string& keyspace, const Future::Ptr& future);
  void close_async(const Future::Ptr& future);

  Future::Ptr prepare(const char* statement, size_t length);
  Future::Ptr execute(const Request::ConstPtr& request,
                      const Address* preferred_address = NULL);

  const Metadata& metadata() const { return metadata_; }

  int protocol_version() const {
    return control_connection_.protocol_version();
  }

  const VersionNumber& cassandra_version() const {
    return control_connection_.cassandra_version();
  }

private:
  void clear(const Config& config);
  int init();

  void close_handles();

  void internal_connect();
  void internal_close();

  void notify_connected();
  void notify_connect_error(CassError code, const std::string& message);
  void notify_closed();

  void execute(const RequestHandler::Ptr& request_handler);

  virtual void on_run();
  virtual void on_after_run();
  virtual void on_event(const SessionEvent& event);

  static void on_resolve(MultiResolver<Session*>::Resolver* resolver);
  static void on_resolve_done(MultiResolver<Session*>* resolver);

#if UV_VERSION_MAJOR >= 1
  struct ResolveNameData {
    ResolveNameData(Session* session,
                    const Host::Ptr& host,
                    bool is_initial_connection)
      : session(session)
      , host(host)
      , is_initial_connection(is_initial_connection) { }
    Session* session;
    Host::Ptr host;
    bool is_initial_connection;
  };
  typedef cass::NameResolver<ResolveNameData> NameResolver;

  static void on_resolve_name(MultiResolver<Session*>::NameResolver* resolver);
  static void on_add_resolve_name(NameResolver* resolver);
#endif

#if UV_VERSION_MAJOR == 0
  static void on_execute(uv_async_t* data, int status);
#else
  static void on_execute(uv_async_t* data);
#endif

  QueryPlan* new_query_plan(const RequestHandler::Ptr& request_handler = RequestHandler::Ptr());
  SpeculativeExecutionPlan* new_execution_plan(const Request* request);

  void on_reconnect(Timer* timer);

private:
  // TODO(mpenick): Consider removing friend access to session
  friend class ControlConnection;

  Host::Ptr add_host(const Address& address);
  void purge_hosts(bool is_initial_connection);

  Metadata& metadata() { return metadata_; }

  void on_control_connection_ready();
  void on_control_connection_error(CassError code, const std::string& message);

  void on_add(Host::Ptr host, bool is_initial_connection);
  void internal_on_add(Host::Ptr host, bool is_initial_connection);

  void on_remove(Host::Ptr host);
  void on_up(Host::Ptr host);
  void on_down(Host::Ptr host);

private:
  typedef std::vector<IOWorker::Ptr > IOWorkerVec;

  Atomic<State> state_;
  uv_mutex_t state_mutex_;

  Config config_;
  ScopedPtr<Metrics> metrics_;
  LoadBalancingPolicy::Ptr load_balancing_policy_;
  SharedRefPtr<SpeculativeExecutionPolicy> speculative_execution_policy_;
  CassError connect_error_code_;
  std::string connect_error_message_;
  Future::Ptr connect_future_;
  Future::Ptr close_future_;

  HostMap hosts_;
  uv_mutex_t hosts_mutex_;

  IOWorkerVec io_workers_;
  ScopedPtr<AsyncQueue<MPMCQueue<RequestHandler*> > > request_queue_;

  ScopedPtr<TokenMap> token_map_;
  Metadata metadata_;
  ScopedPtr<Random> random_;
  ControlConnection control_connection_;
  bool current_host_mark_;
  int pending_pool_count_;
  int pending_workers_count_;
  int current_io_worker_;

  CopyOnWritePtr<std::string> keyspace_;
};

class SessionFuture : public Future {
public:
  typedef SharedRefPtr<SessionFuture> Ptr;

  SessionFuture()
      : Future(CASS_FUTURE_TYPE_SESSION) {}
};

} // namespace cass

EXTERNAL_TYPE(cass::Session, CassSession)

#endif
