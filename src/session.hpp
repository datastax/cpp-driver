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
#include "mpmc_queue.hpp"
#include "ref_counted.hpp"
#include "row.hpp"
#include "schema_metadata.hpp"
#include "scoped_mutex.hpp"
#include "scoped_ptr.hpp"
#include "spsc_queue.hpp"

#include <list>
#include <memory>
#include <set>
#include <string>
#include <uv.h>
#include <vector>

#ifdef TESTING_DIRECTIVE
#include <stdexcept>
#endif

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
    NOTIFY_CLOSED,
    NOTIFY_UP,
    NOTIFY_DOWN
  };

  SessionEvent()
    : type(INVALID) {}

  Type type;
  Address address;
  bool is_critical_failure;
};

class Session : public EventThread<SessionEvent> {
public:
  Session(const Config& config);

  int init();

  const Config& config() const { return config_; }

  void set_load_balancing_policy(LoadBalancingPolicy* policy) {
    load_balancing_policy_.reset(policy);
  }

  void broadcast_keyspace_change(const std::string& keyspace,
                                 const IOWorker* calling_io_worker);

  SharedRefPtr<Host> get_host(const Address& address, bool should_mark = false);
  SharedRefPtr<Host> add_host(const Address& address, bool should_mark = false);
  void purge_hosts(bool is_initial_connection);

  bool notify_ready_async();
  bool notify_closed_async();
  bool notify_up_async(const Address& address);
  bool notify_down_async(const Address& address, bool is_critical_failure);

  bool connect_async(const std::string& keyspace, Future* future);
  void close_async(Future* future);

  Future* prepare(const char* statement, size_t length);
  Future* execute(const RoutableRequest* statement);

  const Schema* copy_schema() const { return cluster_meta_.copy_schema(); }

private:
  void close_handles();

  void internal_connect();

  void execute(RequestHandler* request_handler);

  virtual void on_run();
  virtual void on_after_run();
  virtual void on_event(const SessionEvent& event);

  static void on_resolve(Resolver* resolver);
  static void on_execute(uv_async_t* data, int status);

  QueryPlan* new_query_plan(const Request* request = NULL);

  void on_reconnect(Timer* timer);

private:
  friend class ControlConnection;

  ClusterMetadata& cluster_meta() {
    return cluster_meta_;
  }

  void on_control_connection_ready();
  void on_control_connection_error(CassError code, const std::string& message);

  void on_add(SharedRefPtr<Host> host, bool is_initial_connection);
  void on_remove(SharedRefPtr<Host> host);
  void on_up(SharedRefPtr<Host> host);
  void on_down(SharedRefPtr<Host> host, bool is_critical_failure);

private:
  typedef std::vector<SharedRefPtr<IOWorker> > IOWorkerVec;

  Config config_;
  ControlConnection control_connection_;
  IOWorkerVec io_workers_;
  ScopedRefPtr<Future> connect_future_;
  Future* close_future_;
  HostMap hosts_;
  bool current_host_mark_;
  ScopedPtr<AsyncQueue<MPMCQueue<RequestHandler*> > > request_queue_;
  ScopedRefPtr<LoadBalancingPolicy> load_balancing_policy_;
  int pending_resolve_count_;
  int pending_pool_count_;
  int pending_workers_count_;
  int current_io_worker_;
  ClusterMetadata cluster_meta_;
};

class SessionCloseFuture : public Future {
public:
  SessionCloseFuture(Session* session)
      : Future(CASS_FUTURE_TYPE_SESSION_CLOSE)
      , session_(session) {
    session_thread_guard();
  }

  ~SessionCloseFuture() {
    wait();
  }

  void wait() {
    session_thread_guard();
    ScopedMutex lock(&mutex_);

    internal_wait(lock);

    if (session_ != NULL) {
      session_->join();
      delete session_;
      session_ = NULL;
    }
  }

  bool wait_for(uint64_t timeout) {
    session_thread_guard();
    ScopedMutex lock(&mutex_);
    if (internal_wait_for(lock, timeout)) {
      if (session_ != NULL) {
        session_->join();
        delete session_;
        session_ = NULL;
      }
      return true;
    }
    return false;
  }

private:
  void session_thread_guard() {
    if (session_ != NULL && uv_thread_self() == session_->thread_id()) {
      const char* message = "Attempted to close session with session thread (possibly from close callback). Aborting.";
#ifndef TESTING_DIRECTIVE
      fprintf(stderr, "%s\n", message);
      abort();
#else
      throw std::runtime_error(message);
#endif
    }
  }

private:
  Session* session_;
};

class SessionConnectFuture : public ResultFuture<Session> {
public:
  SessionConnectFuture(Session* session)
      : ResultFuture<Session>(CASS_FUTURE_TYPE_SESSION_CONNECT, session) {}

  ~SessionConnectFuture() {
    Session* session = release_result();
    if (session != NULL) {
      // The future was deleted before obtaining the session
      ScopedRefPtr<cass::SessionCloseFuture> close_future(new cass::SessionCloseFuture(session));
      session->close_async(close_future.get());
      close_future->wait();
    }
  }
};

} // namespace cass

#endif
