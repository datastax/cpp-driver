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

#include "event_thread.hpp"
#include "mpmc_queue.hpp"
#include "spsc_queue.hpp"
#include "scoped_mutex.hpp"
#include "scoped_ptr.hpp"
#include "host.hpp"
#include "future.hpp"
#include "balancing.hpp"
#include "config.hpp"

#include <uv.h>
#include <list>
#include <string>
#include <vector>
#include <memory>
#include <set>

namespace cass {

class Logger;
class RequestHandler;
class Future;
class IOWorker;
class Resolver;
class Request;
class SSLSession;
class SSLContext;

struct SessionEvent {
  enum Type { CONNECT, NOTIFY_READY, NOTIFY_CLOSED };
  Type type;
};

class Session : public EventThread<SessionEvent> {
public:
  Session(const Config& config);
  ~Session();

  int init();

  std::string keyspace() {
    ScopedMutex lock(&keyspace_mutex_);
    return keyspace_;
  }

  void set_keyspace(const std::string& keyspace) {
    ScopedMutex lock(&keyspace_mutex_);
    keyspace_ = keyspace;
  }

  void set_load_balancing_policy(LoadBalancingPolicy* policy) {
    load_balancing_policy_.reset(policy);
  }

  bool notify_ready_async();
  bool notify_closed_async();
  bool notify_set_keyspace_async(const std::string& keyspace);

  bool connect_async(const std::string& keyspace, Future* future);
  void close_async(Future* future);

  Future* prepare(const char* statement, size_t length);
  Future* execute(const Request* statement);

private:
  void close_handles();

  void init_pools();
  SSLSession* ssl_session_new();

  void execute(RequestHandler* request_handler);

  virtual void on_run();
  virtual void on_after_run();
  virtual void on_event(const SessionEvent& event);

  static void on_resolve(Resolver* resolver);
  static void on_execute(uv_async_t* data, int status);

private:
  typedef std::vector<IOWorker*> IOWorkerVec;
  typedef std::set<Host> HostSet;

  SSLContext* ssl_context_;
  IOWorkerVec io_workers_;
  ScopedPtr<Logger> logger_;
  std::string keyspace_;
  uv_mutex_t keyspace_mutex_;
  Future* connect_future_;
  Future* close_future_;
  HostSet hosts_;
  Config config_;
  ScopedPtr<AsyncQueue<MPMCQueue<RequestHandler*> > > request_queue_;
  ScopedPtr<LoadBalancingPolicy> load_balancing_policy_;
  int pending_resolve_count_;
  int pending_pool_count_;
  int pending_workers_count_;
  int current_io_worker_;
};

class SessionCloseFuture : public Future {
public:
  SessionCloseFuture()
      : Future(CASS_FUTURE_TYPE_SESSION_CLOSE) {}
};

class SessionConnectFuture : public ResultFuture<Session> {
public:
  SessionConnectFuture(Session* session)
      : ResultFuture<Session>(CASS_FUTURE_TYPE_SESSION_CONNECT, session) {}

  ~SessionConnectFuture() {
    Session* session = release_result();
    if (session != NULL) {
      // The future was deleted before obtaining the session
      ScopedPtr<cass::SessionCloseFuture> close_future(new cass::SessionCloseFuture());
      session->close_async(close_future.get());
      close_future->wait();
    }
  }
};

} // namespace cass

#endif
