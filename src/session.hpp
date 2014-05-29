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

#include <uv.h>

#include <atomic>
#include <list>
#include <string>
#include <vector>
#include <memory>
#include <set>

#include "event_thread.hpp"
#include "mpmc_queue.hpp"
#include "spsc_queue.hpp"
#include "io_worker.hpp"
#include "load_balancing_policy.hpp"
#include "round_robin_policy.hpp"
#include "resolver.hpp"
#include "config.hpp"
#include "session_future.hpp"
#include "request_handler.hpp"
#include "logger.hpp"

namespace cass {

struct SessionEvent {
    enum Type {
      CONNECT,
      NOTIFY_CONNECTED,
      NOTIFY_SHUTDOWN,
    };
    Type type;
    Host host;
};

class Session : public EventThread<SessionEvent> {
  public:
    Session(const Config& config);

    int init();
    void join();

    std::shared_ptr<std::string> keyspace() const {
      return std::atomic_load(&keyspace_);
    }
    void set_keyspace(const std::string& keyspace) {
      std::atomic_store(&keyspace_, std::shared_ptr<std::string>(new std::string(keyspace)));
    }
    void set_load_balancing_policy(LoadBalancingPolicy* policy) {
      load_balancing_policy_.reset(policy);
    }

    bool notify_connect_async(const Host& host);
    bool notify_shutdown_async();
    bool notify_set_keyspace_async(const std::string& keyspace);

    Future* connect(const std::string& keyspace);
    Future* shutdown();
    Future* prepare(const char* statement, size_t length);
    Future* execute(MessageBody* statement);

  private:
    bool connect_async();
    void close();

    void init_pools();
    SSLSession* ssl_session_new();

    inline void execute(RequestHandler* request_handler) {
      if (!request_queue_->enqueue(request_handler)) {
        request_handler->on_error(CASS_ERROR_LIB_REQUEST_QUEUE_FULL, "The request queue has reached capacity");
        delete request_handler;
      }
    }

    virtual void on_run();
    virtual void on_event(const SessionEvent& event);

    static void on_resolve(Resolver* resolver);
    static void on_execute(uv_async_t* data, int status);

  private:
    typedef std::shared_ptr<IOWorker> IOWorkerPtr;
    typedef std::vector<IOWorkerPtr> IOWorkerCollection;

    enum SessionState {
      SESSION_STATE_NEW,
      SESSION_STATE_CONNECTING,
      SESSION_STATE_READY,
      SESSION_STATE_DISCONNECTING,
      SESSION_STATE_DISCONNECTED
    };

  private:
    std::atomic<SessionState> state_;
    SSLContext* ssl_context_;
    IOWorkerCollection io_workers_;
    std::unique_ptr<Logger> logger_;
    std::shared_ptr<std::string> keyspace_;
    SessionFuture* connect_future_;
    SessionFuture* shutdown_future_;
    std::set<Host> hosts_;
    Config config_;
    std::unique_ptr<AsyncQueue<MPMCQueue<RequestHandler*>>> request_queue_;
    std::unique_ptr<LoadBalancingPolicy> load_balancing_policy_;
    int pending_resolve_count_;
    int pending_connections_count_;
    int pending_workers_count_;
    int current_io_worker_;
};

} // namespace cass

#endif
