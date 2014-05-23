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

#include "mpmc_queue.hpp"
#include "spsc_queue.hpp"
#include "io_worker.hpp"
#include "load_balancing_policy.hpp"
#include "round_robin_policy.hpp"
#include "resolver.hpp"
#include "config.hpp"
#include "session_future.hpp"
#include "request_handler.hpp"

namespace cass {

class Session {
  public:
    Session();
    Session(const Session* session);

    ~Session();

    int init();
    void join();

    void set_keyspace() {
      // TODO(mstump)
    }

    void set_load_balancing_policy(LoadBalancingPolicy* policy) {
      load_balancing_policy_.reset(policy);
    }

    Config& config() { return config_; }

    void notify_connect_q(const Host& host);
    void notify_shutdown_q();

    Future* connect(const std::string& ks);
    Future* shutdown();
    Future* prepare(const char* statement, size_t length);
    Future* execute(Statement* statement);

  private:
    void init_pools();
    SSLSession* ssl_session_new();

    inline void execute(RequestHandler* request_handler) {
      if (!request_queue_->enqueue(request_handler)) {
        request_handler->on_error(CASS_ERROR_LIB_REQUEST_QUEUE_FULL, "The request queue has reached capacity");
        delete request_handler;
      }
    }

    static void on_run(void* data);
    static void on_connect(uv_async_t* data, int status);
    static void on_resolve(Resolver* resolver);
    static void on_event(uv_async_t* async, int status);
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

    struct Payload {
        enum Type {
          ON_CONNECTED,
          ON_SHUTDOWN,
        };
        Type type;
        Host host;
    };

  private:
    std::atomic<SessionState> state_;
    uv_thread_t thread_;
    uv_loop_t* loop_;
    SSLContext* ssl_context_;
    uv_async_t async_connect_;
    IOWorkerCollection io_workers_;
    std::string keyspace_;
    SessionFuture* connect_future_;
    SessionFuture* shutdown_future_;
    std::set<Host> hosts_;
    Config config_;
    std::unique_ptr<AsyncQueue<MPMCQueue<RequestHandler*>>> request_queue_;
    std::unique_ptr<AsyncQueue<MPMCQueue<Payload>>> event_queue_;
    std::unique_ptr<LoadBalancingPolicy> load_balancing_policy_;
    int pending_resolve_count_;
    int pending_connections_count_;
    int pending_workers_count_;
    int current_io_worker_;
};

} // namespace cass

#endif
