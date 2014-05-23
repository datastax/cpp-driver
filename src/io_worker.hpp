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

#ifndef __CASS_IO_WORKER_HPP_INCLUDED__
#define __CASS_IO_WORKER_HPP_INCLUDED__

#include <uv.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "async_queue.hpp"
#include "config.hpp"
#include "host.hpp"
#include "ssl_context.hpp"
#include "spsc_queue.hpp"
#include "request_handler.hpp"

namespace cass {

class Pool;
class Session;

class IOWorker {
  public:
    IOWorker(Session* session, const Config& config);
    ~IOWorker();

    int init();

    bool add_pool_q(const Host& host);
    bool remove_pool_q(const Host& host);
    bool shutdown_q();

    bool is_shutdown() const { return is_shutdown_; }

    void run();
    void join();

    bool execute(RequestHandler* request_handler);

  private:
    void add_pool(Host host);
    void retry(RequestHandler* request_handler, RetryType retry_type);
    void maybe_shutdown();
    void cleanup();

    void on_connect(Host host);
    void on_pool_close(Host host);
    static void on_pool_reconnect(Timer* timer);

    static void on_run(void* data);
    static void on_event(uv_async_t* async, int status);
    static void on_execute(uv_async_t* data, int status);
    static void on_prepare(uv_prepare_t* prepare, int status);

  private:
    typedef std::unordered_map<Host, Pool*> PoolCollection;

    struct ReconnectRequest {
        ReconnectRequest(IOWorker* io_worker, Host host)
          : io_worker(io_worker)
          , host(host) { }

        IOWorker* io_worker;
        Host host;
    };

    struct Payload {
        enum Type {
          ADD_POOL,
          REMOVE_POOL,
          SHUTDOWN,
        };
        Type type;
        Host host;
    };

  private:
    Session* session_;
    uv_thread_t thread_;
    uv_loop_t* loop_;
    uv_prepare_t prepare_;
    SSLContext* ssl_context_;
    PoolCollection pools;
    std::vector<Pool*> pending_delete_;
    std::atomic<bool> is_stopped_;
    bool is_shutting_down_;
    std::atomic<bool> is_shutdown_;

    const Config& config_;
    AsyncQueue<SPSCQueue<RequestHandler*>> request_queue_;
    AsyncQueue<SPSCQueue<Payload>> event_queue_;

};

} // namespace cass

#endif
