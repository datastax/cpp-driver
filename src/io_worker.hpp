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
struct Session;

struct IOWorker {
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

    Session* session_;
    uv_thread_t thread_;
    uv_loop_t* loop_;
    uv_prepare_t prepare_;
    SSLContext* ssl_context_;
    PoolCollection pools;
    std::vector<Pool*> pending_delete_;
    std::atomic<bool> is_stopped_;
    bool is_shutdown_;
    std::atomic<bool> is_shutdown_done_;

    const Config& config_;
    AsyncQueue<SPSCQueue<RequestHandler*>> request_queue_;
    AsyncQueue<SPSCQueue<Payload>> event_queue_;

    explicit
    IOWorker(Session* session, const Config& config)
      : session_(session)
      , loop_(uv_loop_new())
      , ssl_context_(nullptr)
      , is_stopped_(false)
      , is_shutdown_(false)
      , is_shutdown_done_(false)
      , config_(config)
      , request_queue_(config.queue_size_io())
      , event_queue_(config.queue_size_event()) {
      prepare_.data = this;
    }


    ~IOWorker() {
      uv_loop_delete(loop_);
      cleanup();
    }

    int init() {
      int rc = request_queue_.init(loop_, this, &IOWorker::on_execute);
      if(rc != 0) return rc;
      rc = event_queue_.init(loop_, this, &IOWorker::on_event);
      if(rc != 0) return rc;
      rc = uv_prepare_init(loop_, &prepare_);
      if(rc != 0) return rc;
      rc = uv_prepare_start(&prepare_, on_prepare);
      return rc;
    }

    bool add_pool_q(const Host& host) {
      Payload payload;
      payload.type = Payload::ADD_POOL;
      payload.host = host;
      return event_queue_.enqueue(payload);
    }

    bool remove_pool_q(const Host& host) {
      Payload payload;
      payload.type = Payload::REMOVE_POOL;
      payload.host = host;
      return event_queue_.enqueue(payload);
    }

    bool shutdown_q() {
      Payload payload;
      payload.type = Payload::SHUTDOWN;
      return event_queue_.enqueue(payload);
    }

    void add_pool(Host host);

    bool execute(RequestHandler* request_handler) {
      return request_queue_.enqueue(request_handler);
    }

    void retry(RequestHandler* request_handler, RetryType retry_type);
    void maybe_shutdown();
    void cleanup();

    void on_connect(Host host);
    void on_pool_close(Host host);
    static void on_pool_reconnect(Timer* timer);

    static void on_execute(uv_async_t* data, int status);
    static void on_event(uv_async_t* async, int status);
    static void on_prepare(uv_prepare_t* prepare, int status);

    static void
    run(
        void* data) {
      IOWorker* io_worker = reinterpret_cast<IOWorker*>(data);
      io_worker->is_stopped_ = false;
      uv_run(io_worker->loop_, UV_RUN_DEFAULT);
    }

    void
    run() {
      uv_thread_create(&thread_, &IOWorker::run, this);
    }

    void
    join() {
      if(!is_stopped_) {
        uv_thread_join(&thread_);
        is_stopped_ = true;
      }
    }
};

} // namespace cass

#endif
