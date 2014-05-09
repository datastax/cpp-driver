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
#include "async_queue.hpp"
#include "config.hpp"
#include "host.hpp"
#include "ssl_context.hpp"
#include "request.hpp"
#include "spsc_queue.hpp"

namespace cass {

class Pool;

struct IOWorker {
  typedef std::shared_ptr<Pool>  PoolPtr;
  typedef std::unordered_map<Host, PoolPtr> PoolCollection;

  struct Payload {
    enum Type {
      ADD_POOL,
      REMOVE_POOL,
    };
    Type type;
    Host host;
    size_t core_connections_per_host;
    size_t max_connections_per_host;
  };

  Session*session_;
  uv_thread_t thread;
  uv_loop_t* loop;
  SSLContext* ssl_context;
  PoolCollection pools;

  const Config& config_;
  AsyncQueue<SPSCQueue<Request*>> request_queue_;
  AsyncQueue<SPSCQueue<Payload>> event_queue_;

  explicit
  IOWorker(Session* session, const Config& config)
    : session_(session)
    , loop(uv_loop_new())
    , config_(config)
    , request_queue_(config.queue_size_io())
    , event_queue_(config.queue_size_event()) {
  }

  int init() {
    int rc = request_queue_.init(loop, this, &IOWorker::on_execute);
    if(rc != 0) {
      return rc;
    }
    return event_queue_.init(loop, this, &IOWorker::on_event);
  }

  void add_pool_q(const Host& host) {
    Payload payload;
    payload.type = Payload::ADD_POOL;
    payload.host = host;
    payload.core_connections_per_host = config_.core_connections_per_host();
    payload.max_connections_per_host = config_.max_connections_per_host();
    event_queue_.enqueue(payload);
  }

  void remove_pool_q(const Host& host) {
    Payload payload;
    payload.type = Payload::REMOVE_POOL;
    payload.host = host;
  }

  Error*
  execute(
      Request* request) {
    if (!request_queue_.enqueue(request)) {
      return new Error(
          CASS_ERROR_SOURCE_LIBRARY,
          CASS_ERROR_LIB_NO_STREAMS,
          "request queue full",
          __FILE__,
          __LINE__);
    }
    return CASS_ERROR_NO_ERROR;
  }

  static void on_execute(uv_async_t* data, int status);
  static void on_event(uv_async_t* async, int status);

  static void
  run(
      void* data) {
    IOWorker* worker = reinterpret_cast<IOWorker*>(data);
    uv_run(worker->loop, UV_RUN_DEFAULT);
  }

  void
  run() {
    uv_thread_create(&thread, &IOWorker::run, this);
  }

  void
  stop() {
    uv_stop(loop);
  }

  void
  join() {
    uv_thread_join(&thread);
  }

  ~IOWorker() {
    uv_loop_delete(loop);
  }
};

} // namespace cass

#endif
