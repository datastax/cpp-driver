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
#include "pool.hpp"
#include "async_queue.hpp"
#include "config.hpp"

namespace cass {

struct IOWorker {
  typedef std::shared_ptr<Pool>  PoolPtr;
  typedef std::unordered_map<Host, PoolPtr> PoolCollection;

  struct PoolAction {
    enum Type {
      ADD,
      REMOVE,
    };
    Type type;
    Host host;
    size_t core_connections_per_host;
    size_t max_connections_per_host;
  };

  uv_thread_t            thread;
  uv_loop_t*             loop;
  SSLContext*            ssl_context;
  PoolCollection      pools;

  const Config& config_;
  AsyncQueue<SPSCQueue<Request*>> request_queue_;
  AsyncQueue<SPSCQueue<PoolAction>> pool_queue_;

  explicit
  IOWorker(const Config& config)
    : loop(uv_loop_new())
    , config_(config)
    , request_queue_(config.queue_size_io())
    , pool_queue_(config.queue_size_pool()) {
  }

  int init() {
    int rc = request_queue_.init(loop, this, &IOWorker::on_execute);
    if(rc != 0) {
      return rc;
    }
    return pool_queue_.init(loop, this, &IOWorker::on_pool_action);
  }

  void add_pool(const Host& host,
                size_t core_connections_per_host,
                size_t max_connections_per_host) {
    PoolAction action;
    action.type = PoolAction::ADD;
    action.host = host;
    action.core_connections_per_host = core_connections_per_host;
    action.max_connections_per_host = max_connections_per_host;
    pool_queue_.enqueue(action);
  }

  void remove_pool(const Host& host) {
    PoolAction action;
    action.type = PoolAction::REMOVE;
    action.host = host;
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

  static void
  on_execute(
      uv_async_t* data,
      int         status) {
    IOWorker* worker  = reinterpret_cast<IOWorker*>(data->data);
    Request*  request = nullptr;

    while (worker->request_queue_.dequeue(request)) {
      for (const Host& host : request->hosts) {
        auto pool = worker->pools.find(host);
        static_cast<void>(pool);
      }
    }
  }

  static void on_pool_action(uv_async_t* data, int status) {
    IOWorker* worker  = reinterpret_cast<IOWorker*>(data->data);

    PoolAction action;
    while(worker->pool_queue_.dequeue(action)) {
      if(action.type == PoolAction::ADD) {
        if(worker->pools.count(action.host) == 0) {
          worker->pools.insert(
              std::make_pair(
                  action.host,
                  PoolPtr(
                      new Pool(
                          worker->loop,
                          worker->ssl_context,
                          action.host,
                          action.core_connections_per_host,
                          action.max_connections_per_host))));
        }
      } else if(action.type == PoolAction::REMOVE) {
        // TODO:(mpenick)
      }
    }
  }


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
