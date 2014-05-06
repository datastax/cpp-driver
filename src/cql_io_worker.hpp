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

#ifndef __CQL_IO_WORKER_HPP_INCLUDED__
#define __CQL_IO_WORKER_HPP_INCLUDED__

#include <uv.h>
#include <string>
#include <unordered_map>
#include "cql_pool.hpp"
#include "cql_async_queue.hpp"
#include "cql_config.hpp"

struct CqlIOWorker {
  typedef std::shared_ptr<CqlPool>  CqlPoolPtr;
  typedef std::unordered_map<CqlHost, CqlPoolPtr> CqlPoolCollection;

  struct PoolAction {
    enum Type {
      ADD,
      REMOVE,
    };
    Type type;
    CqlHost host;
    size_t core_connections_per_host;
    size_t max_connections_per_host;
  };

  uv_thread_t            thread;
  uv_loop_t*             loop;
  SSLContext*            ssl_context;
  CqlPoolCollection      pools;

  const Config& config_;
  AsyncQueue<SPSCQueue<CqlRequest*>> request_queue_;
  AsyncQueue<SPSCQueue<PoolAction>> pool_queue_;

  explicit
  CqlIOWorker(const Config& config)
    : loop(uv_loop_new())
    , config_(config)
    , request_queue_(config.queue_size_io())
    , pool_queue_(config.queue_size_pool()) {
  }

  int init() {
    int rc = request_queue_.init(loop, this, &CqlIOWorker::on_execute);
    if(rc != 0) {
      return rc;
    }
    return pool_queue_.init(loop, this, &CqlIOWorker::on_pool_action);
  }

  void add_pool(const CqlHost& host,
                size_t core_connections_per_host,
                size_t max_connections_per_host) {
    PoolAction action;
    action.type = PoolAction::ADD;
    action.host = host;
    action.core_connections_per_host = core_connections_per_host;
    action.max_connections_per_host = max_connections_per_host;
    pool_queue_.enqueue(action);
  }

  void remove_pool(const CqlHost& host) {
    PoolAction action;
    action.type = PoolAction::REMOVE;
    action.host = host;
  }

  CqlError*
  execute(
      CqlRequest* request) {
    if (!request_queue_.enqueue(request)) {
      return new CqlError(
          CQL_ERROR_SOURCE_LIBRARY,
          CQL_ERROR_LIB_NO_STREAMS,
          "request queue full",
          __FILE__,
          __LINE__);
    }
    return CQL_ERROR_NO_ERROR;
  }

  static void
  on_execute(
      uv_async_t* data,
      int         status) {
    CqlIOWorker* worker  = reinterpret_cast<CqlIOWorker*>(data->data);
    CqlRequest*  request = nullptr;

    while (worker->request_queue_.dequeue(request)) {
      for (const CqlHost& host : request->hosts) {
        auto pool = worker->pools.find(host);
        static_cast<void>(pool);
      }
    }
  }

  static void on_pool_action(uv_async_t* data, int status) {
    CqlIOWorker* worker  = reinterpret_cast<CqlIOWorker*>(data->data);

    PoolAction action;
    while(worker->pool_queue_.dequeue(action)) {
      if(action.type == PoolAction::ADD) {
        if(worker->pools.count(action.host) == 0) {
          worker->pools.insert(
              std::make_pair(
                  action.host,
                  CqlPoolPtr(
                      new CqlPool(
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
    CqlIOWorker* worker = reinterpret_cast<CqlIOWorker*>(data);
    uv_run(worker->loop, UV_RUN_DEFAULT);
  }

  void
  run() {
    uv_thread_create(&thread, &CqlIOWorker::run, this);
  }

  void
  stop() {
    uv_stop(loop);
  }

  void
  join() {
    uv_thread_join(&thread);
  }

  ~CqlIOWorker() {
    uv_loop_delete(loop);
  }
};
#endif
