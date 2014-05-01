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

struct CqlIOWorker {
  typedef std::shared_ptr<CqlPool>  CqlPoolPtr;
  typedef std::unordered_map<std::string, CqlPoolPtr> CqlPoolCollection;

  uv_thread_t            thread;
  uv_loop_t*             loop;
  SSLContext*            ssl_context;
  CqlPoolCollection      pools;
  uv_async_t             async_execute;
  SPSCQueue<CqlRequest*> request_queue;

  explicit
  CqlIOWorker(
      size_t request_queue_size) :
      loop(uv_loop_new()),
      request_queue(request_queue_size) {
    async_execute.data = this;
    uv_async_init(
        loop,
        &async_execute,
        &CqlIOWorker::on_execute);
  }

  void
  add_pool(
      const CqlHost& host,
      size_t         core_connections_per_host,
      size_t         max_connections_per_host) {
    pools.insert(
        std::make_pair(
            host.address_string,
            CqlPoolPtr(
                new CqlPool(
                    loop,
                    ssl_context,
                    host,
                    core_connections_per_host,
                    max_connections_per_host))));
  }

  CqlError*
  execute(
      CqlRequest* request) {
    if (request_queue.enqueue(request)) {
      uv_async_send(&async_execute);
      return CQL_ERROR_NO_ERROR;
    }
    return new CqlError(
        CQL_ERROR_SOURCE_LIBRARY,
        CQL_ERROR_LIB_NO_STREAMS,
        "request queue full",
        __FILE__,
        __LINE__);
  }

  static void
  on_execute(
      uv_async_t* data,
      int         status) {
    CqlIOWorker* worker  = reinterpret_cast<CqlIOWorker*>(data->data);
    CqlRequest*  request = nullptr;

    while (worker->request_queue.dequeue(request)) {
      for (const std::string& host : request->hosts) {
        auto pool = worker->pools.find(host);
        static_cast<void>(pool);
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
