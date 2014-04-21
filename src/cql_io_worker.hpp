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
#include "cql_request.hpp"

struct IOWorker {
  typedef std::shared_ptr<Pool>  PoolPtr;
  typedef std::unordered_map<std::string, PoolPtr> PoolCollection;

  uv_thread_t               thread;
  uv_loop_t*                loop;
  SSLContext*               ssl_context;
  PoolCollection            pools;

  explicit
  IOWorker(
      size_t io_queue_size) :
      loop(uv_loop_new())
  {}

  void
  add_pool(
      const std::string& host,
      size_t             core_connections_per_host,
      size_t             max_connections_per_host) {
    pools.insert(
        std::make_pair(
            host,
            PoolPtr(
                new Pool(
                    loop,
                    ssl_context,
                    host,
                    core_connections_per_host,
                    max_connections_per_host))));
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
#endif
