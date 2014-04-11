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

#ifndef __SESSION_HPP_INCLUDED__
#define __SESSION_HPP_INCLUDED__

#include <uv.h>
#include <string>
#include <unordered_map>
#include <vector>
#include "cql_mpmc_queue.hpp"
#include "cql_pool.hpp"
#include "cql_request.hpp"

namespace cql {

class Session {
  struct IOWorker {
    typedef std::shared_ptr<cql::Pool>  PoolPtr;
    typedef std::unordered_map<std::string, PoolPtr> PoolCollection;

    uv_thread_t    thread;
    uv_loop_t*     loop;
    SSLContext*    ssl_context;
    PoolCollection pools;

    IOWorker() :
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
                  new cql::Pool(
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
    run_async() {
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

  std::vector<IOWorker*>              io_loops_;
  cql::SSLContext*                    ssl_context_;
  cql::LogCallback                    log_callback_;
  cql::MpmcQueue<cql::CallerRequest*> queue_;

  Session(
      size_t io_loop_count) :
      io_loops_(io_loop_count, NULL),
      queue_(1024) {
    for (size_t i = 0; i < io_loops_.size(); ++i) {
      io_loops_[i] = new IOWorker();
    }
  }

  SSLSession*
  ssl_session_new() {
    if (ssl_context_) {
      return ssl_context_->session_new();
    }
    return NULL;
  }

  inline void
  log(
      int         level,
      const char* message) {
    (void) level;
    std::cout << message << std::endl;
  }

  inline void
  log(
      int                level,
      const std::string& message) {
    (void) level;
    std::cout << message << std::endl;
  }

 public:
  CallerRequest*
  prepare() {
    return nullptr;
  }

  CallerRequest*
  execute() {
    return nullptr;
  }

  void
  shutdown() {
  }

  void
  set_keyspace() {
  }

};
}
#endif
