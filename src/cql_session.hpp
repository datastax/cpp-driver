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

#ifndef __CQL_SESSION_HPP_INCLUDED__
#define __CQL_SESSION_HPP_INCLUDED__

#include <uv.h>
#include <list>
#include <string>
#include <vector>
#include "cql_error.hpp"
#include "cql_spsc_queue.hpp"
#include "cql_pool.hpp"
#include "cql_request.hpp"
#include "cql_io_worker.hpp"

typedef Request<CqlSession*, CqlError*, CqlSession*> CqlSessionRequest;

struct CqlSession {
  enum CqlSessionState {
    SESSION_STATE_NEW,
    SESSION_STATE_READY,
    SESSION_STATE_DISCONNECTING,
    SESSION_STATE_DISCONNECTED
  };

  std::vector<IOWorker*>        io_loops;
  SSLContext*                   ssl_context;
  LogCallback                   log_callback;
  SPSCQueue<CallerRequest*>     queue;
  std::string                   keyspace;
  std::list<CqlSessionRequest*> connect_listeners;
  std::mutex                    mutex;
  CqlSessionState               state;

  CqlSession(
      size_t io_loop_count,
      size_t io_queue_size) :
      io_loops(io_loop_count, NULL),
      queue(io_queue_size) {
    for (size_t i = 0; i < io_loops.size(); ++i) {
      io_loops[i] = new IOWorker();
    }
  }

  CqlSessionRequest*
  connect(
      const char* ks) {
    return connect(std::string(ks));
  }

  CqlSessionRequest*
  connect(
      const std::string& ks) {
    std::unique_lock<std::mutex> lock(mutex);
    CqlSessionRequest* output = new CqlSessionRequest();

    if (state == SESSION_STATE_NEW) {
      keyspace = ks;
      output->data = this;
      connect_listeners.push_back(output);
    } else {
      output->error = new CqlError(
          CQL_ERROR_SOURCE_LIBRARY,
          CQL_ERROR_LIB_SESSION_STATE,
          "invalid session state",
          __FILE__,
          __LINE__);
      output->notify(NULL);
    }
    return output;
  }

  SSLSession*
  ssl_session_new() {
    if (ssl_context) {
      return ssl_context->session_new();
    }
    return NULL;
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

  CqlSessionRequest*
  shutdown() {
    std::unique_lock<std::mutex> lock(mutex);
    CqlSessionRequest* output = new CqlSessionRequest();
    return output;
  }

  void
  set_keyspace() {
  }
};

#endif
