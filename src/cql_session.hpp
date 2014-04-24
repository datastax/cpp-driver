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
#include <atomic>
#include <list>
#include <string>
#include <vector>
#include "cql_error.hpp"
#include "cql_mpmc_queue.hpp"
#include "cql_spsc_queue.hpp"
#include "cql_pool.hpp"
#include "cql_io_worker.hpp"

typedef int (*LoadBalancerDistanceCallback)(char* host, size_t host_size);

struct CqlSession {
  enum CqlSessionState {
    SESSION_STATE_NEW,
    SESSION_STATE_CONNECTING,
    SESSION_STATE_READY,
    SESSION_STATE_DISCONNECTING,
    SESSION_STATE_DISCONNECTED
  };

  std::atomic<CqlSessionState> state;
  uv_thread_t                  thread;
  uv_loop_t*                   loop;
  uv_async_t                   async_connect;
  uv_async_t                   async_request;
  std::vector<IOWorker*>       io_loops;
  SSLContext*                  ssl_context;
  LogCallback                  log_callback;
  std::string                  keyspace;
  CqlSessionFutureImpl*        connect_session_request;
  LoadBalancerDistanceCallback distance_callback;
  MPMCQueue<CqlMessageRequest> request_queue;

  CqlSession(
      size_t io_loop_count,
      size_t io_queue_size) :
      loop(uv_loop_new()),
      io_loops(io_loop_count, NULL),
      request_queue(1024) {
    async_connect.data = this;
    async_request.data = this;

    uv_async_init(
        loop,
        &async_connect,
        &CqlSession::on_connect_called);

    uv_async_init(
        loop,
        &async_request,
        &CqlSession::on_connect_called);

    for (size_t i = 0; i < io_loops.size(); ++i) {
      io_loops[i] = new IOWorker(io_queue_size);
    }
  }

  static void
  on_run(
      void* data) {
    CqlSession* session = reinterpret_cast<CqlSession*>(data);
    uv_run(session->loop, UV_RUN_DEFAULT);

    for (size_t i = 0; i < session->io_loops.size(); ++i) {
      session->io_loops[i]->run();
    }
  }

  CqlSessionFutureImpl*
  connect(
      const char* ks) {
    return connect(std::string(ks));
  }

  CqlSessionFutureImpl*
  connect(
      const std::string& ks) {
    connect_session_request       = new CqlSessionFutureImpl();
    connect_session_request->data = this;

    CqlSessionState new_state = SESSION_STATE_NEW;
    if (state.compare_exchange_strong(
            new_state,
            SESSION_STATE_CONNECTING)) {
      keyspace = ks;
      uv_thread_create(
          &thread,
          &CqlSession::on_run,
          this);

      uv_async_send(&async_connect);
    } else {
     connect_session_request->error.reset(new CqlError(
          CQL_ERROR_SOURCE_LIBRARY,
          CQL_ERROR_LIB_SESSION_STATE,
          "connect has already been called",
          __FILE__,
          __LINE__));
      connect_session_request->notify(loop);
    }
    return connect_session_request;
  }

  void
  add_or_renew_pool(
      char*  host,
      size_t host_size,
      bool   is_host_addition) {

  }


  static void
  on_connect_called(
      uv_async_t* data,
      int         status) {
    CqlSession* session = reinterpret_cast<CqlSession*>(data->data);

  }

  SSLSession*
  ssl_session_new() {
    if (ssl_context) {
      return ssl_context->session_new();
    }
    return NULL;
  }

  CqlMessageFutureImpl*
  prepare(
      const char* statement,
      size_t      length) {
    CqlMessageRequest node(
        new CqlMessageFutureImpl(),
        new CqlMessage(CQL_OPCODE_PREPARE));

    node.future->data.assign(statement, length);

    CqlPrepareStatement* body =
        reinterpret_cast<CqlPrepareStatement*>(node.message->body.get());
    body->prepare_string(statement, length);

    if (request_queue.enqueue(node)) {
      uv_async_send(&async_request);
    } else {
      node.future->error.reset(new CqlError(
          CQL_ERROR_SOURCE_LIBRARY,
          CQL_ERROR_LIB_NO_STREAMS,
          "request queue full",
          __FILE__,
          __LINE__));
      node.future->use_local_loop = true;
      node.future->notify(NULL);
    }
    return node.future;
  }

  CqlMessageFutureImpl*
  execute() {
    return nullptr;
  }

  void
  on_request() {
    CqlMessageRequest node;

    while (request_queue.dequeue(node)) {
      // load balancing policy give me host
      // choose pool
      // ClientConnection* connection = nullptr;
      // pool->borrow_connection(host, connection);
      // connection->send_message_threadsafe(request);
    }
  }

  CqlSessionFutureImpl*
  shutdown() {
    CqlSessionFutureImpl* connect_session_request = new CqlSessionFutureImpl();
    return connect_session_request;
  }

  void
  set_keyspace() {
  }
};

#endif
