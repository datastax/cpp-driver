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
#include <memory>
#include <unordered_set>

#include "cql_error.hpp"
#include "cql_mpmc_queue.hpp"
#include "cql_spsc_queue.hpp"
#include "cql_pool.hpp"
#include "cql_io_worker.hpp"
#include "cql_load_balancing_policy.hpp"
#include "cql_round_robin_policy.hpp"
#include "cql_resolver.hpp"
#include "cql_config.hpp"

struct CqlSession {
  enum CqlSessionState {
    SESSION_STATE_NEW,
    SESSION_STATE_CONNECTING,
    SESSION_STATE_READY,
    SESSION_STATE_DISCONNECTING,
    SESSION_STATE_DISCONNECTED
  };

  std::atomic<CqlSessionState> state_;
  uv_thread_t                  thread_;
  uv_loop_t*                   loop_;
  uv_async_t                   async_connect_;
  std::vector<CqlIOWorker*>    io_workers_;
  SSLContext*                  ssl_context_;
  std::string                  keyspace_;
  CqlSessionFutureImpl*        connect_session_request_;
  std::unordered_set<CqlHost>  hosts_;
  Config                       config_;
  std::unique_ptr<AsyncQueue<MPMCQueue<CqlRequest*>>> request_queue_;
  std::unique_ptr<LoadBalancingPolicy> load_balancing_policy_;

  CqlSession() 
    : loop_(uv_loop_new())
    , load_balancing_policy_(new RoundRobinPolicy()) {
  }

  CqlSession(const CqlSession* session)
    : loop_(uv_loop_new())
    , config_(session->config_)
    , load_balancing_policy_(new RoundRobinPolicy()) {
  }

  int init() {
    async_connect_.data = this;
    uv_async_init( loop_, &async_connect_, &CqlSession::on_connect);

    request_queue_.reset(new AsyncQueue<MPMCQueue<CqlRequest*>>(config_.queue_size_io()));
    int rc = request_queue_->init(loop_, this, &CqlSession::on_execute);
    if(rc != 0) {
      return rc;
    }

    for (size_t i = 0; i < config_.thread_count_io(); ++i) {
      CqlIOWorker* io_worker = new CqlIOWorker(config_);
      int rc = io_worker->init();
      if(rc != 0) {
        return rc;
      }
      io_workers_.push_back(io_worker);
    }

    return 0;
  }

  static void
  on_run(
      void* data) {
    CqlSession* session = reinterpret_cast<CqlSession*>(data);

    for(CqlIOWorker* io_worker : session->io_workers_) {
      io_worker->run();
    }

    uv_run(session->loop_, UV_RUN_DEFAULT);
  }

  CqlSessionFutureImpl*
  connect(
      const char* ks) {
    return connect(std::string(ks));
  }

  CqlSessionFutureImpl*
  connect(
      const std::string& ks) {
    connect_session_request_       = new CqlSessionFutureImpl();
    connect_session_request_->data = this;

    CqlSessionState expected_state = SESSION_STATE_NEW;
    if (state_.compare_exchange_strong(
            expected_state,
            SESSION_STATE_CONNECTING)) {

      init();

      keyspace_ = ks;
      uv_thread_create(
          &thread_,
          &CqlSession::on_run,
          this);

      uv_async_send(&async_connect_);
    } else {
     connect_session_request_->error.reset(new CqlError(
          CQL_ERROR_SOURCE_LIBRARY,
          CQL_ERROR_LIB_SESSION_STATE,
          "connect has already been called",
          __FILE__,
          __LINE__));
      connect_session_request_->notify(loop_);
    }
    return connect_session_request_;
  }

  void
  add_or_renew_pool(CqlHost host,
      bool is_host_addition) {
    // TODO(mstump)
    for (CqlIOWorker* io_worker :  io_workers_) {
      io_worker->add_pool(host, config_.core_connections_per_host(), config_.max_connections_per_host());
    }
  }

  static void
  on_connect(
      uv_async_t* data,
      int         status) {
    CqlSession* session = reinterpret_cast<CqlSession*>(data->data);

    // TODO(mstump)

    int port = session->config_.port();
    for(std::string seed : session->config_.contact_points()) {
      Address address;
      if(Address::from_string(seed, port, &address)) {
        CqlHost host(address);
        if(session->hosts_.count(host) == 0) {
          session->hosts_.insert(host);
          session->add_or_renew_pool(host, false);
        }
      } else {
        Resolver::resolve(session->loop_, seed, port, session, on_resolve);
      }
    }
  }

  static void on_resolve(Resolver* resolver) {
    if(resolver->is_success()) {
      CqlSession* session = reinterpret_cast<CqlSession*>(resolver->data());
      CqlHost host(resolver->address());
      if(session->hosts_.count(host) == 0) {
        session->hosts_.insert(host);
        session->add_or_renew_pool(host, false);
      }
    } else {
      // TODO:(mpenick) log or handle
      fprintf(stderr, "Unable to resolve %s:%d\n", resolver->host().c_str(), resolver->port());
    }
  }

  SSLSession*
  ssl_session_new() {
    if (ssl_context_) {
      return ssl_context_->session_new();
    }
    return NULL;
  }

  CqlMessageFutureImpl*
  prepare(
      const char* statement,
      size_t      length) {
    CqlRequest* request = new CqlRequest(
        new CqlMessageFutureImpl(),
        new CqlMessage(CQL_OPCODE_PREPARE));

    request->future->data.assign(statement, length);

    CqlPrepareStatement* body =
        reinterpret_cast<CqlPrepareStatement*>(request->message->body.get());
    body->prepare_string(statement, length);
    execute(request);
    return request->future;
  }

  CqlMessageFutureImpl*
  execute(
      CqlMessage* message) {
    CqlRequest* request = new CqlRequest(
        new CqlMessageFutureImpl(),
        message);
    execute(request);
    return request->future;
  }

  inline void
  execute(
      CqlRequest* request) {
    if (!request_queue_->enqueue(request)) {
      request->future->error.reset(new CqlError(
          CQL_ERROR_SOURCE_LIBRARY,
          CQL_ERROR_LIB_NO_STREAMS,
          "request queue full",
          __FILE__,
          __LINE__));
      request->future->use_local_loop = true;
      request->future->notify(NULL);
      delete request;
    }
  }

  static void
  on_execute(
      uv_async_t* data,
      int         status) {
    CqlSession* session = reinterpret_cast<CqlSession*>(data->data);

    CqlRequest* request = nullptr;
    while (session->request_queue_->dequeue(request)) {

      session->load_balancing_policy_->new_query_plan(&request->hosts);
      // TODO(mstump)
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
    // TODO(mstump)
  }

  void
  set_keyspace() {
    // TODO(mstump)
  }

  void
  set_load_balancing_policy(LoadBalancingPolicy* policy) {
    load_balancing_policy_.reset(policy);
  }
};

#endif
