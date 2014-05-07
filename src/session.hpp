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

#ifndef __CASS_SESSION_HPP_INCLUDED__
#define __CASS_SESSION_HPP_INCLUDED__

#include <uv.h>

#include <atomic>
#include <list>
#include <string>
#include <vector>
#include <memory>
#include <unordered_set>

#include "error.hpp"
#include "mpmc_queue.hpp"
#include "spsc_queue.hpp"
#include "pool.hpp"
#include "io_worker.hpp"
#include "load_balancing_policy.hpp"
#include "round_robin_policy.hpp"
#include "resolver.hpp"
#include "config.hpp"

namespace cass {

struct Session {
  enum SessionState {
    SESSION_STATE_NEW,
    SESSION_STATE_CONNECTING,
    SESSION_STATE_READY,
    SESSION_STATE_DISCONNECTING,
    SESSION_STATE_DISCONNECTED
  };

  std::atomic<SessionState> state_;
  uv_thread_t                  thread_;
  uv_loop_t*                   loop_;
  uv_async_t                   async_connect_;
  std::vector<IOWorker*>    io_workers_;
  SSLContext*                  ssl_context_;
  std::string                  keyspace_;
  SessionFutureImpl*        connect_session_request_;
  std::unordered_set<Host>  hosts_;
  Config                       config_;
  std::unique_ptr<AsyncQueue<MPMCQueue<Request*>>> request_queue_;
  std::unique_ptr<LoadBalancingPolicy> load_balancing_policy_;

  Session() 
    : loop_(uv_loop_new())
    , load_balancing_policy_(new RoundRobinPolicy()) {
  }

  Session(const Session* session)
    : loop_(uv_loop_new())
    , config_(session->config_)
    , load_balancing_policy_(new RoundRobinPolicy()) {
  }

  int init() {
    async_connect_.data = this;
    uv_async_init( loop_, &async_connect_, &Session::on_connect);

    request_queue_.reset(new AsyncQueue<MPMCQueue<Request*>>(config_.queue_size_io()));
    int rc = request_queue_->init(loop_, this, &Session::on_execute);
    if(rc != 0) {
      return rc;
    }

    for (size_t i = 0; i < config_.thread_count_io(); ++i) {
      IOWorker* io_worker = new IOWorker(config_);
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
    Session* session = reinterpret_cast<Session*>(data);

    for(IOWorker* io_worker : session->io_workers_) {
      io_worker->run();
    }

    uv_run(session->loop_, UV_RUN_DEFAULT);
  }

  Future*
  connect(
      const char* ks) {
    return connect(std::string(ks));
  }

  Future*
  connect(
      const std::string& ks) {
    connect_session_request_       = new SessionFutureImpl();
    connect_session_request_->data = this;

    SessionState expected_state = SESSION_STATE_NEW;
    if (state_.compare_exchange_strong(
            expected_state,
            SESSION_STATE_CONNECTING)) {

      init();

      keyspace_ = ks;
      uv_thread_create(
          &thread_,
          &Session::on_run,
          this);

      uv_async_send(&async_connect_);
    } else {
     connect_session_request_->error.reset(new Error(
          CASS_ERROR_SOURCE_LIBRARY,
          CASS_ERROR_LIB_SESSION_STATE,
          "connect has already been called",
          __FILE__,
          __LINE__));
      connect_session_request_->notify(loop_);
    }
    return connect_session_request_;
  }

  void
  add_or_renew_pool(Host host,
      bool is_host_addition) {
    // TODO(mstump)
    for (IOWorker* io_worker :  io_workers_) {
      io_worker->add_pool(host, config_.core_connections_per_host(), config_.max_connections_per_host());
    }
  }

  static void
  on_connect(
      uv_async_t* data,
      int         status) {
    Session* session = reinterpret_cast<Session*>(data->data);

    // TODO(mstump)

    int port = session->config_.port();
    for(std::string seed : session->config_.contact_points()) {
      Address address;
      if(Address::from_string(seed, port, &address)) {
        Host host(address);
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
      Session* session = reinterpret_cast<Session*>(resolver->data());
      Host host(resolver->address());
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

  MessageFutureImpl*
  prepare(
      const char* statement,
      size_t      length) {
    Request* request = new Request(
        new MessageFutureImpl(),
        new Message(CQL_OPCODE_PREPARE));

    request->future->data.assign(statement, length);

    PrepareStatement* body =
        reinterpret_cast<PrepareStatement*>(request->message->body.get());
    body->prepare_string(statement, length);
    execute(request);
    return request->future;
  }

  MessageFutureImpl*
  execute(
      Message* message) {
    Request* request = new Request(
        new MessageFutureImpl(),
        message);
    execute(request);
    return request->future;
  }

  inline void
  execute(
      Request* request) {
    if (!request_queue_->enqueue(request)) {
      request->future->error.reset(new Error(
          CASS_ERROR_SOURCE_LIBRARY,
          CASS_ERROR_LIB_NO_STREAMS,
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
    Session* session = reinterpret_cast<Session*>(data->data);

    Request* request = nullptr;
    while (session->request_queue_->dequeue(request)) {

      session->load_balancing_policy_->new_query_plan(&request->hosts);
      // TODO(mstump)
      // choose pool
      // ClientConnection* connection = nullptr;
      // pool->borrow_connection(host, connection);
      // connection->send_message_threadsafe(request);
    }
  }

  Future*
  shutdown() {
    SessionFutureImpl* connect_session_request = new SessionFutureImpl();
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

} // namespace cass

#endif
