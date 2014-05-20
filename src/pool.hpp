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

#ifndef __CASS_POOL_HPP_INCLUDED__
#define __CASS_POOL_HPP_INCLUDED__

#include <list>
#include <string>
#include <algorithm>

#include "client_connection.hpp"
#include "request.hpp"
#include "session.hpp"
#include "timer.hpp"

namespace cass {

class Pool {
  typedef std::list<ClientConnection*> ConnectionCollection;

  IOWorker* io_worker_;
  Host                    host_;
  size_t                  core_connections_per_host_;
  size_t                  max_connections_per_host_;
  size_t                  max_simultaneous_creation_;
  ConnectionCollection    connections_;
  ConnectionCollection    connections_pending_;
  std::list<RequestFuture*> pending_request_queue_;
  size_t                  max_pending_requests_;
  uint64_t                connection_timeout_;

 public:
  Pool(IOWorker* io_worker,
      const Host& host,
      size_t         core_connections_per_host,
      size_t         max_connections_per_host,
      size_t         max_simultaneous_creation = 1) :
      io_worker_(io_worker),
      host_(host),
      core_connections_per_host_(core_connections_per_host),
      max_connections_per_host_(max_connections_per_host),
      max_simultaneous_creation_(max_simultaneous_creation),
      max_pending_requests_(128 * max_connections_per_host),
      connection_timeout_(1000) {
    for (size_t i = 0; i < core_connections_per_host_; ++i) {
      spawn_connection();
    }
  }

  void on_connect(
      ClientConnection* connection,
      Error* error) {

    io_worker_->session_->notify_connect_q(host_);
    connections_pending_.remove(connection);

    if (error) {
      delete error;
    } else {
      connections_.push_back(connection);
      execute_pending_request(connection);
    }
  }

  void on_close(ClientConnection* connection) {
    connections_.remove(connection);
    delete connection;
  }

  void on_request_finished(ClientConnection* connection, Message* message) {
    if(connection->is_ready()) {
      execute_pending_request(connection);
    }
  }

  ~Pool() {
    for (auto c : connections_) {
      delete c;
    }
  }

  void
  shutdown() {
    for (auto c : connections_) {
      c->shutdown();
    }
  }

  void
  set_keyspace() {
    // TODO(mstump)
  }

  void
  spawn_connection() {
    ClientConnection* connection = new ClientConnection(
        io_worker_->loop,
        io_worker_->ssl_context ? io_worker_->ssl_context->session_new() : NULL,
        host_,
        std::bind(&Pool::on_connect, this,
                  std::placeholders::_1, std::placeholders::_2),
        std::bind(&Pool::on_close, this,
          std::placeholders::_1));

    connection->connect();

    connections_pending_.push_back(connection);
  }

  void
  maybe_spawn_connection() {
    if (connections_pending_.size() >= max_simultaneous_creation_) {
      return;
    }

    if (connections_.size() + connections_pending_.size()
        >= max_connections_per_host_) {
      return;
    }

    spawn_connection();
  }

  static bool
  least_busy_comp(
      ClientConnection* a,
      ClientConnection* b) {
    return a->available_streams() < b->available_streams();
  }

  ClientConnection*
  find_least_busy() {
    ConnectionCollection::iterator it =
        std::max_element(
            connections_.begin(),
            connections_.end(),
            Pool::least_busy_comp);
    if ((*it)->is_ready() && (*it)->available_streams()) {
      return *it;
    }
    return nullptr;
  }

  ClientConnection* borrow_connection() {
    if(connections_.empty()) {
      for (size_t i = 0; i < core_connections_per_host_; ++i) {
        spawn_connection();
      }
      return nullptr;
    }

    maybe_spawn_connection();

    return find_least_busy();
  }

  bool execute(ClientConnection* connection, RequestFuture* request_future) {
    return connection->execute(request_future->message,
                               request_future,
                               std::bind(&Pool::on_request_finished, this,
                                         std::placeholders::_1, std::placeholders::_2));
  }

  void on_timeout(Timer* timer) {
    RequestFuture* request_future = static_cast<RequestFuture*>(timer->data());
    pending_request_queue_.remove(request_future);
    io_worker_->try_next_host(request_future);
  }

  bool wait_for_connection(RequestFuture* request_future) {
    if(pending_request_queue_.size() + 1 > max_pending_requests_) {
      return false;
    }
    request_future->timer = Timer::start(io_worker_->loop, connection_timeout_,
                                request_future,
                                std::bind(&Pool::on_timeout, this, std::placeholders::_1));
    pending_request_queue_.push_back(request_future);
    return true;
  }

  void execute_pending_request(ClientConnection* connection) {
    if(!pending_request_queue_.empty()) {
      RequestFuture* request_future = pending_request_queue_.front();
      pending_request_queue_.pop_front();
      if(request_future->timer) {
        Timer::stop(request_future->timer);
        request_future->timer = nullptr;
      }
      if(!execute(connection, request_future)) {
        io_worker_->try_next_host(request_future);
      }
    }
  }
};

} // namespace cass

#endif
