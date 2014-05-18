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

  Session* session_;
  uv_loop_t*              loop_;
  SSLContext*             ssl_context_;
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
  Pool(Session* session,
      uv_loop_t*     loop,
      SSLContext*    ssl_context,
      const Host& host,
      size_t         core_connections_per_host,
      size_t         max_connections_per_host,
      size_t         max_simultaneous_creation = 1) :
      session_(session),
      loop_(loop),
      ssl_context_(ssl_context),
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

  void connect_callback(
      ClientConnection* connection,
      Error* error) {

    session_->notify_connect_q(host_);
    connections_pending_.remove(connection);

    if (error) {
      delete error;
      // TODO(mstump) do something with failure to connect
    } else {
      connections_.push_back(connection);
      execute_pending_request(connection);
    }
  }

  void request_finished_callback(ClientConnection* connection) {
    execute_pending_request(connection);
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
        loop_,
        ssl_context_ ? ssl_context_->session_new() : NULL,
        host_);

    connection->init(
        std::bind(
            &Pool::connect_callback,
            this,
            std::placeholders::_1,
            std::placeholders::_2),
         std::bind(&Pool::request_finished_callback, this, std::placeholders::_1));

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
    if ((*it)->available_streams()) {
      return *it;
    }
    return nullptr;
  }

  bool
  borrow_connection(
      ClientConnection** output) {

    if(connections_.empty()) {
      for (size_t i = 0; i < core_connections_per_host_; ++i) {
        spawn_connection();
      }
      return false;
    }

    maybe_spawn_connection();

    *output = find_least_busy();
    if (*output) {
      return true;
    }

    return false;
  }

  void on_timeout(void* data) {
    RequestFuture* request = static_cast<RequestFuture*>(data);
    // TODO(mpenick): Maybe we want to move to the next host here?
    pending_request_queue_.remove(request);
  }

  bool wait_for_connection(RequestFuture* request) {
    if(pending_request_queue_.size() + 1 > max_pending_requests_) {
      return false;
    }
    Timer* timer = Timer::start(loop_, connection_timeout_,
                                request,
                                std::bind(&Pool::on_timeout, this, std::placeholders::_1));
    request->timer = timer;
    pending_request_queue_.push_back(request);
    return true;
  }

  void execute_pending_request(ClientConnection* connection) {
    if(!pending_request_queue_.empty()) {
      RequestFuture* request = pending_request_queue_.front();
      if(request->timer) {
        Timer::stop(request->timer);
        request->timer = nullptr;
      }
      Error* error = nullptr;
      if(!connection->execute(request->message, request, &error)) {
        request->set_error(error);
      }
    }
  }
};

} // namespace cass

#endif
