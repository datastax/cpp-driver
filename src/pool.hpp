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

#include "bound_queue.hpp"
#include "client_connection.hpp"
#include "request.hpp"
#include "session.hpp"

namespace cass {

class Pool {
  typedef std::list<ClientConnection*> ConnectionCollection;

  Session* session_;
  uv_loop_t*              loop_;
  SSLContext*             ssl_context_;
  Host                 host_;
  size_t                  core_connections_per_host_;
  size_t                  max_connections_per_host_;
  size_t                  max_simultaneous_creation_;
  ConnectionCollection    connections_;
  ConnectionCollection    connections_pending_;
  BoundQueue<Request*> request_queue_;

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
      request_queue_(128 * max_connections_per_host) {
    for (size_t i = 0; i < core_connections_per_host_; ++i) {
      spawn_connection();
    }
  }

  void
  connect_callback(
      ClientConnection* connection,
      Error*            error) {

    session_->notify_connect_q(host_);

    if (error) {
      // TODO(mstump) do something with failure to connect
    }

    // TODO(mstump) do we need notification?
    connections_pending_.remove(connection);
    connections_.push_back(connection);
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
            std::placeholders::_2));

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
    *output = find_least_busy();
    if (output) {
      return true;
    }

    if (connections_.size() >= max_connections_per_host_) {
      return false;
    }

    // TODO(mpenick): create new connection

    return false;
  }
};

} // namespace cass

#endif
