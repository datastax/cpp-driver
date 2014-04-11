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

#ifndef __POOL_HPP_INCLUDED__
#define __POOL_HPP_INCLUDED__

#include <list>
#include <string>
#include <algorithm>

#include "cql_client_connection.hpp"
#include "cql_cluster.hpp"

namespace cql {

class Pool {
  typedef std::list<cql::ClientConnection*> ConnectionCollection;

  uv_loop_t*           loop_;
  SSLContext*          ssl_context_;
  std::string          address_;
  // HostDistance         distance_;
  size_t               core_connections_per_host_;
  size_t               max_connections_per_host_;
  size_t               max_simultaneous_creation_;
  ConnectionCollection connections_;
  ConnectionCollection connections_pending_;

 public:
  Pool(
      uv_loop_t*         loop,
      SSLContext*        ssl_context,
      const std::string& address,
//      HostDistance       distance,
      size_t             core_connections_per_host,
      size_t             max_connections_per_host,
      size_t             max_simultaneous_creation = 1) :
      loop_(loop),
      ssl_context_(ssl_context),
      address_(address),
//      distance_(distance),
      core_connections_per_host_(core_connections_per_host),
      max_connections_per_host_(max_connections_per_host),
      max_simultaneous_creation_(max_simultaneous_creation) {
    for (size_t i = 0; i < core_connections_per_host_; ++i) {
      spawn_connection();
    }
  }

  void
  connect_callback(
      ClientConnection* connection,
      cql::Error*       error) {

    (void) connection;
    (void) error;

  }

  ~Pool() {
    for (auto c : connections_) {
      delete c;
    }
  }

  void
  prepare() {
  }

  void
  execute() {
  }

  void
  shutdown() {
  }

  void
  set_keyspace() {
  }

  void
  spawn_connection() {
    ClientConnection* connection = new ClientConnection(
        loop_,
        ssl_context_ ? ssl_context_->session_new() : NULL);

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

  Error*
  borrow_connection(
      ClientConnection** output) {
    *output = find_least_busy();
    if (output) {
      return CQL_ERROR_NO_ERROR;
    }

    if (connections_.size() >= max_connections_per_host_) {
      return new Error(
          CQL_ERROR_SOURCE_LIBRARY,
          CQL_ERROR_LIB_MAX_CONNECTIONS,
          "all connections busy",
          __FILE__,
          __LINE__);
    }

    return CQL_ERROR_NO_ERROR;
  }


};
}
#endif
