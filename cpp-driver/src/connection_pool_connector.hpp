/*
  Copyright (c) DataStax, Inc.

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

#ifndef __CASS_CONNECTION_POOL_CONNECTOR_HPP_INCLUDED__
#define __CASS_CONNECTION_POOL_CONNECTOR_HPP_INCLUDED__

#include "address.hpp"
#include "atomic.hpp"
#include "connection_pool.hpp"
#include "pooled_connector.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "vector.hpp"

#include <uv.h>

namespace cass {

class ConnectionPoolManager;
class EventLoopGroup;

/**
 * A connector for a connection pool. This handles the connection process for a
 * connection pool.
 */
class ConnectionPoolConnector : public RefCounted<ConnectionPoolConnector> {
public:
  typedef SharedRefPtr<ConnectionPoolConnector> Ptr;
  typedef Vector<Ptr> Vec;

  typedef void (*Callback)(ConnectionPoolConnector*);

  /**
   * Constructor
   *
   * @param manager The manager for the pool.
   * @param address The address to connect to.
   * @param data User data that's passed to the callback.
   * @param callback A callback that is called when the connection is connected or
   * if an error occurred.
   */
  ConnectionPoolConnector(ConnectionPoolManager* manager,
                          const Address& address,
                          void* data, Callback callback);
  ~ConnectionPoolConnector();

  /**
   * Connect a pool.
   *
   * @param event_loop_group An event loop group to use run the connection process.
   */
  void connect(EventLoopGroup* event_loop_group);

  /**
   * Cancel the connection process.
   */
  void cancel();

public:
  const ConnectionPool::Ptr& pool() { return pool_; }
  const Address& address() const { return pool_->address(); }
  void* data() { return data_; }

  Connector::ConnectionError error_code() const;
  String error_message() const;

  bool is_ok() const;
  bool is_critical_error() const;
  bool is_keyspace_error() const;

private:
  static void on_connect(PooledConnector* connector, EventLoop* event_loop);
  void handle_connect(PooledConnector* connector, EventLoop* event_loop);

private:
  ConnectionPool::Ptr pool_;

  void* data_;
  Callback callback_;

  Atomic<size_t> remaining_;

  mutable uv_mutex_t lock_;
  PooledConnector::Vec pending_connections_;
  PooledConnector::Ptr critical_error_connector_;
};

} // namespace cass

#endif
