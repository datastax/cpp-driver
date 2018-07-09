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

#ifndef __CASS_CONNECTION_POOL_MANAGER_HPP_INCLUDED__
#define __CASS_CONNECTION_POOL_MANAGER_HPP_INCLUDED__

#include "address.hpp"
#include "atomic.hpp"
#include "connection_pool.hpp"
#include "connection_pool_connector.hpp"
#include "histogram_wrapper.hpp"
#include "ref_counted.hpp"
#include "string.hpp"

#include <uv.h>

namespace cass {

class EventLoop;

/**
 * A listener that handles connection pool events.
 */
class ConnectionPoolManagerListener : public ConnectionPoolStateListener {
public:
  virtual ~ConnectionPoolManagerListener() { }

  /**
   * A callback that's called when one of the manager's connections requires a
   * flush. It's only called once on the first write to the connection.
   */
  virtual void on_requires_flush() { }

  /**
   * A callback that's called when a manager is closed.
   *
   * @param manager The manager object that's closing.
   */
  virtual void on_close(ConnectionPoolManager* manager) = 0;
};

/**
 * A manager for one or more connection pools to different hosts.
 */
class ConnectionPoolManager
    : public RefCounted<ConnectionPoolManager>
    , public ConnectionPoolListener {
public:
  typedef SharedRefPtr<ConnectionPoolManager> Ptr;

  /**
   * Constructor. Don't use directly.
   *
   * @param pools
   * @param loop Event loop to utilize for handling requests.
   * @param protocol_version The protocol version to use for connections.
   * @param keyspace The current keyspace to use for connections.
   ( @param listener A listener that handles manager events.
   * @param metrics An object for recording metrics.
   * @param settings Settings for the manager and its connections.
   */
  ConnectionPoolManager(const ConnectionPool::Map& pools,
                        uv_loop_t* loop,
                        int protocol_version,
                        const String& keyspace,
                        ConnectionPoolManagerListener* listener,
                        Metrics* metrics,
                        const ConnectionPoolSettings& settings);
  ~ConnectionPoolManager();

  /**
   * Find the least busy connection for a given host.
   *
   * @param address The address of the host to find a least busy connection.
   * @return The least busy connection for a host or null if no connections are
   * available.
   */
  PooledConnection::Ptr find_least_busy(const Address& address) const;

  /**
   * Flush connection pools with pending writes.
   */
  void flush();

  /**
   * Get addresses for all available hosts.
   *
   * @return A vector of addresses.
   */
  AddressVec available() const;

  /**
   * Add a connection pool for the given host.
   *
   * @param address The address of the host to add.
   */
  void add(const Address& address);

  /**
   * Remove a connection pool for the given host.
   *
   * @param address The address of the host to remove.
   */
  void remove(const Address& address);

  /**
   * Close all connection pools.
   */
  void close();

  /**
   * Set the listener that will handle events for the connection pool manager.
   *
   * @param listener The connection pool manager listener.
   */
  void set_listener(ConnectionPoolManagerListener* listener);

public:
  uv_loop_t* loop() const { return loop_; }
  int protocol_version() const { return protocol_version_; }
  const ConnectionPoolSettings& settings() const { return settings_; }
  ConnectionPoolManagerListener* listener() const { return listener_; }

  String keyspace() const;
  void set_keyspace(const String& keyspace);

  Metrics* metrics() const { return metrics_; }

#ifdef CASS_INTERNAL_DIAGNOSTICS
  HistogramWrapper& flush_bytes() { return flush_bytes_; }
#endif

private:
  // Connection pool listener methods

  virtual void on_pool_up(const Address& address);

  virtual void on_pool_down(const Address& address);

  virtual void on_pool_critical_error(const Address& address,
                                 Connector::ConnectionError code,
                                 const String& message);

  virtual void on_requires_flush(ConnectionPool* pool);

  virtual void on_close(ConnectionPool* pool);

private:
  enum CloseState {
    CLOSE_STATE_OPEN,
    CLOSE_STATE_CLOSING,
    CLOSE_STATE_CLOSED
  };

private:
  void add_pool(const ConnectionPool::Ptr& pool);
  bool maybe_closed();

private:
  void on_connect(ConnectionPoolConnector* pool_connector);

private:
  uv_loop_t* loop_;

  const int protocol_version_;
  ConnectionPoolManagerListener* listener_;
  const ConnectionPoolSettings settings_;

  CloseState close_state_;
  ConnectionPool::Map pools_;
  ConnectionPoolConnector::Vec pending_pools_;
  DenseHashSet<ConnectionPool*> to_flush_;

  // This lock ensures keyspaces are updated as soon as it occurs
  mutable uv_mutex_t keyspace_mutex_;
  String keyspace_;

  Metrics* const metrics_;

#ifdef CASS_INTERNAL_DIAGNOSTICS
  HistogramWrapper flush_bytes_;
#endif
};

} // namespace cass

#endif
