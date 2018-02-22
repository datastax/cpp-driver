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
#include "ref_counted.hpp"

#include <uv.h>

namespace cass {

class EventLoopGroup;
class RequestQueueManager;

/**
 * A listener that handles connection pool events.
 */
class ConnectionPoolManagerListener {
public:
  virtual ~ConnectionPoolManagerListener() { }

  /**
   * A callback that's called when a host is up.
   *
   * @param address The address of the host.
   */
  virtual void on_up(const Address& address) = 0;

  /**
   * A callback that's called when a host is down.
   *
   * @param address The address of the host.
   */
  virtual void on_down(const Address& address) = 0;

  /**
   * A callback that's called when a host has a critical error
   * during reconnection.
   *
   * The following are critical errors:
   * * Invalid keyspace
   * * Invalid protocol version
   * * Authentication failure
   * * SSL failure
   *
   * @param address The address of the host.
   * @param code The code of the critical error.
   * @param message The message of the critical error.
   */
  virtual void on_critical_error(const Address& address,
                                 Connector::ConnectionError code,
                                 const String& message) = 0;

  /**
   * A callback that's called when a manager is closed.
   */
  virtual void on_close() { }
};

/**
 * The connection pool manager settings.
 */
struct ConnectionPoolManagerSettings {
  ConnectionPoolManagerSettings()
    : num_connections_per_host(1)
    , reconnect_wait_time_ms(2000) { }

  /**
   * Constructor. Initialize manager settings from a config object.
   *
   * @param config The config object.
   */
  ConnectionPoolManagerSettings(const Config& config);

  ConnectionSettings connection_settings;
  size_t num_connections_per_host;
  uint64_t reconnect_wait_time_ms;
};

/**
 * A manager for one or more connection pools to different hosts.
 */
class ConnectionPoolManager : public RefCounted<ConnectionPoolManager> {
public:
  typedef SharedRefPtr<ConnectionPoolManager> Ptr;

  /**
   * Constructor. Don't use directly.
   *
   * @param request_queue_manager A request queue manager for handling requests.
   * @param protocol_version The protocol version to use for connections.
   * @param keyspace The current keyspace to use for connections.
   * @param listener A listener that handles manager events.
   * @param metrics An object for recording metrics.
   * @param settings Settings for the manager and its connections.
   */
  ConnectionPoolManager(RequestQueueManager* request_queue_manager,
                        int protocol_version,
                        const String& keyspace,
                        ConnectionPoolManagerListener* listener,
                        Metrics* metrics,
                        const ConnectionPoolManagerSettings& settings);
  ~ConnectionPoolManager();

  /**
   * Find the least busy connection for a given host (thread-safe).
   *
   * @param address The address of the host to find a least busy connection.
   * @return The least busy connection for a host or null if no connections are
   * available.
   */
  PooledConnection::Ptr find_least_busy(const Address& address) const;

  /**
   * Get addresses for all available hosts (thread-safe).
   *
   * @return A vector of addresses.
   */
  AddressVec available() const;

  /**
   * Add a connection pool for the given host (thread-safe).
   *
   * @param address The address of the host to add.
   */
  void add(const Address& address);

  /**
   * Remove a connection pool for the given host (thread-safe).
   *
   * @param address The address of the host to remove.
   */
  void remove(const Address& address);


  /**
   * Close all connection pools (thread-safe).
   */
  void close();

public:
  EventLoopGroup* event_loop_group() const;
  RequestQueueManager* request_queue_manager() const { return request_queue_manager_; }
  int protocol_version() const { return protocol_version_; }
  const ConnectionPoolManagerSettings& settings() const { return settings_; }
  ConnectionPoolManagerListener* listener() const { return listener_; }

  String keyspace() const;
  void set_keyspace(const String& keyspace);

  Metrics* metrics() const { return metrics_; }

public:
  class Protected {
    friend class ConnectionPool;
    friend class ConnectionPoolManagerInitializer;
    Protected() { }
    Protected(Protected const&) { }
  };

  /**
   * Add a connection pool from the event loop thread.
   *
   * @param pool A pool to add.
   * @param A key to restrict access to the method.
   */
  void add_pool(const ConnectionPool::Ptr& pool, Protected);

  /**
   * Remove a connection pool from the event loop thread.
   *
   * @param pool A pool to remove.
   * @param A key to restrict access to the method.
   */
  void remove_pool(ConnectionPool* pool, Protected);

  /**
   * Notify that a pool is up.
   *
   * @param pool A pool that now has connections available.
   * @param A key to restrict access to the method.
   */
  void notify_up(ConnectionPool* pool, Protected);

  /**
   * Notify that a pool is down.
   *
   * @param pool A pool that now has no more connection available.
   * @param A key to restrict access to the method.
   */
  void notify_down(ConnectionPool* pool, Protected);

  /**
   * Notify that a pool has had a critical error.
   *
   * @param pool A pool that had a critical error.
   * @param A key to restrict access to the method.
   */
  void notify_critical_error(ConnectionPool* pool,
                             Connector::ConnectionError code,
                             const String& message,
                             Protected);

private:
  void internal_add_pool(const ConnectionPool::Ptr& pool);
  void internal_remove_pool(const Address& address);
  void maybe_closed(ScopedWriteLock& wl);

private:
  static void on_connect(ConnectionPoolConnector* pool_connector);
  void handle_connect(ConnectionPoolConnector* pool_connector);

private:
  RequestQueueManager* const request_queue_manager_;

  const int protocol_version_;
  ConnectionPoolManagerListener* const listener_;
  const ConnectionPoolManagerSettings settings_;

  mutable uv_rwlock_t rwlock_;
  bool is_closing_;
  ConnectionPool::Map pools_;
  ConnectionPoolConnector::Vec pending_pools_;

  mutable uv_rwlock_t keyspace_rwlock_;
  String keyspace_;
  Metrics* const metrics_;
};

} // namespace cass

#endif
