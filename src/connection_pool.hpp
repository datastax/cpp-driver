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

#ifndef __CASS_CONNECTION_POOL_HPP_INCLUDED__
#define __CASS_CONNECTION_POOL_HPP_INCLUDED__

#include "address.hpp"
#include "delayed_connector.hpp"
#include "pooled_connection.hpp"

#include <uv.h>

namespace cass {

class ConnectionPool;
class ConnectionPoolConnector;
class ConnectionPoolManager;
class EventLoop;

class ConnectionPoolStateListener {
public:
  virtual ~ConnectionPoolStateListener() { }

  /**
   * A callback that's called when a host is up.
   *
   * @param address The address of the host.
   */
  virtual void on_pool_up(const Address& address) = 0;

  /**
   * A callback that's called when a host is down.
   *
   * @param address The address of the host.
   */
  virtual void on_pool_down(const Address& address) = 0;

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
  virtual void on_pool_critical_error(const Address& address,
                                      Connector::ConnectionError code,
                                      const String& message) = 0;
};


class ConnectionPoolListener : public ConnectionPoolStateListener {
public:
  virtual void on_requires_flush(ConnectionPool* pool) { }

  virtual void on_close(ConnectionPool* pool) = 0;
};

/**
 * The connection pool settings.
 */
struct ConnectionPoolSettings {
  /**
   * Constructor. Initialize with default settings.
   */
  ConnectionPoolSettings();

  /**
   * Constructor. Initialize the pool settings from a config object.
   *
   * @param config The config object.
   */
  ConnectionPoolSettings(const Config& config);

  ConnectionSettings connection_settings;
  size_t num_connections_per_host;
  uint64_t reconnect_wait_time_ms;
};

/**
 * A pool of connections to the same host.
 */
class ConnectionPool : public RefCounted<ConnectionPool> {
public:
  typedef SharedRefPtr<ConnectionPool> Ptr;

  class Map : public DenseHashMap<Address, Ptr, AddressHash> {
  public:
    Map() {
      set_empty_key(Address::EMPTY_KEY);
      set_deleted_key(Address::DELETED_KEY);
    }
  };

  /**
   * Constructor. Don't use directly.
   *
   * @param connections
   * @param listener
   * @param keyspace
   * @param loop
   * @param address
   * @param protocol_version
   * @param settings
   * @param metrics
   */
  ConnectionPool(const Connection::Vec& connections,
                 ConnectionPoolListener* listener,
                 const String& keyspace,
                 uv_loop_t* loop,
                 const Address& address,
                 int protocol_version,
                 const ConnectionPoolSettings& settings,
                 Metrics* metrics);

  /**
   * Find the least busy connection for the pool. The least busy connection has
   * the lowest number of outstanding requests.
   *
   * @return The least busy connection or null if no connection is available.
   */
  PooledConnection::Ptr find_least_busy() const;

  /**
   * Flush connections with pending writes.
   */
  void flush();

  /**
   * Close the pool.
   */
  void close();

  /**
   * Set the listener that will handle events for the pool.
   *
   * @param listener The pool listener.
   */
  void set_listener(ConnectionPoolListener* listener);

public:
  const uv_loop_t* loop() const { return loop_; }
  const Address& address() const { return  address_; }
  int protocol_version() const { return protocol_version_; }
  const String& keyspace() const { return keyspace_; }

  void set_keyspace(const String& keyspace);

public:
  class Protected {
    friend class PooledConnection;
    friend class ConnectionPoolConnector;
    Protected() { }
    Protected(Protected const&) { }
  };

  /**
   * Remove the connection and schedule a reconnection.
   *
   * @param connection A connection that closed.
   * @param A key to restrict access to the method.
   */
  void close_connection(PooledConnection* connection, Protected);

  /**
   * Add a connection to be flushed.
   *
   * @param connection A connection with pending writes.
   */
  void requires_flush(PooledConnection* connection, Protected);

private:
  enum CloseState {
    CLOSE_STATE_OPEN,
    CLOSE_STATE_CLOSING,
    CLOSE_STATE_WAITING_FOR_CONNECTIONS,
    CLOSE_STATE_CLOSED
  };

  enum NotifyState {
    NOTIFY_STATE_NEW,
    NOTIFY_STATE_UP,
    NOTIFY_STATE_DOWN,
    NOTIFY_STATE_CRITICAL
  };

private:
  friend class NotifyDownOnRemovePoolOp;

private:
  void notify_up_or_down();
  void notify_critical_error(Connector::ConnectionError code,
                             const String& message);
  void add_connection(const PooledConnection::Ptr& connection);
  void schedule_reconnect();
  void internal_close();
  void maybe_closed();

  void on_reconnect(DelayedConnector* connector);

private:
  ConnectionPoolListener* listener_;
  String keyspace_;
  uv_loop_t* const loop_;
  const Address address_;
  const int protocol_version_;
  const ConnectionPoolSettings settings_;
  Metrics* const metrics_;

  CloseState close_state_;
  NotifyState notify_state_;
  PooledConnection::Vec connections_;
  DelayedConnector::Vec pending_connections_;
  DenseHashSet<PooledConnection*> to_flush_;
};

} // namespace cass

#endif
