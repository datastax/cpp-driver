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
#include "pooled_connection.hpp"
#include "pooled_connector.hpp"

#include <uv.h>

namespace cass {

class ConnectionPoolConnector;
class ConnectionPoolManager;
class EventLoop;

/**
 * A pool of connections to the same host.
 */
class ConnectionPool : public RefCounted<ConnectionPool> {
public:
  typedef SharedRefPtr<ConnectionPool> Ptr;
  typedef DenseHashMap<Address, Ptr, AddressHash> Map;

  /**
   * Constructor. Don't use directly.
   *
   * @param manager The manager for this pool.
   * @param address The address for this pool.
   */
  ConnectionPool(ConnectionPoolManager* manager, const Address& address);

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

public:
  ConnectionPoolManager* manager() const { return manager_; }
  const Address& address() const { return  address_; }

public:
  class Protected {
    friend class PooledConnection;
    friend class ConnectionPoolConnector;
    Protected() { }
    Protected(Protected const&) { }
  };

  /**
   * Add connection to the pool.
   *
   * @param connection A connection to add to the pool.
   * @param A key to restrict access to the method.
   */
  void add_connection(const PooledConnection::Ptr& connection, Protected);

  /**
   * Remove the connection and schedule a reconnection.
   *
   * @param connection A connection that closed.
   * @param A key to restrict access to the method.
   */
  void close_connection(PooledConnection* connection, Protected);

  /**
   * Notify the pool manager that the host is up/down
   *
   * @param A key to restrict access to the method.
   */
  void notify_up_or_down(Protected);

  /**
   * Notify the pool manager that the host is critical
   *
   * @param code The code of the critical error.
   * @param message The message of the critical error.
   * @param A key to restrict access to the method.
   */
  void notify_critical_error(Connector::ConnectionError code,
                             const String& message,
                             Protected);

  /**
   * Schedule a new connection.
   *
   * @param A key to restrict access to the method.
   */
  void schedule_reconnect(Protected);

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
  void internal_notify_up_or_down();
  void internal_notify_critical_error(Connector::ConnectionError code,
                                      const String& message);
  void internal_add_connection(const PooledConnection::Ptr& connection);
  void internal_schedule_reconnect();
  void internal_close();
  void maybe_closed();

  static void on_reconnect(PooledConnector* connector);
  void handle_reconnect(PooledConnector* connector);

private:
  ConnectionPoolManager* manager_;
  Address address_;

  CloseState close_state_;
  NotifyState notify_state_;
  PooledConnection::Vec connections_;
  PooledConnector::Vec pending_connections_;
  DenseHashSet<PooledConnection*> to_flush_;
};

} // namespace cass

#endif
