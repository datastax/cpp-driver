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

#ifndef DATASTAX_INTERNAL_CONNECTION_POOL_MANAGER_INITIALIZER_HPP
#define DATASTAX_INTERNAL_CONNECTION_POOL_MANAGER_INITIALIZER_HPP

#include "address.hpp"
#include "atomic.hpp"
#include "callback.hpp"
#include "connection_pool_connector.hpp"
#include "connection_pool_manager.hpp"
#include "ref_counted.hpp"
#include "string.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class Config;
class Metrics;

/**
 * An initializer for a connection pool manager. This connects many connection
 * pools to different hosts.
 */
class ConnectionPoolManagerInitializer
    : public RefCounted<ConnectionPoolManagerInitializer>
    , public ConnectionPoolListener {
public:
  typedef SharedRefPtr<ConnectionPoolManagerInitializer> Ptr;

  typedef internal::Callback<void, ConnectionPoolManagerInitializer*> Callback;

  /**
   * Constructor
   *
   * @param protocol_version The protocol version to use for connections.
   * @param callback A callback that is called when the manager is connected or
   * if an error occurred.
   */
  ConnectionPoolManagerInitializer(ProtocolVersion protocol_version, const Callback& callback);

  /**
   * Initialize a connection pool manager use the given hosts.
   *
   * @param loop Event loop to utilize for handling requests.
   * @param hosts A map of hosts to connect pools to.
   */
  void initialize(uv_loop_t* loop, const HostMap& hosts);

  /**
   * Cancel the initialization process of the manager.
   */
  void cancel();

  /**
   * Set the keyspace to connect pools with.
   *
   * @param keyspace A keyspace to register after connection.
   * @return The initializer to chain calls.
   */
  ConnectionPoolManagerInitializer* with_keyspace(const String& keyspace);

  /**
   * Set the listener for the connection pool manager.
   *
   * @param listener Connection pool listener object.
   * @return The initializer to chain calls.
   */
  ConnectionPoolManagerInitializer* with_listener(ConnectionPoolManagerListener* listener);

  /**
   * Set the metrics object to use to record metrics.
   *
   * @param metrics A metrics object.
   * @return The initializer to chain calls.
   */
  ConnectionPoolManagerInitializer* with_metrics(Metrics* metrics);

  /**
   * Set settings to use for the manager and its connections.
   *
   * @param settings A manager settings object.
   * @return The initializer to chain calls.
   */
  ConnectionPoolManagerInitializer* with_settings(const ConnectionPoolSettings& settings);

  /**
   * Critical failures that happened during the connection process.
   *
   * @return A vector of pool connectors that failed.
   */
  ConnectionPoolConnector::Vec failures() const;

  /**
   * Release the manager from the initializer. If not released in the callback
   * the manager will automatically be closed.
   *
   * @return The manager object for this initializer. This returns a null object
   * if the manager is not connected or an error occurred.
   */
  ConnectionPoolManager::Ptr release_manager() {
    ConnectionPoolManager::Ptr temp = manager_;
    manager_.reset();
    return temp;
  }

private:
  // Connection pool listener methods

  virtual void on_pool_up(const Address& address);

  virtual void on_pool_down(const Address& address);

  virtual void on_pool_critical_error(const Address& address, Connector::ConnectionError code,
                                      const String& message);

  virtual void on_close(ConnectionPool* pool);

private:
  void on_connect(ConnectionPoolConnector* pool_connector);

private:
  uv_loop_t* loop_;
  ConnectionPoolManager::Ptr manager_;
  Callback callback_;
  bool is_canceled_;
  size_t remaining_;

  ConnectionPool::Map pools_;
  ConnectionPoolConnector::Vec pending_pools_;
  ConnectionPoolConnector::Vec failures_;

  ProtocolVersion protocol_version_;
  String keyspace_;
  ConnectionPoolManagerListener* listener_;
  Metrics* metrics_;
  ConnectionPoolSettings settings_;
};

}}} // namespace datastax::internal::core

#endif
