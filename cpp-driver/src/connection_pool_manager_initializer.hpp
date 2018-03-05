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

#ifndef __CASS_CONNECTION_POOL_MANAGER_INITIALIZER_HPP_INCLUDED__
#define __CASS_CONNECTION_POOL_MANAGER_INITIALIZER_HPP_INCLUDED__

#include "address.hpp"
#include "atomic.hpp"
#include "connection_pool_connector.hpp"
#include "connection_pool_manager.hpp"
#include "ref_counted.hpp"
#include "string.hpp"

#include <uv.h>

namespace cass {

class Config;
class Metrics;
class RequestQueueManager;

/**
 * An initializer for a connection pool manager. This connects many connection
 * pools to different hosts.
 */
class ConnectionPoolManagerInitializer : public RefCounted<ConnectionPoolManagerInitializer> {
public:
  typedef SharedRefPtr<ConnectionPoolManagerInitializer> Ptr;

  typedef void (*Callback)(ConnectionPoolManagerInitializer*);

  /**
   * Constructor
   *
   * @param request_queue_manager A request queue manager to use for handling requests.
   * @param protocol_version The protocol version to use for connections.
   * @param data User data that's passed to the callback.
   * @param callback A callback that is called when the manager is connected or
   * if an error occurred.
   */
  ConnectionPoolManagerInitializer(RequestQueueManager* request_queue_manager,
                                   int protocol_version,
                                   void* data, Callback callback);
  ~ConnectionPoolManagerInitializer();

  /**
   * Initialize a connection pool manager use the given hosts.
   *
   * @param hosts A vector of addresses to connect pools to.
   */
  void initialize(const AddressVec& hosts);

  /**
   * Set the keyspace to connect pools with.
   *
   * @param keyspace A keyspace to register after connection.
   * @return The initializer to chain calls.
   */
  ConnectionPoolManagerInitializer* with_keyspace(const String& keyspace);

  /**
   * Set the listener that handles connection pool events.
   *
   * @param listener A listener that handles connection pool events.
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
  ConnectionPoolManagerInitializer* with_settings(const ConnectionPoolManagerSettings& settings);


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

public:
  void* data() { return data_; }

private:
  static void on_connect(ConnectionPoolConnector* pool_connector);
  void handle_connect(ConnectionPoolConnector* pool_connector);

private:
  ConnectionPoolManager::Ptr manager_;

  void* data_;
  Callback callback_;

  Atomic<size_t> remaining_;

  mutable uv_mutex_t lock_;
  ConnectionPoolConnector::Vec failures_;

  RequestQueueManager* const request_queue_manager_;

  int protocol_version_;
  String keyspace_;
  ConnectionPoolManagerListener* listener_;
  Metrics* metrics_;
  ConnectionPoolManagerSettings settings_;
};

} // namespace cass

#endif
