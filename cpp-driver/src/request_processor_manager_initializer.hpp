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

#ifndef __CASS_REQUEST_PROCESSOR_MANAGER_INITIALIZER_HPP_INCLUDED__
#define __CASS_REQUEST_PROCESSOR_MANAGER_INITIALIZER_HPP_INCLUDED__

#include "callback.hpp"
#include "ref_counted.hpp"
#include "request_processor_initializer.hpp"
#include "request_processor_manager.hpp"

namespace cass {

/**
 * An initializer for a request processor manager. This creates IO workers or
 * or request processors for handling session requests.
 */
class RequestProcessorManagerInitializer : public RefCounted<RequestProcessorManagerInitializer> {
public:
  typedef SharedRefPtr<RequestProcessorManagerInitializer> Ptr;

  typedef cass::Callback<void, RequestProcessorManagerInitializer*> Callback;

  /**
   * Constructor; uses the builder pattern to initialize the manager
   *
   * @param current_host The currently connected control connection host.
   * @param protocol_version The highest negotiated protocol for the cluster.
   * @param hosts A mapping of available hosts in the cluster.
   * @param token_map A token map.
   * @param request_queue A thread-safe queue that is used to process requests.
   * @param callback A callback that is called when the manager has completed
   *                 its connection for all request processor IO workers
   */
  RequestProcessorManagerInitializer(const Host::Ptr& connected_host,
                                     int protocol_version,
                                     const HostMap& hosts,
                                     const Callback& callback);
  ~RequestProcessorManagerInitializer();

  /**
   * Set the request processor manager settings; which contains the connection
   * pool manager settings for the request processors
   *
   * @param settings A request processor settings object
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_settings(const RequestProcessorSettings& settings);

  /**
   * Set the initial keyspace for each request process
   *
   * @param keyspace A keyspace to register after connection
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_keyspace(const String& keyspace);

  /**
   * Set the listener that handles request processor events
   *
   * @param listener A listener that handles request processor events
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_listener(RequestProcessorManagerListener* listener);

  /**
   * Set the metrics object to use to record metrics
   *
   * @param metrics A metrics object
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_metrics(Metrics* metrics);

  /**
   * Set the RNG for use randomizing hosts in load balancing policies.
   *
   * @param random A random number generator object.
   * @return The initializer to chain calls.
   */
  RequestProcessorManagerInitializer* with_random(Random* random);

  /**
   * Set the initially constructed token map (do not clone)
   *
   * @param token_map Initially calculated token map
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_token_map(const TokenMap::Ptr& token_map);

  /**
   * Initialize the request processors
   */
  void initialize(EventLoopGroup* event_loop_group);

public:
  /**
   * Critical failures that happened during the initialization process.
   *
   * @return A vector of request processors that failed
   */
  RequestProcessorInitializer::Vec failures() const;

  /**
   * Release the request_processor manager from the initializer. If not released
   * in the callback the request processor manager will automatically be closed
   *
   * @return The manager object for this initializer
   */
  RequestProcessorManager::Ptr release_manager();

private:
  void on_initialize(RequestProcessorInitializer* initializer);

private:
  RequestProcessorManager::Ptr manager_;
  RequestProcessorInitializer::Vec initializers_;

  Callback callback_;

  mutable uv_mutex_t lock_;
  RequestProcessorInitializer::Vec failures_;
  Atomic<size_t> remaining_;

  RequestProcessorSettings settings_;
  String keyspace_;
  const Host::Ptr connected_host_;
  const int protocol_version_;
  const HostMap hosts_;
  RequestProcessorManagerListener* listener_;
  Metrics* metrics_;
  Random* random_;
  TokenMap::Ptr token_map_;
};

} // namespace cass

#endif // __CASS_REQUEST_PROCESSOR_MANAGER_INITIALIZER_HPP_INCLUDED__
