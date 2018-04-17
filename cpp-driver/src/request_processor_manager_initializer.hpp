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

#include "ref_counted.hpp"
#include "request_processor_manager.hpp"

namespace cass {

/**
 * An initializer for a request processor manager. This creates IO workers or
 * or request processors for handling session requests.
 */
class RequestProcessorManagerInitializer : public RefCounted<RequestProcessorManagerInitializer> {
public:
  typedef SharedRefPtr<RequestProcessorManagerInitializer> Ptr;
  typedef void (*Callback)(RequestProcessorManagerInitializer*);

  /**
   * Constructor; uses the builder pattern to initialize the manger
   *
   * @param data User data that's passed to the callback
   * @param callback A callback that is called when the manager has completed
   *                 its connection for all request processor IO workers
   */
  RequestProcessorManagerInitializer(void* data, Callback callback);
  ~RequestProcessorManagerInitializer();

  /**
   * Set the request processor manager settings; which contains the connection
   * pool manager settings for the request processors
   *
   * @param settings A manager settings object
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_settings(const RequestProcessorManagerSettings& settings);
  /**
   * Set the initial keyspace for each request process
   *
   * @param keyspace A keyspace to register after connection
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_connect_keyspace(const String& connect_keyspace);
  /**
   * Set the default execution profile
   *
   * @param default_profile Default execution profile
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_default_profile(const ExecutionProfile& default_profile);
  /**
   * Set the initial list of hosts to use for establishing connections
   *
   * @param connected_host Current control connected host
   * @param hosts Map of addresses with their associated hosts
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_hosts(const Host::Ptr& connected_host,
                                                 const HostMap& hosts);
  /**
   * Set the listener that handles request processor events
   *
   * @param listener A listener that handles request processor events
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_listener(RequestProcessorListener* listener);
  /**
   * Set the max schema agreement wait time (in milliseconds)
   *
   * @param max_schema_wait_time_ms Maximum wait time for schema agreement
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_max_schema_wait_time_ms(unsigned max_schema_wait_time_ms);
  /**
   * Set the metrics object to use to record metrics
   *
   * @param metrics A metrics object
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_metrics(Metrics* metrics);
  /**
   * Set the prepare statements on all available hosts setting
   *
   * @param prepare_on_all_hosts True if statements should be prepared on all
   *                             hosts; false otherwise
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_prepare_statements_on_all(bool prepare_on_all_hosts);
  /**
   * Set the execution profiles (excludes default profile)
   *
   * @param profiles Execution profiles
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_profiles(const ExecutionProfile::Map& profiles);
  /**
   * Set the protocol version to use for the request processor connections
   *
   * @param protocol_version Protocol version established
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_protocol_version(int protocol_version);
  /**
   * Set the use of randomized contact points
   *
   * @param use_randomized_contact_points True if randomized contract points
   *                                      should be used; false otherwise
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_randomized_contact_points(bool use_randomized_contact_points);
  /**
   * Set session request queue pointer for processing requests
   *
   * @param request_queue Request queue
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_request_queue(MPMCQueue<RequestHandler*>* request_queue);
  /**
   * Set the timestamp generator
   *
   * @param timestamp_generator Time stamp generator to utilize
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_timestamp_generator(TimestampGenerator* timestamp_generator);
  /**
   * Set the initially constructed token map (do not clone)
   *
   * @param token_map Initially calculated token map
   * @return The initializer to chain calls
   */
  RequestProcessorManagerInitializer* with_token_map(TokenMap* token_map);

  /**
   * Initialize the request processors
   */
  void initialize();

public:
  /**
   * Get the user data passed in for the callback
   *
   * @return User data
   */
  void* data();
  /**
   * Critical failures that happened during the initialization process.
   *
   * @return A vector of request processors that failed
   */
  RequestProcessor::Vec failures() const;
  /**
   * Release the request_processor manager from the initializer. If not released
   * in the callback the request processor manager will automatically be closed
   *
   * @return The manager object for this initializer
   */
  RequestProcessorManager::Ptr release_manager();

private:
  static void on_connect(RequestProcessor* request_processor);
  void handle_connect(RequestProcessor* request_processor);

private:
  RequestProcessorManager::Ptr manager_;

  void* data_;
  Callback callback_;

  mutable uv_mutex_t lock_;
  RequestProcessor::Vec failures_;
  Atomic<size_t> remaining_;

  RequestProcessorManagerSettings settings_;
  String connect_keyspace_;
  Host::Ptr connected_host_;
  ExecutionProfile default_profile_;
  HostMap hosts_;
  RequestProcessorListener* listener_;
  unsigned max_schema_wait_time_ms_;
  Metrics* metrics_;
  bool prepare_on_all_hosts_;
  ExecutionProfile::Map profiles_;
  int protocol_version_;
  MPMCQueue<RequestHandler*>* request_queue_;
  TimestampGenerator* timestamp_generator_;
  TokenMap* token_map_;
  bool use_randomized_contact_points_;
};

} // namespace cass

#endif // __CASS_REQUEST_PROCESSOR_MANAGER_INITIALIZER_HPP_INCLUDED__
