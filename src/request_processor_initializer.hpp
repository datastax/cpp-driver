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

#ifndef DATASTAX_INTERNAL_REQUEST_PROCESSOR_INITIALIZER_HPP
#define DATASTAX_INTERNAL_REQUEST_PROCESSOR_INITIALIZER_HPP

#include "atomic.hpp"
#include "callback.hpp"
#include "connection_pool_manager_initializer.hpp"
#include "ref_counted.hpp"
#include "request_processor.hpp"
#include "timestamp_generator.hpp"
#include "vector.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class Config;
class EventLoop;
class RequestProcessorManager;

/**
 * A request processor initializer. This contains all the logic responsible for
 * connecting and initializing a request processor object.
 */
class RequestProcessorInitializer
    : public RefCounted<RequestProcessorInitializer>
    , public ConnectionPoolManagerListener {
public:
  typedef SharedRefPtr<RequestProcessorInitializer> Ptr;
  typedef Vector<Ptr> Vec;
  typedef internal::Callback<void, RequestProcessorInitializer*> Callback;

  enum RequestProcessorError {
    REQUEST_PROCESSOR_OK,
    REQUEST_PROCESSOR_ERROR_KEYSPACE,
    REQUEST_PROCESSOR_ERROR_NO_HOSTS_AVAILABLE,
    REQUEST_PROCESSOR_ERROR_UNABLE_TO_INIT
  };

  /**
   * Constructor.
   *
   * @param connected_host The currently connected control connection host.
   * @param protocol_version The highest negotiated protocol for the cluster.
   * @param hosts A mapping of available hosts in the cluster.
   * @param token_map A token map.
   * @param local_dc The local datacenter for initializing the load balancing policies.
   * @param callback A callback that is called when the processor is initialized
   * or if an error occurred.
   */
  RequestProcessorInitializer(const Host::Ptr& connected_host, ProtocolVersion protocol_version,
                              const HostMap& hosts, const TokenMap::Ptr& token_map,
                              const String& local_dc, const Callback& callback);
  ~RequestProcessorInitializer();

  /**
   * Initialize the request processor.
   *
   * @param event_loop The event loop to run the request processor on.
   */
  void initialize(EventLoop* event_loop);

  /**
   * Set the settings for use by the processor.
   *
   * @param settings A settings object.
   * @return The initializer to chain calls.
   */
  RequestProcessorInitializer* with_settings(const RequestProcessorSettings& setttings);

  /**
   * Set the listener to handle events for the processor.
   *
   * @param settings A processor listener.
   * @return The initializer to chain calls.
   */
  RequestProcessorInitializer* with_listener(RequestProcessorListener* listener);

  /**
   * Set the keyspace to connect with.
   *
   * @param keyspace The initial keyspace to connect with.
   * @return The initializer to chain calls.
   */
  RequestProcessorInitializer* with_keyspace(const String& keyspace);

  /**
   * Set the metrics object for recording metrics.
   *
   * @param metrics A shared metrics object.
   * @return The initializer to chain calls.
   */
  RequestProcessorInitializer* with_metrics(Metrics* metrics);

  /**
   * Set the RNG for use randomizing hosts in load balancing policies.
   *
   * @param random A random number generator object.
   * @return The initializer to chain calls.
   */
  RequestProcessorInitializer* with_random(Random* random);

  /**
   * Release the processor from the initializer. If not released in the callback
   * the processor will automatically be closed.
   *
   * @return The processor object for this initializer. This returns a null object
   * if the processor is not initialized or an error occurred.
   */
  RequestProcessor::Ptr release_processor();

public:
  RequestProcessorError error_code() const { return error_code_; }
  const String& error_message() const { return error_message_; }

  bool is_ok() const { return error_code_ == REQUEST_PROCESSOR_OK; }
  bool is_keyspace_error() const { return error_code_ == REQUEST_PROCESSOR_ERROR_KEYSPACE; }

private:
  friend class RunInitializeProcessor;

private:
  // Connection pool manager listener methods

  virtual void on_pool_up(const Address& address);
  virtual void on_pool_down(const Address& address);
  virtual void on_pool_critical_error(const Address& address, Connector::ConnectionError code,
                                      const String& message);
  virtual void on_close(ConnectionPoolManager* manager);

private:
  void internal_initialize();

private:
  void on_initialize(ConnectionPoolManagerInitializer* initializer);

private:
  uv_mutex_t mutex_;

  ConnectionPoolManagerInitializer::Ptr connection_pool_manager_initializer_;
  RequestProcessor::Ptr processor_;

  EventLoop* event_loop_;
  RequestProcessorSettings settings_;
  RequestProcessorListener* listener_;
  String keyspace_;
  Metrics* metrics_;
  Random* random_;

  const Host::Ptr connected_host_;
  const ProtocolVersion protocol_version_;
  HostMap hosts_;
  const TokenMap::Ptr token_map_;
  String local_dc_;

  RequestProcessorError error_code_;
  String error_message_;

  Callback callback_;

  Atomic<size_t> remaining_;
};

}}} // namespace datastax::internal::core

#endif
