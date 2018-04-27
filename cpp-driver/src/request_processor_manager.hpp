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

#ifndef __CASS_REQUEST_PROCESSOR_MANAGER_HPP_INCLUDED__
#define __CASS_REQUEST_PROCESSOR_MANAGER_HPP_INCLUDED__

#include "event_loop.hpp"
#include "host.hpp"
#include "ref_counted.hpp"
#include "request_processor.hpp"

namespace cass {

class RequestProcessorManager;

class RequestProcessorManagerListener : public ConnectionPoolListener {
public:
  virtual void on_prepared_metadata_changed(const String& id,
                                           const PreparedMetadata::Entry::Ptr& entry) = 0;
  virtual void on_close(RequestProcessorManager* manager) = 0;
};


/**
 * A manager for one or more request processor that will process requests from
 * the session
 */
class RequestProcessorManager : public RefCounted<RequestProcessorManager> {
public:
  typedef SharedRefPtr<RequestProcessorManager> Ptr;

  /**
   * Constructor; Don't use directly use the initializer
   *
   * Handles initialization and connections for each request processor to the
   * pre-established hosts from the control connection
   *
   * @param listener A listener for handling processor events.
   */
  RequestProcessorManager(RequestProcessorManagerListener* listener);

  ~RequestProcessorManager();

  /**
   * Close/Terminate the request processors (thread-safe).
   *
   * @param A key to restrict access to the method
   */
  void close();

  /**
   * Add a new host to the request processors
   * (thread-safe, asynchronous).
   *
   * @param host New host to be added
   */
  void notify_host_add(const Host::Ptr& host);

  /**
   * Remove a host from the request processors
   * (thread-safe, asynchronous).
   *
   * @param host Host to be removed
   */
  void notify_host_remove(const Host::Ptr& host);

  /**
   * Update the token map being used for the requests
   * (thread-safe, asynchronous).
   *
   * @param token_map Update token map (do not clone)
   */
  void notify_token_map_changed(const TokenMap::Ptr& token_map);

  /**
   * Notify one of the request processors that a new request is available
   * (thread-safe, asynchronous).
   *
   * NOTE: The request processor selected during the round robin process may or
   *       may not be notified if it is currently flushing requests from the
   *       queue.
   */
  void notify_request();

public:
  class Protected {
    friend class RequestProcessor;
    friend class RequestProcessorManagerInitializer;
    Protected() { }
    Protected(Protected const&) { }
  };

  /**
   * Add request processor to the manager.
   *
   * @param request_processor A request processor to add to the manager.
   * @param A key to restrict access to the method.
   */
  void add_processor(const RequestProcessor::Ptr& processor, Protected);

  /**
   * Notify that a processor closed.
   *
   * @param processor The processor that closed.
   * @param A key to restrict access to the method.
   */
  void notify_closed(RequestProcessor* processor, Protected);

  /**
   * Notify that the keyspace has changed.
   *
   * @param keyspace The new keyspace.
   * @param A key to restrict access to the method.
   */
  void notify_keyspace_changed(const String& keyspace, Protected);

  /**
   * Notify that a connection pool now has available connections.
   *
   * @param address The address of the host that's available.
   * @param A key to restrict access to the method.
   */
  void notify_pool_up(const Address& address, Protected);

  /**
   * Notify that a connection pool no longer has available connections.
   *
   * @param address The address of the host that's not available.
   * @param A key to restrict access to the method.
   */
  void notify_pool_down(const Address& address, Protected);

  /**
   * Notify that a connection pool has encounter a critical error attempting
   * to reconnect.
   *
   * @param address The address of the host with critical error.
   * @param code The error code.
   * @param message The error message.
   * @param A key to restrict access to the method.
   */
  void notify_pool_critical_error(const Address& address,
                                  Connector::ConnectionError code,
                                  const String& message,
                                  Protected);

  /**
   * Notify that the prepared result metadata has changes.
   *
   * @param id The id of the prepared metadata.
   * @param entry The associated prepared metadata.
   * @param A key to restrict access to the method.
   */
  void notify_prepared_metadata_changed(const String& id,
                                        const PreparedMetadata::Entry::Ptr& entry,
                                        Protected);

private:
  Atomic<size_t> current_;
  uv_mutex_t mutex_;
  RequestProcessor::Vec processors_;
  RequestProcessorManagerListener* const listener_;
};

} // namespace cass

#endif // __CASS_REQUEST_PROCESSOR_MANAGER_HPP_INCLUDED__
