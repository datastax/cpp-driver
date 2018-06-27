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

class RequestProcessorManagerListener
    : public ConnectionPoolListener
    , public RequestChangeListener {
public:
  virtual void on_close(RequestProcessorManager* manager) = 0;
};

/**
 * A manager for one or more request processor that will process requests from
 * the session
 */
class RequestProcessorManager
    : public RefCounted<RequestProcessorManager>
    , public RequestProcessorListener {
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
   * Enqueue a request to be processed on the least busy request processor.
   *
   * (thread-safe, asynchronous)).
   * @param request_handler
   */
  void process_request(const RequestHandler::Ptr& request_handler);

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


private:
  // Request processor listener methods

  void on_close(RequestProcessor* processor);
  void on_keyspace_changed(const String& keyspace);
  void on_pool_up(const Address& address);
  void on_pool_down(const Address& address);
  void on_pool_critical_error(const Address& address,
                              Connector::ConnectionError code,
                              const String& message);
  void on_prepared_metadata_changed(const String& id,
                                    const PreparedMetadata::Entry::Ptr& entry);

private:
  Atomic<size_t> current_;
  uv_mutex_t mutex_;
  int processor_count_;
  RequestProcessor::Vec processors_;
  RequestProcessorManagerListener* const listener_;
};

} // namespace cass

#endif // __CASS_REQUEST_PROCESSOR_MANAGER_HPP_INCLUDED__
