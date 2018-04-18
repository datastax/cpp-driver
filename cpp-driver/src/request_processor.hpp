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

#ifndef __CASS_REQUEST_PROCESSOR_HPP_INCLUDED__
#define __CASS_REQUEST_PROCESSOR_HPP_INCLUDED__

#include "atomic.hpp"
#include "config.hpp"
#include "connection_pool_manager.hpp"
#include "event_loop.hpp"
#include "host.hpp"
#include "prepare_host_handler.hpp"
#include "random.hpp"
#include "schema_agreement_handler.hpp"
#include "scoped_ptr.hpp"
#include "timer.hpp"
#include "token_map.hpp"

namespace cass {

class ConnectionPoolManagerInitializer;
class Session;

class RequestProcessorListener {
public:
  virtual void on_keyspace_update(const String& keyspace) = 0;
  virtual void on_prepared_metadata_update(const String& id,
                                           const PreparedMetadata::Entry::Ptr& entry) = 0;
};

/**
 * Request processor for processing client session request(s). This processor
 * will fetch a request from the queue and process them accordingly by applying
 * the load balancing policy, executing and routing the request to the
 * appropriate node and perform the callback to the client.
 */
class RequestProcessor : public RefCounted<RequestProcessor>
                       , public ConnectionPoolManagerListener
                       , public RequestListener
                       , public SchemaAgreementListener {
public:
  typedef SharedRefPtr<RequestProcessor> Ptr;
  typedef Vector<Ptr> Vec;
  typedef void(*Callback)(RequestProcessor*); // Initialization callback

  /**
   * Create the request processor; Don't use directly use the request processor
   * manager initializer
   *
   * @param event_loop Event loop to utilize for handling requests
   * @param connect_keyspace Keyspace session is connecting to; may be empty
   * @param listener Request processor listener for connection events
   * @param max_schema_wait_time_ms Maximum schema agreement wait time (in
   *                                milliseconds)
   * @param prepare_on_all_hosts True if statements should be prepared on all
   *                             hosts; false otherwise
   * @param request_queue Request queue associated with the session
   * @param timestamp_generator Session timestamp generator
   * @param data User data that's passed to the callback
   * @param callback A callback that is called when the connection is connected
   *                 or if an error occurred
   */
  RequestProcessor(EventLoop* event_loop,
                   const String& connect_keyspace,
                   RequestProcessorListener* listener,
                   unsigned max_schema_wait_time_ms,
                   bool prepare_on_all_hosts,
                   MPMCQueue<RequestHandler*>* request_queue,
                   TimestampGenerator* timestamp_generator,
                   void* data,
                   Callback callback);

  /**
   * Close/Terminate the request request processor
   */
  void close();
  /**
   * Close the request processor handles
   */
  void close_handles();
  /**
   * Update the current keyspace being used for requests (thread-safe)
   *
   * @param keyspace New current keyspace to utilize
   */
  void keyspace_update(const String& keyspace);

  /* Notifications to be performed by the request processor */
  void notify_host_add_async(const Host::Ptr& host);
  void notify_host_remove_async(const Host::Ptr& host);
  void notify_token_map_update_async(const TokenMap* token_map);
  void notify_request_async();

public:
  class Protected {
    friend class RequestProcessorManagerInitializer;
    Protected() { }
    Protected(Protected const&) { }
  };

  /**
   * Initialize the request processor
   *
   * @param default_profile Default execution profile
   * @param profiles Execution profiles (excludes default profile)
   * @param token_map Initial calculated token map
   * @param use_randomized_contact_points True if randomized contract points
   *                                      should be used; false otherwise
   * @param A key to restrict access to the method
   */
  void init(const ExecutionProfile& default_profile,
            const ExecutionProfile::Map& profiles,
            const TokenMap* token_map,
            bool use_randomized_contact_points,
            Protected);
  /**
   * Connect the request processor to the pre-established hosts using the
   * given protocol version
   *
   * @param connected_host Current connected host
   * @param hosts Map of addresses with their associated hosts
   * @param metrics Metrics associated with the session
   * @param protocol_version Protocol version established
   * @param settings A manager settings object for the connection pool manager
   * @param A key to restrict access to the method
   */
  void connect(const Host::Ptr& connected_host,
               const HostMap& hosts,
               Metrics* metrics,
               int protocol_version,
               const ConnectionPoolManagerSettings& settings,
               Protected);
  /**
   * Initialize the async flushing mechanism for the request processor
   *
   * @param A key to restrict access to the method
   */
  void start_async(Protected);

public:
  /* Connection pool manager listener callbacks */
  virtual void on_up(const Address& address);
  virtual void on_down(const Address& address);
  virtual void on_critical_error(const Address& address,
                                 Connector::ConnectionError code,
                                 const String& message);

  /* Request listener callbacks */
  virtual void on_result_metadata_changed(const String& prepared_id,
                                          const String& query,
                                          const String& keyspace,
                                          const String& result_metadata_id,
                                          const ResultResponse::ConstPtr& result_response);
  virtual void on_keyspace_changed(const String& keyspace);
  virtual bool on_wait_for_schema_agreement(const RequestHandler::Ptr& request_handler,
                                            const Host::Ptr& current_host,
                                            const Response::Ptr& response);
  virtual bool on_prepare_all(const RequestHandler::Ptr& request_handler,
                              const Host::Ptr& current_host,
                              const Response::Ptr& response);

  /* Schema agreement listener callback */
  virtual bool on_is_host_up(const Address& address);

public:
  /**
   * Retrieve the user data
   *
   * @return User data
   */
  bool is_ok();
  /**
   * Get the error code associated with the initialization of the request
   * processor
   *
   * @return Error code
   */
  CassError error_code() const;
  /**
   * Get the error message associated with the initialization of the request
   * processor
   *
   * @return Error message; may be empty
   */
  String error_message() const;
  /**
   * Determine if the request processor was successfully initialized
   *
   * @return True request processor was successfully initialized; false
   *         otherwise
   */
  void* data();

private:
  void internal_connect(const Host::Ptr& current_host,
                        const HostMap& hosts,
                        Metrics* metrics,
                        int protocol_version,
                        const ConnectionPoolManagerSettings& settings);
  void internal_close();
  void internal_token_map_update(const TokenMap* token_map);

  Host::Ptr get_host(const Address& address);
  bool execution_profile(const String& name, ExecutionProfile& profile) const;
  const LoadBalancingPolicy::Vec& load_balancing_policies() const;

private:
  /**
   * Task to easily add the request processor executing a task on its event
   * loop
   */
  class RequestProcessorTask : public Task {
  public:
    RequestProcessorTask(const RequestProcessor::Ptr& request_processor)
      : request_processor_(request_processor) { }
  protected:
    RequestProcessor::Ptr request_processor_;
  };

  // Control Connection/Session tasks for async operations
  class NotifyHostAdd : public RequestProcessorTask {
  public:
    NotifyHostAdd(const Host::Ptr host,
                  const RequestProcessor::Ptr& request_processor)
      : host_(host)
      , RequestProcessorTask(request_processor) { }
    virtual void run(EventLoop* event_loop);
  private:
    const Host::Ptr host_;
  };

  class NotifyHostRemove : public RequestProcessorTask {
  public:
    NotifyHostRemove(const Host::Ptr host,
                     const RequestProcessor::Ptr& request_processor)
      : host_(host)
      , RequestProcessorTask(request_processor) { }
    virtual void run(EventLoop* event_loop);
  private:
    const Host::Ptr host_;
  };

  class NotifyTokenMapUpdate : public RequestProcessorTask {
  public:
    NotifyTokenMapUpdate(const TokenMap* token_map,
                         const RequestProcessor::Ptr& request_processor)
      : token_map_(token_map)
      , RequestProcessorTask(request_processor) { }
    virtual void run(EventLoop* event_loop);
  private:
    const TokenMap* token_map_;
  };

  static void on_connection_pool_manager_initialize(ConnectionPoolManagerInitializer* initializer);
  void internal_connection_pool_manager_initialize(const ConnectionPoolManager::Ptr& manager,
                                                   const ConnectionPoolConnector::Vec& failures);

  void internal_host_add_down_up(const Host::Ptr& host, Host::HostState state);
  void internal_host_remove(const Host::Ptr& host);

  static void on_flush(Async* async);
  static void on_flush_timer(Timer* timer);
  void internal_flush_requests();

private:
  void* data_;
  Callback callback_;

  CassError error_code_;
  String error_message_;

  String connect_keyspace_;
  ExecutionProfile default_profile_;
  HostMap hosts_;
  RequestProcessorListener* listener_;
  LoadBalancingPolicy::Vec load_balancing_policies_;
  unsigned max_schema_wait_time_ms_;
  bool prepare_on_all_hosts_;
  ExecutionProfile::Map profiles_;
  ScopedPtr<Random> random_;
  MPMCQueue<RequestHandler*>* request_queue_;
  TimestampGenerator* timestamp_generator_;
  ScopedPtr<const TokenMap> token_map_;

  ConnectionPoolManager::Ptr manager_;

  EventLoop* event_loop_;
  Atomic<bool> is_flushing_;
  Atomic<bool> is_closing_;
  Async async_;
  Timer timer_;
};

} // namespace cass

#endif // __CASS_REQUEST_PROCESSOR_HPP_INCLUDED__
