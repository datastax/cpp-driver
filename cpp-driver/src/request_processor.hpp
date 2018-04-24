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
#include "mpmc_queue.hpp"
#include "prepare_host_handler.hpp"
#include "random.hpp"
#include "schema_agreement_handler.hpp"
#include "scoped_ptr.hpp"
#include "timer.hpp"
#include "token_map.hpp"

namespace cass {

class ConnectionPoolManagerInitializer;
class RequestProcessor;
class Session;

class RequestProcessorListener : public ConnectionPoolListener {
public:
  virtual void on_keyspace_update(const String& keyspace) = 0;
  virtual void on_prepared_metadata_update(const String& id,
                                           const PreparedMetadata::Entry::Ptr& entry) = 0;
};

struct RequestProcessorSettings {
  RequestProcessorSettings();

  RequestProcessorSettings(const Config& config);

  ConnectionPoolManagerSettings connection_pool_manager_settings;

  unsigned max_schema_wait_time_ms;

  bool prepare_on_all_hosts;

  TimestampGenerator::Ptr timestamp_generator;

  ExecutionProfile default_profile;

  ExecutionProfile::Map profiles;
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

  /**
   * Create the request processor; Don't use directly use the request processor
   * manager initializer
   *
   * @param event_loop The event loop the request process is running on.
   * @param manager A manager of connection pools for handling requests.
   * @param connected_host The currently connected control connection host.
   * @param hosts A mapping of the currently available hosts.
   * @param token_map The current token map.
   * @param listener A listener for handling processor callbacks.
   * @param settings The current settings for the request processor.
   * @param random A RNG for randomizing hosts in the load balancing policies.
   * @param request_queue A thread-safe queue for processing requests.
   */
  RequestProcessor(EventLoop* event_loop,
                   const ConnectionPoolManager::Ptr& manager,
                   const Host::Ptr& connected_host,
                   const HostMap& hosts,
                   TokenMap* token_map,
                   RequestProcessorListener* listener,
                   const RequestProcessorSettings& settings,
                   Random* random,
                   MPMCQueue<RequestHandler*>* request_queue);

  /**
   * Close/Terminate the request request processor (thread-safe).
   */
  void close();

  /**
   * Set the current keyspace being used for requests
   * (thread-safe, synchronous).
   *
   * @param keyspace New current keyspace to utilize
   */
  void set_keyspace(const String& keyspace);

  /**
   * Notify that a host has been added to the cluster
   * (thread-safe, asynchronous).
   *
   * @param host The host that's been added.
   */
  void notify_host_add(const Host::Ptr& host);

  /**
   * Notify that a host has been removed from the cluster
   * (thread-safe, asynchronous).
   *
   * @param host The host that's been removed.
   */
  void notify_host_remove(const Host::Ptr& host);

  /**
   * Notify that the token map has been updated
   * (thread-safe, asynchronous).
   *
   * @param token_map The updated token map.
   */
  void notify_token_map_update(const TokenMap* token_map);

  /**
   * Notify that a request has been added to the request queue
   * (thread-safe, asynchronous)).
   */
  void notify_request();

public:
  class Protected {
    friend class RequestProcessorInitializer;
    Protected() { }
    Protected(Protected const&) { }
  };

  /**
   * Initialize the async flushing mechanism for the request processor
   *
   * @param A key to restrict access to the method
   */
  int init(Protected);

private:
  /* Connection pool manager listener callbacks */
  virtual void on_pool_up(const Address& address);
  virtual void on_pool_down(const Address& address);
  virtual void on_pool_critical_error(const Address& address,
                                 Connector::ConnectionError code,
                                 const String& message);
  virtual void on_close(ConnectionPoolManager* manager);

private:
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

private:
  /* Schema agreement listener callback */
  virtual bool on_is_host_up(const Address& address);

private:
  void internal_connect(const Host::Ptr& current_host,
                        const HostMap& hosts,
                        Metrics* metrics,
                        int protocol_version,
                        const ConnectionPoolManagerSettings& settings);
  void internal_close();
  void internal_token_map_update(const TokenMap* token_map);
  void internal_pool_down(const Address& address);

  Host::Ptr get_host(const Address& address);
  bool execution_profile(const String& name, ExecutionProfile& profile) const;
  const LoadBalancingPolicy::Vec& load_balancing_policies() const;

private:
  friend class RunCloseProcessor;
  friend class NotifyHostAddProcessor;
  friend class NotifyHostRemoveProcessor;
  friend class NotifyTokenMapUpdateProcessor;

private:
  void internal_host_add_down_up(const Host::Ptr& host, Host::HostState state);
  void internal_host_remove(const Host::Ptr& host);

  static void on_flush(Async* async);
  static void on_flush_timer(Timer* timer);
  void internal_flush_requests();

private:
  CassError error_code_;
  String error_message_;

  ConnectionPoolManager::Ptr manager_;
  String connect_keyspace_;
  HostMap hosts_;
  EventLoop* const event_loop_;
  RequestProcessorListener* const listener_;
  LoadBalancingPolicy::Vec load_balancing_policies_;
  const unsigned max_schema_wait_time_ms_;
  const bool prepare_on_all_hosts_;
  const TimestampGenerator::Ptr timestamp_generator_;
  ExecutionProfile default_profile_;
  ExecutionProfile::Map profiles_;
  MPMCQueue<RequestHandler*>* const request_queue_;
  ScopedPtr<const TokenMap> token_map_;


  Atomic<bool> is_flushing_;
  Atomic<bool> is_closing_;
  Async async_;
  Timer timer_;
};

} // namespace cass

#endif // __CASS_REQUEST_PROCESSOR_HPP_INCLUDED__
