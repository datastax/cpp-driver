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
#include "histogram_wrapper.hpp"
#include "host.hpp"
#include "micro_timer.hpp"
#include "mpmc_queue.hpp"
#include "loop_watcher.hpp"
#include "prepare_host_handler.hpp"
#include "random.hpp"
#include "schema_agreement_handler.hpp"
#include "scoped_ptr.hpp"
#include "timer.hpp"
#include "token_map.hpp"

namespace cass {

class ConnectionPoolManagerInitializer;
class RequestProcessor;
class RequestProcessorManager;
class Session;

struct RequestProcessorSettings {
  RequestProcessorSettings();

  RequestProcessorSettings(const Config& config);

  ConnectionPoolManagerSettings connection_pool_manager_settings;

  unsigned max_schema_wait_time_ms;

  bool prepare_on_all_hosts;

  TimestampGenerator::Ptr timestamp_generator;

  ExecutionProfile default_profile;

  ExecutionProfile::Map profiles;

  unsigned request_queue_size;

  uint64_t coalesce_delay_us;

  int new_request_ratio;
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
   * @param manager The manager for this processor.
   * @param event_loop The event loop the request process is running on.
   * @param connection_pool_manager A manager of connection pools for handling requests.
   * @param connected_host The currently connected control connection host.
   * @param hosts A mapping of the currently available hosts.
   * @param token_map The current token map.
   * @param settings The current settings for the request processor.
   * @param random A RNG for randomizing hosts in the load balancing policies.
   */
  RequestProcessor(RequestProcessorManager* manager,
                   EventLoop* event_loop,
                   const ConnectionPoolManager::Ptr& connection_pool_manager,
                   const Host::Ptr& connected_host,
                   const HostMap& hosts,
                   const TokenMap::Ptr& token_map,
                   const RequestProcessorSettings& settings,
                   Random* random);

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
  void notify_token_map_changed(const TokenMap::Ptr& token_map);

  /**
   * Enqueue a request to be processed.
   * (thread-safe, asynchronous)).
   *
   * @param request_handler
   */
  void process_request(const RequestHandler::Ptr& request_handler);

  /**
   * Get the number of requests the processor is handling
   *
   * @return Request count
   */
  int request_count() const {
    return request_count_.load(MEMORY_ORDER_RELAXED);
  }

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
  // Connection pool manager listener methods

  virtual void on_pool_up(const Address& address);
  virtual void on_pool_down(const Address& address);
  virtual void on_pool_critical_error(const Address& address,
                                 Connector::ConnectionError code,
                                 const String& message);
  virtual void on_close(ConnectionPoolManager* manager);

private:
  // Request listener methods

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
  virtual void on_done();

private:
  // Schema agreement listener methods

  virtual bool on_is_host_up(const Address& address);

private:
  void on_timeout(MicroTimer* timer);

private:
  void internal_close();
  void internal_pool_down(const Address& address);

  Host::Ptr get_host(const Address& address);
  const ExecutionProfile* execution_profile(const String& name) const;
  const LoadBalancingPolicy::Vec& load_balancing_policies() const;

private:
  friend class RunCloseProcessor;
  friend class NotifyHostAddProcessor;
  friend class NotifyHostRemoveProcessor;
  friend class NotifyTokenMapUpdateProcessor;

private:
  void internal_host_add_down_up(const Host::Ptr& host, Host::HostState state);
  void internal_host_remove(const Host::Ptr& host);

  void on_async(Async* async);
  void on_prepare(Prepare* prepare);

  void maybe_close(int request_count);
  int process_requests(uint64_t processing_time);

private:
  ConnectionPoolManager::Ptr connection_pool_manager_;
  String connect_keyspace_;
  HostMap hosts_;
  RequestProcessorManager* const manager_;
  EventLoop* const event_loop_;
  LoadBalancingPolicy::Vec load_balancing_policies_;
  const unsigned max_schema_wait_time_ms_;
  const bool prepare_on_all_hosts_;
  const TimestampGenerator::Ptr timestamp_generator_;
  const uint64_t coalesce_delay_us_;
  const int new_request_ratio_;
  ExecutionProfile default_profile_;
  ExecutionProfile::Map profiles_;
  Atomic<int> request_count_;
  ScopedPtr<MPMCQueue<RequestHandler*> > const request_queue_;
  TokenMap::Ptr token_map_;

  bool is_closing_;
  Atomic<bool> is_processing_;
  int attempts_without_requests_;
  uint64_t io_time_during_coalesce_;
  Async async_;
  Prepare prepare_;
  MicroTimer timer_;

#ifdef CASS_INTERNAL_DIAGNOSTICS
  int reads_during_coalesce_;
  int writes_during_coalesce_;

  HistogramWrapper writes_per_;
  HistogramWrapper reads_per_;
#endif
};

} // namespace cass

#endif // __CASS_REQUEST_PROCESSOR_HPP_INCLUDED__
