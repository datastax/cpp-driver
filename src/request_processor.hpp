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

#ifndef DATASTAX_INTERNAL_REQUEST_PROCESSOR_HPP
#define DATASTAX_INTERNAL_REQUEST_PROCESSOR_HPP

#include "atomic.hpp"
#include "config.hpp"
#include "connection_pool_manager.hpp"
#include "event_loop.hpp"
#include "histogram_wrapper.hpp"
#include "host.hpp"
#include "loop_watcher.hpp"
#include "micro_timer.hpp"
#include "mpmc_queue.hpp"
#include "prepare_host_handler.hpp"
#include "random.hpp"
#include "schema_agreement_handler.hpp"
#include "scoped_ptr.hpp"
#include "timer.hpp"
#include "token_map.hpp"

namespace datastax { namespace internal { namespace core {

class ConnectionPoolManagerInitializer;
class RequestProcessor;
class RequestProcessorManager;
class Session;

/**
 * A wrapper around a keyspace change response that makes sure the final
 * processing for the request happens on the original event loop. This
 * needs to be reference counted so that the last processing thread triggers
 * setting the response on the request's future.
 */
class KeyspaceChangedHandler : public RefCounted<KeyspaceChangedHandler> {
public:
  typedef SharedRefPtr<KeyspaceChangedHandler> Ptr;

  KeyspaceChangedHandler(EventLoop* event_loop, const KeyspaceChangedResponse& response)
      : event_loop_(event_loop)
      , response_(response) {}

  ~KeyspaceChangedHandler() { event_loop_->add(new Task(response_)); }

private:
  /**
   * An internal task that keeps the original keyspace change handler
   * alive so that processing happens on the original event loop.
   */
  class Task : public core::Task {
  public:
    Task(const KeyspaceChangedResponse& response)
        : response_(response) {}
    virtual void run(EventLoop* event_loop) { response_.set_response(); }

  private:
    KeyspaceChangedResponse response_;
  };

private:
  EventLoop* const event_loop_;
  KeyspaceChangedResponse response_;
};

class KeyspaceChangedListener {
public:
  virtual ~KeyspaceChangedListener() {}

  virtual void on_keyspace_changed(const String& keyspace,
                                   const KeyspaceChangedHandler::Ptr& handler) = 0;
};

class RequestProcessorListener
    : public ConnectionPoolStateListener
    , public PreparedMetadataListener
    , public KeyspaceChangedListener {
public:
  /**
   * A callback that's called when the processor connects.
   *
   * Note: This is mostly for testing.
   *
   * @param processor The processor object.
   */
  virtual void on_connect(RequestProcessor* processor) {}

  /**
   * A callback that's called when the processor has closed.
   *
   * @param processor The processor object.
   */
  virtual void on_close(RequestProcessor* processor) = 0;
};

struct RequestProcessorSettings {
  RequestProcessorSettings();

  RequestProcessorSettings(const Config& config);

  ConnectionPoolSettings connection_pool_settings;

  unsigned max_schema_wait_time_ms;

  bool prepare_on_all_hosts;

  TimestampGenerator::Ptr timestamp_generator;

  ExecutionProfile default_profile;

  ExecutionProfile::Map profiles;

  unsigned request_queue_size;

  uint64_t coalesce_delay_us;

  int new_request_ratio;

  uint64_t max_tracing_wait_time_ms;

  uint64_t retry_tracing_wait_time_ms;

  CassConsistency tracing_consistency;

  AddressFactory::Ptr address_factory;
};

/**
 * Request processor for processing client session request(s). This processor
 * will fetch a request from the queue and process them accordingly by applying
 * the load balancing policy, executing and routing the request to the
 * appropriate node and perform the callback to the client.
 */
class RequestProcessor
    : public RefCounted<RequestProcessor>
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
   * @param listener A listener that handles the events for the processor.
   * @param event_loop The event loop the request process is running on.
   * @param connection_pool_manager A manager of connection pools for handling requests.
   * @param connected_host The currently connected control connection host.
   * @param hosts A mapping of the currently available hosts.
   * @param token_map The current token map.
   * @param settings The current settings for the request processor.
   * @param random A RNG for randomizing hosts in the load balancing policies.
   * @param local_dc The local datacenter for initializing the load balancing policies.
   */
  RequestProcessor(RequestProcessorListener* listener, EventLoop* event_loop,
                   const ConnectionPoolManager::Ptr& connection_pool_manager,
                   const Host::Ptr& connected_host, const HostMap& hosts,
                   const TokenMap::Ptr& token_map, const RequestProcessorSettings& settings,
                   Random* random, const String& local_dc);

  /**
   * Close/Terminate the request request processor (thread-safe).
   */
  void close();

  /**
   * Set the listener that will handle events for the processor
   * (*NOT* thread-safe).
   *
   * @param listener The processor listener.
   */
  void set_listener(RequestProcessorListener* listener = NULL);

  /**
   * Set the current keyspace being used for requests
   * (thread-safe, asynchronous).
   *
   * @param keyspace New current keyspace to utilize
   * @param handler A keyspace handler to trigger a response to a "USE" query.
   */
  void set_keyspace(const String& keyspace, const KeyspaceChangedHandler::Ptr& handler);

  /**
   * Notify that a host has been added to the cluster
   * (thread-safe, asynchronous).
   *
   * @param host The host that's been added.
   */
  void notify_host_added(const Host::Ptr& host);

  /**
   * Notify that a host has been removed from the cluster
   * (thread-safe, asynchronous).
   *
   * @param host The host that's been removed.
   */
  void notify_host_removed(const Host::Ptr& host);

  /**
   * Notify that a host is now available to handle queries. This has no effect
   * if the host has no connections available.
   *
   * (thread-safe, asynchronous).
   *
   * @param host A host that's ready to be marked up.
   */
  void notify_host_ready(const Host::Ptr& host);

  /**
   * Notify that a host might be available. This expedites the reconnection
   * process for the provided host.
   *
   * (thread-safe, asynchronous).
   *
   * @param address An address of the host might be UP.
   */
  void notify_host_maybe_up(const Address& address);

  /**
   * Notify that the token map has been updated
   * (thread-safe, asynchronous).
   *
   * @param token_map The updated token map.
   */
  void notify_token_map_updated(const TokenMap::Ptr& token_map);

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
  int request_count() const { return request_count_.load(MEMORY_ORDER_RELAXED); }

public:
  class Protected {
    friend class RequestProcessorInitializer;
    Protected() {}
    Protected(Protected const&) {}
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
  virtual void on_pool_critical_error(const Address& address, Connector::ConnectionError code,
                                      const String& message);
  virtual void on_requires_flush();
  virtual void on_close(ConnectionPoolManager* manager);

private:
  // Request listener methods

  virtual void on_prepared_metadata_changed(const String& id,
                                            const PreparedMetadata::Entry::Ptr& entry);
  virtual void on_keyspace_changed(const String& keyspace, KeyspaceChangedResponse handler);
  virtual bool on_wait_for_tracing_data(const RequestHandler::Ptr& request_handler,
                                        const Host::Ptr& current_host,
                                        const Response::Ptr& response);
  virtual bool on_wait_for_schema_agreement(const RequestHandler::Ptr& request_handler,
                                            const Host::Ptr& current_host,
                                            const Response::Ptr& response);
  virtual bool on_prepare_all(const RequestHandler::Ptr& request_handler,
                              const Host::Ptr& current_host, const Response::Ptr& response);
  virtual void on_done();

private:
  // Schema agreement listener methods

  virtual bool on_is_host_up(const Address& address);

private:
  void on_timeout(MicroTimer* timer);

private:
  void internal_close();
  void internal_pool_down(const Address& address);

  const ExecutionProfile* execution_profile(const String& name) const;
  const LoadBalancingPolicy::Vec& load_balancing_policies() const;

private:
  friend class ProcessorRunClose;
  friend class ProcessorNotifyHostAdd;
  friend class ProcessorNotifyHostRemove;
  friend class ProcessorNotifyHostReady;
  friend class ProcessorNotifyMaybeHostUp;
  friend class ProcessorNotifyTokenMapUpdate;

private:
  void internal_host_add(const Host::Ptr& host);
  void internal_host_remove(const Host::Ptr& host);
  void internal_host_ready(const Host::Ptr& host);
  void internal_host_maybe_up(const Address& address);

  void start_coalescing();
  void on_async(Async* async);
  void on_prepare(Prepare* prepare);

  void maybe_close(int request_count);
  int process_requests(uint64_t processing_time);

  bool write_wait_callback(const RequestHandler::Ptr& request_handler,
                           const Host::Ptr& current_host, const RequestCallback::Ptr& callback);

private:
  ConnectionPoolManager::Ptr connection_pool_manager_;
  String connect_keyspace_;
  RequestProcessorListener* listener_;
  EventLoop* const event_loop_;
  LoadBalancingPolicy::Vec load_balancing_policies_;
  const RequestProcessorSettings settings_;
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

}}} // namespace datastax::internal::core

#endif // DATASTAX_INTERNAL_REQUEST_PROCESSOR_HPP
