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

#include "request_processor.hpp"

#include "connection_pool_manager_initializer.hpp"
#include "prepare_all_handler.hpp"
#include "request_processor.hpp"
#include "session.hpp"
#include "tracing_data_handler.hpp"
#include "utils.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

namespace datastax { namespace internal { namespace core {

class ProcessorRunClose : public Task {
public:
  ProcessorRunClose(const RequestProcessor::Ptr& processor)
      : processor_(processor) {}

  virtual void run(EventLoop* event_loop) { processor_->internal_close(); }

private:
  const RequestProcessor::Ptr processor_;
};

class ProcessorNotifyHostAdd : public Task {
public:
  ProcessorNotifyHostAdd(const Host::Ptr host, const RequestProcessor::Ptr& request_processor)
      : request_processor_(request_processor)
      , host_(host) {}
  virtual void run(EventLoop* event_loop) { request_processor_->internal_host_add(host_); }

private:
  const RequestProcessor::Ptr request_processor_;
  const Host::Ptr host_;
};

class ProcessorNotifyHostRemove : public Task {
public:
  ProcessorNotifyHostRemove(const Host::Ptr host, const RequestProcessor::Ptr& request_processor)
      : request_processor_(request_processor)
      , host_(host) {}
  virtual void run(EventLoop* event_loop) { request_processor_->internal_host_remove(host_); }

private:
  RequestProcessor::Ptr request_processor_;
  const Host::Ptr host_;
};

class ProcessorNotifyHostReady : public Task {
public:
  ProcessorNotifyHostReady(const Host::Ptr& host, const RequestProcessor::Ptr& request_processor)
      : request_processor_(request_processor)
      , host_(host) {}
  virtual void run(EventLoop* event_loop) { request_processor_->internal_host_ready(host_); }

private:
  const RequestProcessor::Ptr request_processor_;
  const Host::Ptr host_;
};

class ProcessorNotifyMaybeHostUp : public Task {
public:
  ProcessorNotifyMaybeHostUp(const Address& address, const RequestProcessor::Ptr& request_processor)
      : request_processor_(request_processor)
      , address_(address) {}
  virtual void run(EventLoop* event_loop) { request_processor_->internal_host_maybe_up(address_); }

private:
  const RequestProcessor::Ptr request_processor_;
  const Address address_;
};

class ProcessorNotifyTokenMapUpdate : public Task {
public:
  ProcessorNotifyTokenMapUpdate(const TokenMap::Ptr& token_map,
                                const RequestProcessor::Ptr& request_processor)
      : request_processor_(request_processor)
      , token_map_(token_map) {}

  virtual void run(EventLoop* event_loop) { request_processor_->token_map_ = token_map_; }

private:
  const RequestProcessor::Ptr request_processor_;
  const TokenMap::Ptr token_map_;
};

class SetKeyspaceProcessor : public Task {
public:
  SetKeyspaceProcessor(const ConnectionPoolManager::Ptr& manager, const String& keyspace,
                       const KeyspaceChangedHandler::Ptr& handler)
      : manager_(manager)
      , keyspace_(keyspace)
      , handler_(handler) {}

  virtual void run(EventLoop* event_loop) { manager_->set_keyspace(keyspace_); }

private:
  ConnectionPoolManager::Ptr manager_;
  const String keyspace_;
  KeyspaceChangedHandler::Ptr handler_;
};

class NopRequestProcessorListener : public RequestProcessorListener {
public:
  virtual void on_pool_up(const Address& address) {}
  virtual void on_pool_down(const Address& address) {}
  virtual void on_pool_critical_error(const Address& address, Connector::ConnectionError code,
                                      const String& message) {}
  virtual void on_keyspace_changed(const String& keyspace,
                                   const KeyspaceChangedHandler::Ptr& handler) {}
  virtual void on_prepared_metadata_changed(const String& id,
                                            const PreparedMetadata::Entry::Ptr& entry) {}
  virtual void on_close(RequestProcessor* processor) {}
};

}}} // namespace datastax::internal::core

static NopRequestProcessorListener nop_request_processor_listener__;

RequestProcessorSettings::RequestProcessorSettings()
    : max_schema_wait_time_ms(10000)
    , prepare_on_all_hosts(true)
    , timestamp_generator(new ServerSideTimestampGenerator())
    , default_profile(Config().default_profile())
    , request_queue_size(8192)
    , coalesce_delay_us(CASS_DEFAULT_COALESCE_DELAY)
    , new_request_ratio(CASS_DEFAULT_NEW_REQUEST_RATIO)
    , max_tracing_wait_time_ms(CASS_DEFAULT_MAX_TRACING_DATA_WAIT_TIME_MS)
    , retry_tracing_wait_time_ms(CASS_DEFAULT_RETRY_TRACING_DATA_WAIT_TIME_MS)
    , tracing_consistency(CASS_DEFAULT_TRACING_CONSISTENCY)
    , address_factory(new AddressFactory()) {
  profiles.set_empty_key("");
}

RequestProcessorSettings::RequestProcessorSettings(const Config& config)
    : connection_pool_settings(config)
    , max_schema_wait_time_ms(config.max_schema_wait_time_ms())
    , prepare_on_all_hosts(config.prepare_on_all_hosts())
    , timestamp_generator(config.timestamp_gen())
    , default_profile(config.default_profile())
    , profiles(config.profiles())
    , request_queue_size(config.queue_size_io())
    , coalesce_delay_us(config.coalesce_delay_us())
    , new_request_ratio(config.new_request_ratio())
    , max_tracing_wait_time_ms(config.max_tracing_wait_time_ms())
    , retry_tracing_wait_time_ms(config.retry_tracing_wait_time_ms())
    , tracing_consistency(config.tracing_consistency())
    , address_factory(create_address_factory_from_config(config)) {}

RequestProcessor::RequestProcessor(RequestProcessorListener* listener, EventLoop* event_loop,
                                   const ConnectionPoolManager::Ptr& connection_pool_manager,
                                   const Host::Ptr& connected_host, const HostMap& hosts,
                                   const TokenMap::Ptr& token_map,
                                   const RequestProcessorSettings& settings, Random* random,
                                   const String& local_dc)
    : connection_pool_manager_(connection_pool_manager)
    , listener_(listener ? listener : &nop_request_processor_listener__)
    , event_loop_(event_loop)
    , settings_(settings)
    , default_profile_(settings.default_profile)
    , profiles_(settings.profiles)
    , request_count_(0)
    , request_queue_(new MPMCQueue<RequestHandler*>(settings.request_queue_size))
    , is_closing_(false)
    , is_processing_(false)
    , attempts_without_requests_(0)
    , io_time_during_coalesce_(0)
#ifdef CASS_INTERNAL_DIAGNOSTICS
    , reads_during_coalesce_(0)
    , writes_during_coalesce_(0)
    , writes_per_("writes")
    , reads_per_("reads")
#endif
{
  inc_ref(); // For the connection pool manager
  connection_pool_manager_->set_listener(this);

  // Build/Assign the load balancing policies from the execution profiles
  default_profile_.build_load_balancing_policy();
  load_balancing_policies_.push_back(default_profile_.load_balancing_policy());
  for (ExecutionProfile::Map::iterator it = profiles_.begin(), end = profiles_.end(); it != end;
       ++it) {
    it->second.build_load_balancing_policy();
    const LoadBalancingPolicy::Ptr& load_balancing_policy = it->second.load_balancing_policy();
    if (load_balancing_policy) {
      LOG_TRACE("Built load balancing policy for '%s' execution profile", it->first.c_str());
      load_balancing_policies_.push_back(load_balancing_policy);
    } else {
      it->second.use_load_balancing_policy(default_profile_.load_balancing_policy());
    }
  }

  token_map_ = token_map;

  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(); it != policies.end(); ++it) {
    // Initialize the load balancing policies
    (*it)->init(connected_host, hosts, random, local_dc);
    (*it)->register_handles(event_loop_->loop());
  }

  listener_->on_connect(this);
}

void RequestProcessor::close() { event_loop_->add(new ProcessorRunClose(Ptr(this))); }

void RequestProcessor::set_listener(RequestProcessorListener* listener) {
  listener_ = listener ? listener : &nop_request_processor_listener__;
}

void RequestProcessor::set_keyspace(const String& keyspace,
                                    const KeyspaceChangedHandler::Ptr& handler) {
  // If running on the the current event loop then just set the keyspace,
  // otherwise we're on a different thread so we'll wait for the keyspace
  // to be set in a task on the event loop thread.
  if (event_loop_->is_running_on()) {
    connection_pool_manager_->set_keyspace(keyspace);
  } else {
    event_loop_->add(new SetKeyspaceProcessor(connection_pool_manager_, keyspace, handler));
  }
}

void RequestProcessor::notify_host_added(const Host::Ptr& host) {
  event_loop_->add(new ProcessorNotifyHostAdd(host, Ptr(this)));
}

void RequestProcessor::notify_host_removed(const Host::Ptr& host) {
  event_loop_->add(new ProcessorNotifyHostRemove(host, Ptr(this)));
}

void RequestProcessor::notify_host_ready(const Host::Ptr& host) {
  event_loop_->add(new ProcessorNotifyHostReady(host, Ptr(this)));
}

void RequestProcessor::notify_host_maybe_up(const Address& address) {
  event_loop_->add(new ProcessorNotifyMaybeHostUp(address, Ptr(this)));
}

void RequestProcessor::notify_token_map_updated(const TokenMap::Ptr& token_map) {
  event_loop_->add(new ProcessorNotifyTokenMapUpdate(token_map, Ptr(this)));
}

void RequestProcessor::process_request(const RequestHandler::Ptr& request_handler) {
  request_handler->inc_ref(); // Queue reference

  if (request_queue_->enqueue(request_handler.get())) {
    request_count_.fetch_add(1);
    // Only signal the request queue if it's not already processing requests.
    bool expected = false;
    if (!is_processing_.load(MEMORY_ORDER_RELAXED) &&
        is_processing_.compare_exchange_strong(expected, true)) {
      async_.send();
    }
  } else {
    request_handler->dec_ref();
    request_handler->set_error(CASS_ERROR_LIB_REQUEST_QUEUE_FULL,
                               "The request queue has reached capacity");
  }
}

int RequestProcessor::init(Protected) {
  int rc = async_.start(event_loop_->loop(), bind_callback(&RequestProcessor::on_async, this));
  if (rc != 0) return rc;
  return prepare_.start(event_loop_->loop(), bind_callback(&RequestProcessor::on_prepare, this));
}

void RequestProcessor::on_pool_up(const Address& address) {
  // Don't immediately update the load balancing policies. Give the listener
  // a chance to process the up status and it should call `notify_host_ready()`
  // when it's ready.
  listener_->on_pool_up(address);
}

void RequestProcessor::on_pool_down(const Address& address) {
  internal_pool_down(address);
  listener_->on_pool_down(address);
}

void RequestProcessor::on_pool_critical_error(const Address& address,
                                              Connector::ConnectionError code,
                                              const String& message) {
  internal_pool_down(address);
  listener_->on_pool_critical_error(address, code, message);
}

void RequestProcessor::on_requires_flush() {
  if (!timer_.is_running()) {
    is_processing_.store(true);
    start_coalescing();
  }
}

void RequestProcessor::on_close(ConnectionPoolManager* manager) {
  for (LoadBalancingPolicy::Vec::const_iterator it = load_balancing_policies_.begin();
       it != load_balancing_policies_.end(); ++it) {
    (*it)->close_handles();
  }
  async_.close_handle();
  prepare_.close_handle();
  timer_.stop();
  connection_pool_manager_.reset();
  listener_->on_close(this);
  dec_ref();
}

void RequestProcessor::on_prepared_metadata_changed(const String& id,
                                                    const PreparedMetadata::Entry::Ptr& entry) {
  listener_->on_prepared_metadata_changed(id, entry);
}

void RequestProcessor::on_keyspace_changed(const String& keyspace,
                                           KeyspaceChangedResponse response) {
  listener_->on_keyspace_changed(
      keyspace, KeyspaceChangedHandler::Ptr(new KeyspaceChangedHandler(event_loop_, response)));
}

bool RequestProcessor::on_wait_for_tracing_data(const RequestHandler::Ptr& request_handler,
                                                const Host::Ptr& current_host,
                                                const Response::Ptr& response) {
  TracingDataHandler::Ptr handler(new TracingDataHandler(
      request_handler, current_host, response, settings_.tracing_consistency,
      settings_.max_tracing_wait_time_ms, settings_.retry_tracing_wait_time_ms));

  return write_wait_callback(request_handler, current_host, handler->callback());
}

bool RequestProcessor::on_wait_for_schema_agreement(const RequestHandler::Ptr& request_handler,
                                                    const Host::Ptr& current_host,
                                                    const Response::Ptr& response) {
  SchemaAgreementHandler::Ptr handler(
      new SchemaAgreementHandler(request_handler, current_host, response, this,
                                 settings_.max_schema_wait_time_ms, settings_.address_factory));

  return write_wait_callback(request_handler, current_host, handler->callback());
}

bool RequestProcessor::on_prepare_all(const RequestHandler::Ptr& request_handler,
                                      const Host::Ptr& current_host,
                                      const Response::Ptr& response) {
  if (!settings_.prepare_on_all_hosts) {
    return false;
  }

  AddressVec addresses = connection_pool_manager_->available();
  if (addresses.empty() || (addresses.size() == 1 && addresses[0] == current_host->address())) {
    return false;
  }

  PrepareAllHandler::Ptr prepare_all_handler(
      new PrepareAllHandler(current_host, response, request_handler,
                            // Subtract the node that's already been prepared
                            addresses.size() - 1));

  for (AddressVec::const_iterator it = addresses.begin(), end = addresses.end(); it != end; ++it) {
    const Address& address(*it);

    // Skip over the node we've already prepared
    if (address == current_host->address()) {
      continue;
    }

    // The destructor of `PrepareAllCallback` will decrement the remaining
    // count in `PrepareAllHandler` even if this is unable to write to a
    // connection successfully.
    PrepareAllCallback::Ptr prepare_all_callback(
        new PrepareAllCallback(address, prepare_all_handler));

    PooledConnection::Ptr connection(connection_pool_manager_->find_least_busy(address));
    if (connection) {
      connection->write(prepare_all_callback.get());
    }
  }

  return true;
}

void RequestProcessor::on_done() {
#ifdef CASS_INTERNAL_DIAGNOSTICS
  reads_during_coalesce_++;
#endif
  maybe_close(request_count_.fetch_sub(1) - 1);
}

bool RequestProcessor::on_is_host_up(const Address& address) {
  return default_profile_.load_balancing_policy()->is_host_up(address);
}

void RequestProcessor::internal_close() {
  is_closing_ = true;
  maybe_close(request_count_.load());
}

void RequestProcessor::internal_pool_down(const Address& address) {
  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(); it != policies.end(); ++it) {
    (*it)->on_host_down(address);
  }
}

const ExecutionProfile* RequestProcessor::execution_profile(const String& name) const {
  // Determine if cluster profile should be used
  if (name.empty()) {
    return &default_profile_;
  }

  // Handle profile lookup
  ExecutionProfile::Map::const_iterator it = profiles_.find(name);
  if (it != profiles_.end()) {
    return &it->second;
  }
  return NULL;
}

const LoadBalancingPolicy::Vec& RequestProcessor::load_balancing_policies() const {
  return load_balancing_policies_;
}

void RequestProcessor::internal_host_add(const Host::Ptr& host) {
  if (connection_pool_manager_) {
    LoadBalancingPolicy::Vec policies = load_balancing_policies();
    if (!is_host_ignored(policies, host)) {
      connection_pool_manager_->add(host);
      for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(); it != policies.end();
           ++it) {
        if ((*it)->distance(host) != CASS_HOST_DISTANCE_IGNORE) {
          (*it)->on_host_added(host);
        }
      }
    } else {
      LOG_DEBUG("Host %s will be ignored by all query plans", host->address_string().c_str());
    }
  }
}

void RequestProcessor::internal_host_remove(const Host::Ptr& host) {
  if (connection_pool_manager_) {
    connection_pool_manager_->remove(host->address());
    LoadBalancingPolicy::Vec policies = load_balancing_policies();
    for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(); it != policies.end();
         ++it) {
      (*it)->on_host_removed(host);
    }
  }
}

void RequestProcessor::internal_host_ready(const Host::Ptr& host) {
  // Only mark the host as up if it has connections.
  if (connection_pool_manager_ && connection_pool_manager_->has_connections(host->address())) {
    LoadBalancingPolicy::Vec policies = load_balancing_policies();
    for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(); it != policies.end();
         ++it) {
      (*it)->on_host_up(host);
    }
  }
}

void RequestProcessor::internal_host_maybe_up(const Address& address) {
  if (connection_pool_manager_) {
    connection_pool_manager_->attempt_immediate_connect(address);
  }
}

void RequestProcessor::start_coalescing() {
  io_time_during_coalesce_ = 0;
  timer_.start(event_loop_->loop(), settings_.coalesce_delay_us,
               bind_callback(&RequestProcessor::on_timeout, this));
}

void RequestProcessor::on_timeout(MicroTimer* timer) {
  // Don't process for more time than the coalesce delay.
  uint64_t processing_time =
      std::min((io_time_during_coalesce_ * settings_.new_request_ratio) / 100,
               settings_.coalesce_delay_us * 1000);
  int processed = process_requests(processing_time);

  connection_pool_manager_->flush();

  if (processed > 0) {
    attempts_without_requests_ = 0;

#ifdef CASS_INTERNAL_DIAGNOSTICS
    reads_per_.record_value(reads_during_coalesce_);
    writes_per_.record_value(writes_during_coalesce_);
    reads_during_coalesce_ = 0;
    writes_during_coalesce_ = 0;
#endif
  } else {
    // Keep trying to process more requests before for a few iterations before
    // putting the loop back to sleep.
    attempts_without_requests_++;
    if (attempts_without_requests_ > 5) {
      attempts_without_requests_ = 0;
      is_processing_.store(false);
      bool expected = false;
      if (request_queue_->is_empty() || !is_processing_.compare_exchange_strong(expected, true)) {
        return;
      }
    }
  }

  if (!timer_.is_running()) {
    start_coalescing();
  }
}

void RequestProcessor::on_async(Async* async) {
  process_requests(0);

  connection_pool_manager_->flush();

  // Always attempt to coalesce even if no requests are written so that
  // processing is properly terminated.
  if (!timer_.is_running()) {
    start_coalescing();
  }
}

void RequestProcessor::on_prepare(Prepare* prepare) {
  io_time_during_coalesce_ += event_loop_->io_time_elapsed();
}

void RequestProcessor::maybe_close(int request_count) {
  if (is_closing_ && request_count <= 0 && request_queue_->is_empty()) {
    if (connection_pool_manager_) connection_pool_manager_->close();
  }
}

int RequestProcessor::process_requests(uint64_t processing_time) {
  uint64_t finish_time = uv_hrtime() + processing_time;

  int processed = 0;
  RequestHandler* request_handler = NULL;
  while (request_queue_->dequeue(request_handler)) {
    if (request_handler) {
      const String& profile_name = request_handler->request()->execution_profile_name();
      const ExecutionProfile* profile(execution_profile(profile_name));
      if (profile) {
        if (!profile_name.empty()) {
          LOG_TRACE("Using execution profile '%s'", profile_name.c_str());
        }
        request_handler->init(*profile, connection_pool_manager_.get(), token_map_.get(),
                              settings_.timestamp_generator.get(), this);
        request_handler->execute();
        processed++;
      } else {
        maybe_close(request_count_.fetch_sub(1) - 1);
        request_handler->set_error(CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID,
                                   profile_name + " does not exist");
      }
      request_handler->dec_ref();
    }

    if ((processed & 0x3F) == 0 && // Check the finish time every 64 requests
        uv_hrtime() >= finish_time) {
      break;
    }
  }

#ifdef CASS_INTERNAL_DIAGNOSTICS
  writes_during_coalesce_ += processed;
#endif

  return processed;
}

bool RequestProcessor::write_wait_callback(const RequestHandler::Ptr& request_handler,
                                           const Host::Ptr& current_host,
                                           const RequestCallback::Ptr& callback) {
  PooledConnection::Ptr connection(
      connection_pool_manager_->find_least_busy(current_host->address()));
  if (connection && connection->write(callback.get()) > 0) {
    // Stop the original request timer now that we have a response and
    // are waiting for the maximum wait time of the handler.
    request_handler->stop_timer();
    return true;
  }
  return false;
}
