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
#include "request_processor_manager.hpp"
#include "prepare_all_handler.hpp"
#include "session.hpp"
#include "utils.hpp"

namespace cass {

class RunCloseProcessor : public Task {
public:
  RunCloseProcessor(const RequestProcessor::Ptr& processor)
    : processor_(processor) { }

  virtual void run(EventLoop* event_loop) {
    processor_->internal_close();
  }

private:
  RequestProcessor::Ptr processor_;
};

class NotifyHostAddProcessor : public Task {
public:
  NotifyHostAddProcessor(const Host::Ptr host,
                         const RequestProcessor::Ptr& request_processor)
    : request_processor_(request_processor)
    , host_(host) { }
  virtual void run(EventLoop* event_loop) {
    request_processor_->internal_host_add_down_up(host_, Host::ADDED);
  }
private:
  RequestProcessor::Ptr request_processor_;
  const Host::Ptr host_;
};

class NotifyHostRemoveProcessor : public Task {
public:
  NotifyHostRemoveProcessor(const Host::Ptr host,
                            const RequestProcessor::Ptr& request_processor)
    : request_processor_(request_processor)
    , host_(host) { }
  virtual void run(EventLoop* event_loop) {
    request_processor_->internal_host_remove(host_);
  }
private:
  RequestProcessor::Ptr request_processor_;
  const Host::Ptr host_;
};

class NotifyTokenMapUpdateProcessor : public Task {
public:
  NotifyTokenMapUpdateProcessor(const TokenMap::Ptr& token_map,
                                const RequestProcessor::Ptr& request_processor)
    : request_processor_(request_processor)
    , token_map_(token_map) { }

  virtual void run(EventLoop* event_loop) {
    request_processor_->token_map_ = token_map_;
  }
private:
  RequestProcessor::Ptr request_processor_;
  const TokenMap::Ptr token_map_;
};

RequestProcessorSettings::RequestProcessorSettings()
  : max_schema_wait_time_ms(10000)
  , prepare_on_all_hosts(true)
  , timestamp_generator(Memory::allocate<ServerSideTimestampGenerator>())
  , default_profile(Config().default_profile())
  , request_queue_size(8192)
  , coalesce_delay_us(CASS_DEFAULT_COALESCE_DELAY)
  , new_request_ratio(CASS_DEFAULT_NEW_REQUEST_RATIO) { }

RequestProcessorSettings::RequestProcessorSettings(const Config& config)
  : connection_pool_manager_settings(config)
  , max_schema_wait_time_ms(config.max_schema_wait_time_ms())
  , prepare_on_all_hosts(config.prepare_on_all_hosts())
  , timestamp_generator(config.timestamp_gen())
  , default_profile(config.default_profile())
  , profiles(config.profiles())
  , request_queue_size(config.queue_size_io())
  , coalesce_delay_us(config.coalesce_delay_us())
  , new_request_ratio(config.new_request_ratio()) { }

RequestProcessor::RequestProcessor(RequestProcessorManager* manager,
                                   EventLoop* event_loop,
                                   const ConnectionPoolManager::Ptr& connection_pool_manager,
                                   const Host::Ptr& connected_host,
                                   const HostMap& hosts,
                                   const TokenMap::Ptr& token_map,
                                   const RequestProcessorSettings& settings,
                                   Random* random)
  : connection_pool_manager_(connection_pool_manager)
  , manager_(manager)
  , event_loop_(event_loop)
  , max_schema_wait_time_ms_(settings.max_schema_wait_time_ms)
  , prepare_on_all_hosts_(settings.prepare_on_all_hosts)
  , timestamp_generator_(settings.timestamp_generator)
  , coalesce_delay_us_(settings.coalesce_delay_us)
  , new_request_ratio_(settings.new_request_ratio)
  , default_profile_(settings.default_profile)
  , profiles_(settings.profiles)
  , request_count_(0)
  , request_queue_(Memory::allocate<MPMCQueue<RequestHandler*> >(settings.request_queue_size))
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
  connection_pool_manager_->set_listener(this);

  // Build/Assign the load balancing policies from the execution profiles
  default_profile_.build_load_balancing_policy();
  load_balancing_policies_.push_back(default_profile_.load_balancing_policy());
  for (ExecutionProfile::Map::iterator it = profiles_.begin(),
       end = profiles_.end();  it != end; ++it) {
    it->second.build_load_balancing_policy();
    const LoadBalancingPolicy::Ptr& load_balancing_policy = it->second.load_balancing_policy();
    if (load_balancing_policy) {
      LOG_TRACE("Built load balancing policy for '%s' execution profile",
                it->first.c_str());
      load_balancing_policies_.push_back(load_balancing_policy);
    } else {
      it->second.set_load_balancing_policy(default_profile_.load_balancing_policy().get());
    }
  }

  token_map_ = token_map;
  hosts_ = hosts;

  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    // Initialize the load balancing policies
    (*it)->init(connected_host, hosts_, random);
  }
}

void RequestProcessor::close() {
  event_loop_->add(Memory::allocate<RunCloseProcessor>(Ptr(this)));
}

void RequestProcessor::set_keyspace(const String& keyspace) {
  connection_pool_manager_->set_keyspace(keyspace);
}

void RequestProcessor::notify_host_add(const Host::Ptr& host) {
  event_loop_->add(Memory::allocate<NotifyHostAddProcessor>(host, Ptr(this)));
}

void RequestProcessor::notify_host_remove(const Host::Ptr& host) {
  event_loop_->add(Memory::allocate<NotifyHostRemoveProcessor>(host, Ptr(this)));
}

void RequestProcessor::notify_token_map_changed(const TokenMap::Ptr& token_map) {
  event_loop_->add(Memory::allocate<NotifyTokenMapUpdateProcessor>(token_map, Ptr(this)));
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
  int rc = async_.start(event_loop_->loop(),
                        bind_callback(&RequestProcessor::on_async, this));
  if (rc != 0) return rc;
  return prepare_.start(event_loop_->loop(),
                        bind_callback(&RequestProcessor::on_prepare, this));
}

void RequestProcessor::on_pool_up(const Address& address) {
  // on_up is using the request processor event loop (no need for a task)
  Host::Ptr host = get_host(address);
  if (host) {
    internal_host_add_down_up(host, Host::UP);
  } else {
    LOG_DEBUG("Tried to up host %s that doesn't exist", address.to_string().c_str());
  }
  manager_->notify_pool_up(address, RequestProcessorManager::Protected());
}

void RequestProcessor::on_pool_down(const Address& address) {
  internal_pool_down(address);
  manager_->notify_pool_down(address, RequestProcessorManager::Protected());
}

void RequestProcessor::on_pool_critical_error(const Address& address,
                                              Connector::ConnectionError code,
                                              const String& message) {
  internal_pool_down(address);
  manager_->notify_pool_critical_error(address, code, message,
                                       RequestProcessorManager::Protected());
}

void RequestProcessor::on_close(ConnectionPoolManager* manager) {
  async_.close_handle();
  prepare_.close_handle();
  timer_.close_handle();
  manager_->notify_closed(this, RequestProcessorManager::Protected());
}

void RequestProcessor::on_result_metadata_changed(const String& prepared_id,
                                                  const String& query,
                                                  const String& keyspace,
                                                  const String& result_metadata_id,
                                                  const ResultResponse::ConstPtr& result_response) {
  PreparedMetadata::Entry::Ptr entry(
        Memory::allocate<PreparedMetadata::Entry>(query,
                                                  keyspace,
                                                  result_metadata_id,
                                                  result_response));
  manager_->notify_prepared_metadata_changed(prepared_id, entry,
                                             RequestProcessorManager::Protected());
}

void RequestProcessor::on_keyspace_changed(const String& keyspace) {
  manager_->notify_keyspace_changed(keyspace, RequestProcessorManager::Protected());
}

bool RequestProcessor::on_wait_for_schema_agreement(const RequestHandler::Ptr& request_handler,
                                                    const Host::Ptr& current_host,
                                                    const Response::Ptr& response) {
  SchemaAgreementHandler::Ptr handler(Memory::allocate<SchemaAgreementHandler>(request_handler,
                                                                               current_host,
                                                                               response,
                                                                               this,
                                                                               max_schema_wait_time_ms_));

  PooledConnection::Ptr connection(connection_pool_manager_->find_least_busy(current_host->address()));
  if (connection && connection->write(handler->callback().get())) {
    return true;
  }
  return false;
}

bool RequestProcessor::on_prepare_all(const RequestHandler::Ptr& request_handler,
                                      const Host::Ptr& current_host,
                                      const Response::Ptr& response) {
  if (!prepare_on_all_hosts_) {
    return false;
  }

  AddressVec addresses = connection_pool_manager_->available();
  if (addresses.empty() ||
      (addresses.size() == 1 && addresses[0] == current_host->address())) {
    return false;
  }

  PrepareAllHandler::Ptr prepare_all_handler(Memory::allocate<PrepareAllHandler>(current_host,
                                                                                 response,
                                                                                 request_handler,
                                                                                 // Subtract the node that's already been prepared
                                                                                 addresses.size() - 1));

  for (AddressVec::const_iterator it = addresses.begin(),
       end = addresses.end(); it != end; ++it) {
    const Address& address(*it);

    // Skip over the node we've already prepared
    if (address == current_host->address()) {
      continue;
    }

    // The destructor of `PrepareAllCallback` will decrement the remaining
    // count in `PrepareAllHandler` even if this is unable to write to a
    // connection successfully.
    PrepareAllCallback::Ptr prepare_all_callback(Memory::allocate<PrepareAllCallback>(address,
                                                                                      prepare_all_handler));

    PooledConnection::Ptr connection(connection_pool_manager_->find_least_busy(address));
    if (connection) {
      connection->write(prepare_all_callback.get());
    }
  }

  connection_pool_manager_->flush();

  return true;
}


void RequestProcessor::on_done() {
#ifdef CASS_INTERNAL_DIAGNOSTICS
  reads_during_coalesce_++;
#endif
  maybe_close(request_count_.fetch_sub(1) - 1);
}

bool RequestProcessor::on_is_host_up(const Address& address) {
  Host::Ptr host(get_host(address));
  return host && host->is_up();
}

void RequestProcessor::internal_close() {
  is_closing_ = true;
  maybe_close(request_count_.load());
}

void RequestProcessor::internal_pool_down(const Address& address) {
  Host::Ptr host = get_host(address);
  if (host) {
    internal_host_add_down_up(host, Host::DOWN);
  } else {
    LOG_DEBUG("Tried to down host %s that doesn't exist", address.to_string().c_str());
  }
}

Host::Ptr RequestProcessor::get_host(const Address& address) {
  HostMap::iterator it = hosts_.find(address);
  if (it == hosts_.end()) {
    return Host::Ptr();
  }
  return it->second;
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

void RequestProcessor::internal_host_add_down_up(const Host::Ptr& host,
                                                 Host::HostState state) {
  if (state == Host::ADDED) {
    connection_pool_manager_->add(host->address());
  }

  bool is_host_ignored = true;
  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    if ((*it)->distance(host) != CASS_HOST_DISTANCE_IGNORE) {
      is_host_ignored = false;
      switch (state) {
        case Host::ADDED:
          (*it)->on_add(host);
          break;
        case Host::DOWN:
          (*it)->on_down(host);
          break;
        case Host::UP:
          (*it)->on_up(host);
          break;
        default:
          assert(false && "Invalid host state");
          break;
      }
    }
  }

  if (is_host_ignored) {
    LOG_DEBUG("Host %s will be ignored by all query plans",
              host->address_string().c_str());
  }
}

void RequestProcessor::internal_host_remove(const Host::Ptr& host) {
  connection_pool_manager_->remove(host->address());
  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    (*it)->on_remove(host);
  }
}

void RequestProcessor::on_timeout(MicroTimer* timer) {
  int processed = process_requests((io_time_during_coalesce_ * new_request_ratio_) / 100);
  io_time_during_coalesce_ = 0;

  if (processed > 0) {
    attempts_without_requests_ = 0;
    connection_pool_manager_->flush();

#ifdef CASS_INTERNAL_DIAGNOSTICS
    reads_per_.record_value(reads_during_coalesce_);
    writes_per_.record_value(writes_during_coalesce_);
    reads_during_coalesce_ = 0;
    writes_during_coalesce_ = 0;
#endif
  } else {
    attempts_without_requests_++;
    if (attempts_without_requests_ > 5) {
      attempts_without_requests_ = 0;
      is_processing_.store(false);
      bool expected = false;
      if (request_queue_->is_empty() ||
          !is_processing_.compare_exchange_strong(expected, true)) {
        return;
      }
    }
  }

  timer_.start(event_loop_->loop(), coalesce_delay_us_,
               bind_callback(&RequestProcessor::on_timeout, this));
}

void RequestProcessor::on_async(Async* async) {
  if (process_requests(0) > 0) {
    connection_pool_manager_->flush();
  }

  if (!timer_.is_running()) {
    timer_.start(event_loop_->loop(), coalesce_delay_us_,
                 bind_callback(&RequestProcessor::on_timeout, this));
  }
}

void RequestProcessor::on_prepare(Prepare* prepare) {
  io_time_during_coalesce_ += event_loop_->io_time_elapsed();
}

void RequestProcessor::maybe_close(int request_count) {
  if (is_closing_ &&
      request_count <= 0 &&
      request_queue_->is_empty()) {
    connection_pool_manager_->close();
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
        request_handler->init(*profile,
                              connection_pool_manager_.get(),
                              token_map_.get(),
                              timestamp_generator_.get(),
                              this);
        request_handler->execute();
        processed++;
      } else {
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

} // namespace cass
