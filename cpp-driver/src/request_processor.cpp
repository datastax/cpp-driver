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

RequestProcessor::RequestProcessor(RequestProcessorManager* manager,
                                   EventLoop* event_loop,
                                   const ConnectionPoolManager::Ptr& connection_pool_manager,
                                   const Host::Ptr& connected_host,
                                   const HostMap& hosts,
                                   const TokenMap::Ptr& token_map,
                                   const RequestProcessorSettings& settings,
                                   Random* random,
                                   MPMCQueue<RequestHandler*>* request_queue)
  : connection_pool_manager_(connection_pool_manager)
  , manager_(manager)
  , event_loop_(event_loop)
  , max_schema_wait_time_ms_(settings.max_schema_wait_time_ms)
  , prepare_on_all_hosts_(settings.prepare_on_all_hosts)
  , timestamp_generator_(settings.timestamp_generator)
  , default_profile_(settings.default_profile)
  , profiles_(settings.profiles)
  , request_queue_(request_queue)
  , is_flushing_(false)
  , is_closing_(false)
  , attempts_without_writes_(0) {
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
    (*it)->register_handles(event_loop_->loop());
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

void RequestProcessor::notify_request() {
  // Only signal the request queue if it's not already processing requests.
  bool expected = false;
  if (!is_flushing_.load() && is_flushing_.compare_exchange_strong(expected, true)) {
    async_.send();
  }
}

int RequestProcessor::init(Protected) {
  int rc;
  rc = async_.start(event_loop_->loop(), this, on_process);
  if (rc != 0) return rc;
  return prepare_.start(event_loop_->loop(), this, on_flush);
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
  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    (*it)->close_handles();
  }

  is_closing_.store(true);
  async_.send();
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

  return true;
}

bool RequestProcessor::on_is_host_up(const Address& address) {
  Host::Ptr host(get_host(address));
  return host && host->is_up();
}

void RequestProcessor::internal_close() {
  connection_pool_manager_->close();
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
  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    (*it)->on_remove(host);
  }
}

void RequestProcessor::on_process(Async* async) {
  RequestProcessor* request_processor = static_cast<RequestProcessor*>(async->data());
  request_processor->handle_process();
}

void RequestProcessor::on_process_timer(Timer* timer) {
  RequestProcessor* request_processor = static_cast<RequestProcessor*>(timer->data());
  request_processor->handle_process();
}

void RequestProcessor::handle_process() {
  const int new_request_ratio = 90;
  uint64_t start_time_ns = uv_hrtime();

  while (true) {
     bool writes_done = false;

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
          writes_done = true;
        } else {
          request_handler->set_error(CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID,
                                     profile_name + " does not exist");
        }
        request_handler->dec_ref();
      }
    }

    if (is_closing_.load()) {
      async_.close_handle();
      prepare_.close_handle();
      timer_.close_handle();
      manager_->notify_closed(this, RequestProcessorManager::Protected());
      return;
    }

    if (writes_done) {
      attempts_without_writes_ = 0;
    } else {
      if (++attempts_without_writes_ > 5) {
        attempts_without_writes_ = 0;
        // Determine if a another flush should be scheduled
        is_flushing_.store(false);
        bool expected = false;
        if (request_queue_->is_empty() ||
            !is_flushing_.compare_exchange_strong(expected, true)) {
          return;
        }
      }
    }

    uint64_t flush_time_ns = uv_hrtime() - start_time_ns;
    uint64_t processing_time_ns = flush_time_ns * (100 - new_request_ratio) / new_request_ratio;
    if (processing_time_ns >= 1000000) { // Schedule more request processing to be run in the future
      timer_.start(event_loop_->loop(), (processing_time_ns + 500000) / 1000000, this, on_process_timer);
      return;
    }
  }
}

void RequestProcessor::on_flush(Prepare* prepare) {
  RequestProcessor* processor = static_cast<RequestProcessor*>(prepare->data());
  processor->handle_flush();
}

void RequestProcessor::handle_flush() {
  connection_pool_manager_->flush();
}

} // namespace cass
