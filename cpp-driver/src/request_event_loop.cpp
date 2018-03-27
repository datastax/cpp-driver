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

#include "request_event_loop.hpp"

#include "connection_pool_manager_initializer.hpp"
#include "request_queue.hpp"
#include "session.hpp"

namespace cass {

RequestEventLoop::RequestEventLoop()
  : is_flushing_(false) { }

int RequestEventLoop::init(const Config& config,
                           const String& connect_keyspace,
                           Session* session) {
  config_ = config.new_instance();
  connect_keyspace_ = connect_keyspace;
  session_ = session;
  metrics_ = session_->metrics();
  return EventLoop::init("Request Event Loop");
}

void RequestEventLoop::connect(const Host::Ptr& current_host,
                               int protocol_version,
                               const HostMap& hosts,
                               const TokenMap* token_map) {
  internal_token_map_update(token_map);
  internal_connect(current_host, protocol_version, hosts);
}

void RequestEventLoop::keyspace_update(const String& keyspace) {
  if (manager_) manager_->set_keyspace(keyspace);
}

void RequestEventLoop::terminate() {
  internal_terminate();
}

void RequestEventLoop::notify_host_add_async(const Host::Ptr& host) {
  add(Memory::allocate<NotifyHostAdd>(host));
}

void RequestEventLoop::notify_host_remove_async(const Host::Ptr& host) {
  add(Memory::allocate<NotifyHostRemove>(host));
}

void RequestEventLoop::notify_token_map_update_async(const TokenMap* token_map) {
  add(Memory::allocate<NotifyTokenMapUpdate>(token_map));
}

void RequestEventLoop::notify_request_async() {
  // Only signal request processing if it's not already processing requests.
  bool expected = false;
  if (!is_flushing_.load() &&
      is_flushing_.compare_exchange_strong(expected, true)) {
    add(Memory::allocate<NotifyRequest>());
  }
}

AddressVec RequestEventLoop::available() const {
  if (manager_) return manager_->available();
  return AddressVec();
}

PooledConnection::Ptr RequestEventLoop::find_least_busy(const Address& address) const {
  if (manager_) return manager_->find_least_busy(address);
  return PooledConnection::Ptr();
}

void RequestEventLoop::on_up(const Address& address) {
  add(Memory::allocate<NotifyHostUp>(address));
}

void RequestEventLoop::on_down(const Address& address) {
  add(Memory::allocate<NotifyHostDown>(address));
}

void RequestEventLoop::on_critical_error(const Address& address,
                                         Connector::ConnectionError code,
                                         const String& message) {
  on_down(address);
}

void RequestEventLoop::on_close() {
  internal_close();
}

void RequestEventLoop::internal_connect(const Host::Ptr& current_host,
                                        int protocol_version,
                                        const HostMap& hosts) {
  hosts_ = hosts;

  // This needs to be done on the control connection thread because it could
  //  pause generating a new random seed.
  if (config_.use_randomized_contact_points()) {
    random_.reset(Memory::allocate<Random>());
  }

  // Initialize the load balancing policies and ensure there are hosts available
  AddressVec addresses;
  addresses.reserve(hosts_.size());
  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (HostMap::const_iterator i = hosts_.begin(), end = hosts_.end(); i != end; ++i) {
    Host::Ptr host(i->second);
    for (LoadBalancingPolicy::Vec::const_iterator j = policies.begin(),
         end = policies.end(); j != end; ++j) {
      if ((*j)->distance(host) != CASS_HOST_DISTANCE_IGNORE) {
        addresses.push_back(host->address());
        (*j)->init(current_host, hosts_, random_.get());
        (*j)->register_handles(loop());
      }
    }
  }

  if (!addresses.empty()) {
    ConnectionPoolManagerInitializer::Ptr initializer(Memory::allocate<ConnectionPoolManagerInitializer>(this,
                                                      protocol_version,
                                                      this,
                                                      on_connection_pool_manager_initialize));
    initializer
      ->with_settings(ConnectionPoolManagerSettings(config_))
      ->with_listener(this)
      ->with_keyspace(connect_keyspace_)
      ->with_metrics(metrics_)
      ->initialize(addresses);
  }
}

void RequestEventLoop::internal_close() {
  while (is_flushing_.load()) {
    ;; // Wait for flushing to complete
  }

  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    (*it)->close_handles();
  }
  EventLoop::close_handles();
}

void RequestEventLoop::internal_terminate() {
  if (manager_) {
    manager_->close();
  } else {
    internal_close(); // Manager is not available; however LBPs need to be properly closed
  }
}

void RequestEventLoop::internal_token_map_update(const TokenMap* token_map) {
  token_map_.reset(token_map);
}

Host::Ptr RequestEventLoop::get_host(const Address& address) {
  HostMap::iterator it = hosts_.find(address);
  if (it == hosts_.end()) {
    return Host::Ptr();
  }
  return it->second;
}

const LoadBalancingPolicy::Vec& RequestEventLoop::load_balancing_policies() const {
  return config_.load_balancing_policies();
}

void RequestEventLoop::on_connection_pool_manager_initialize(ConnectionPoolManagerInitializer* initializer) {
  RequestEventLoop* request_event_loop = static_cast<RequestEventLoop*>(initializer->data());
  request_event_loop->add(Memory::allocate<NotifyConnectionPoolManagerInitalize>(initializer->release_manager(),
                                                                                 initializer->failures()));
}

void RequestEventLoop::NotifyConnectionPoolManagerInitalize::run(EventLoop* event_loop) {
  RequestEventLoop* request_event_loop = static_cast<RequestEventLoop*>(event_loop);
  request_event_loop->internal_connection_pool_manager_initialize(manager_, failures_);
}

void RequestEventLoop::internal_connection_pool_manager_initialize(const ConnectionPoolManager::Ptr& manager,
                                                                   const ConnectionPoolConnector::Vec& failures) {
  manager_ = manager;

  // Check for failed connection(s)
  bool is_keyspace_error = false;
  for (ConnectionPoolConnector::Vec::const_iterator it = failures.begin(),
       end = failures.end(); it != end; ++it) {
    ConnectionPoolConnector::Ptr connector(*it);
    if (connector->is_keyspace_error()) {
      is_keyspace_error = true;
      break;
    } else {
      hosts_.erase(connector->address());
    }
  }

  if (is_keyspace_error) {
    session_->notify_connect_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                                   "Keyspace '" + connect_keyspace_
                                   + "' does not exist");
  } else if (hosts_.empty()) {
    session_->notify_connect_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                                   "Unable to connect to any hosts");
  } else {
    for (HostMap::iterator it = hosts_.begin(), end = hosts_.end();
         it != end; ++it) {
      it->second->set_up();
    }
    session_->notify_connected();
  }
}

void RequestEventLoop::internal_host_add_down_up(const Host::Ptr& host,
                                                 Host::HostState state) {
  bool is_host_ignored = true;
  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    if ((*it)->distance(host) != CASS_HOST_DISTANCE_IGNORE) {
      is_host_ignored = false;
      switch (state) {
        case Host::ADDED:
          (*it)->on_add(host);
          manager_->add(host->address());
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
    //TODO: Should this message only appear once per request event loop?
    LOG_DEBUG("Host %s will be ignored by all query plans",
              host->address_string().c_str());
  }
}

void RequestEventLoop::internal_host_remove(const Host::Ptr& host) {
  LoadBalancingPolicy::Vec policies = load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    (*it)->on_remove(host);
  }
}

void RequestEventLoop::on_flush_timer(Timer* timer) {
  RequestEventLoop* request_event_loop = static_cast<RequestEventLoop*>(timer->data());
  request_event_loop->internal_flush_requests();
}

void RequestEventLoop::internal_flush_requests() {
  const int flush_ratio = 90;
  uint64_t start_time_ns = uv_hrtime();

  RequestHandler* request_handler = NULL;
  while (session_->dequeue(request_handler)) {
    if (request_handler) {
      const String& profile_name = request_handler->request()->execution_profile_name();
      ExecutionProfile profile;
      if (config_.profile(profile_name, profile)) {
        if (!profile_name.empty()) {
          LOG_TRACE("Using execution profile '%s'", profile_name.c_str());
        }
        request_handler->init(config_,
                              profile,
                              manager_.get(),
                              token_map_.get(),
                              session_->prepared_metadata());
        request_handler->execute();
      } else {
        request_handler->set_error(CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID,
                                   profile_name + " does not exist");
      }
      request_handler->dec_ref();
    }
  }

  // Determine if a another flush should be scheduled
  is_flushing_.store(false);
  bool expected = false;
  if (session_->request_queue_empty() ||
      !is_flushing_.compare_exchange_strong(expected, true)) {
    return;
  }

  uint64_t flush_time_ns = uv_hrtime() - start_time_ns;
  uint64_t processing_time_ns = flush_time_ns * (100 - flush_ratio) / flush_ratio;
  if (processing_time_ns >= 1000000) { // Schedule another flush to be run in the future
    timer_.start(loop(), (processing_time_ns + 500000) / 1000000, this, on_flush_timer);
  } else {
    add(Memory::allocate<NotifyRequest>()); // Schedule another flush to be run immediately
  }
}

void RequestEventLoop::NotifyTokenMapUpdate::run(EventLoop* event_loop) {
  RequestEventLoop* request_event_loop = static_cast<RequestEventLoop*>(event_loop);
  request_event_loop->internal_token_map_update(token_map_);
}

void RequestEventLoop::NotifyHostAdd::run(EventLoop* event_loop) {
  RequestEventLoop* request_event_loop = static_cast<RequestEventLoop*>(event_loop);
  request_event_loop->internal_host_add_down_up(host_, Host::ADDED);
}

void RequestEventLoop::NotifyHostRemove::run(EventLoop* event_loop) {
  RequestEventLoop* request_event_loop = static_cast<RequestEventLoop*>(event_loop);
  request_event_loop->internal_host_remove(host_);
}

void RequestEventLoop::NotifyHostDown::run(EventLoop* event_loop) {
  RequestEventLoop* request_event_loop = static_cast<RequestEventLoop*>(event_loop);
  Host::Ptr host = request_event_loop->get_host(address_);
  request_event_loop->internal_host_add_down_up(host, Host::DOWN);
}

void RequestEventLoop::NotifyHostUp::run(EventLoop* event_loop) {
  RequestEventLoop* request_event_loop = static_cast<RequestEventLoop*>(event_loop);
  Host::Ptr host = request_event_loop->get_host(address_);
  request_event_loop->internal_host_add_down_up(host, Host::UP);
}

void RequestEventLoop::NotifyRequest::run(EventLoop* event_loop) {
  RequestEventLoop* request_event_loop = static_cast<RequestEventLoop*>(event_loop);
  request_event_loop->internal_flush_requests();
}

int RoundRobinRequestEventLoopGroup::init(const Config& config,
                                          const String& keyspace,
                                          Session* session) {
  for (size_t i = 0; i < threads_.size(); ++i) {
    int rc = threads_[i].init(config, keyspace, session);
    if (rc != 0) return rc;
  }
  return 0;
}

void RoundRobinRequestEventLoopGroup::run() {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].run();
  }
}

void RoundRobinRequestEventLoopGroup::join() {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].join();
  }
}

cass::AddressVec RoundRobinRequestEventLoopGroup::available() {
  RequestEventLoop* request_event_loop = &threads_[current_.fetch_add(1) % threads_.size()];
  return request_event_loop->available();
}

PooledConnection::Ptr RoundRobinRequestEventLoopGroup::find_least_busy(const Address& address) {
  RequestEventLoop* request_event_loop = &threads_[current_.fetch_add(1) % threads_.size()];
  return request_event_loop->find_least_busy(address);
}

void RoundRobinRequestEventLoopGroup::connect(const Host::Ptr& current_host,
                                              int protocol_version,
                                              const HostMap& hosts,
                                              const TokenMap* token_map) {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].connect(current_host,
                        protocol_version,
                        hosts,
                        token_map->clone());
  }
}

void RoundRobinRequestEventLoopGroup::terminate() {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].terminate();
  }
}

void RoundRobinRequestEventLoopGroup::notify_host_add_async(const Host::Ptr& host) {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].notify_host_add_async(host);
  }
}

void RoundRobinRequestEventLoopGroup::notify_host_remove_async(const Host::Ptr& host) {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].notify_host_remove_async(host);
  }
}

void RoundRobinRequestEventLoopGroup::keyspace_update(const String& keyspace) {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].keyspace_update(keyspace);
  }
}

void RoundRobinRequestEventLoopGroup::notify_token_map_update_async(const TokenMap* token_map) {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].notify_token_map_update_async(token_map->clone());
  }
}

void RoundRobinRequestEventLoopGroup::notify_request_async() {
  RequestEventLoop* request_event_loop = &threads_[current_.fetch_add(1) % threads_.size()];
  request_event_loop->notify_request_async();
}

} // namespace cass
