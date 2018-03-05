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

#include "session.hpp"

#include "batch_request.hpp"
#include "cluster.hpp"
#include "config.hpp"
#include "connection_pool_manager_initializer.hpp"
#include "constants.hpp"
#include "execute_request.hpp"
#include "logger.hpp"
#include "prepare_all_handler.hpp"
#include "prepare_request.hpp"
#include "scoped_lock.hpp"
#include "statement.hpp"
#include "timer.hpp"
#include "external.hpp"

extern "C" {

CassSession* cass_session_new() {
  return CassSession::to(cass::Memory::allocate<cass::Session>());
}

void cass_session_free(CassSession* session) { // This attempts to close the session because the joining will
  // hang indefinitely otherwise. This causes minimal delay
  // if the session is already closed.
  cass::SharedRefPtr<cass::Future> future(cass::Memory::allocate<cass::SessionFuture>());
  session->close_async(future);
  future->wait();

  cass::Memory::deallocate(session->from());
}

CassFuture* cass_session_connect(CassSession* session, const CassCluster* cluster) {
  return cass_session_connect_keyspace(session, cluster, "");
}

CassFuture* cass_session_connect_keyspace(CassSession* session,
                                          const CassCluster* cluster,
                                          const char* keyspace) {
  return cass_session_connect_keyspace_n(session,
                                         cluster,
                                         keyspace,
                                         SAFE_STRLEN(keyspace));
}

CassFuture* cass_session_connect_keyspace_n(CassSession* session,
                                            const CassCluster* cluster,
                                            const char* keyspace,
                                            size_t keyspace_length) {
  cass::SessionFuture::Ptr connect_future(cass::Memory::allocate<cass::SessionFuture>());
  session->connect_async(cluster->config(), cass::String(keyspace, keyspace_length), connect_future);
  connect_future->inc_ref();
  return CassFuture::to(connect_future.get());
}

CassFuture* cass_session_close(CassSession* session) {
  cass::SessionFuture::Ptr close_future(cass::Memory::allocate<cass::SessionFuture>());
  session->close_async(close_future);
  close_future->inc_ref();
  return CassFuture::to(close_future.get());
}

CassFuture* cass_session_prepare(CassSession* session, const char* query) {
  return cass_session_prepare_n(session, query, SAFE_STRLEN(query));
}

CassFuture* cass_session_prepare_n(CassSession* session,
                                   const char* query,
                                   size_t query_length) {
  cass::Future::Ptr future(session->prepare(query, query_length));
  future->inc_ref();
  return CassFuture::to(future.get());
}

CassFuture* cass_session_prepare_from_existing(CassSession* session,
                                               CassStatement* statement) {
  cass::Future::Ptr future(session->prepare(statement));
  future->inc_ref();
  return CassFuture::to(future.get());
}

CassFuture* cass_session_execute(CassSession* session,
                                 const CassStatement* statement) {
  cass::Future::Ptr future(session->execute(cass::Request::ConstPtr(statement->from())));
  future->inc_ref();
  return CassFuture::to(future.get());
}

CassFuture* cass_session_execute_batch(CassSession* session, const CassBatch* batch) {
  cass::Future::Ptr future(session->execute(cass::Request::ConstPtr(batch->from())));
  future->inc_ref();
  return CassFuture::to(future.get());
}

const CassSchemaMeta* cass_session_get_schema_meta(const CassSession* session) {
  return CassSchemaMeta::to(cass::Memory::allocate<cass::Metadata::SchemaSnapshot>(session->metadata().schema_snapshot(session->cassandra_version())));
}

void  cass_session_get_metrics(const CassSession* session,
                               CassMetrics* metrics) {
  const cass::Metrics* internal_metrics = session->metrics();

  cass::Metrics::Histogram::Snapshot requests_snapshot;
  internal_metrics->request_latencies.get_snapshot(&requests_snapshot);

  metrics->requests.min = requests_snapshot.min;
  metrics->requests.max = requests_snapshot.max;
  metrics->requests.mean = requests_snapshot.mean;
  metrics->requests.stddev = requests_snapshot.stddev;
  metrics->requests.median = requests_snapshot.median;
  metrics->requests.percentile_75th = requests_snapshot.percentile_75th;
  metrics->requests.percentile_95th = requests_snapshot.percentile_95th;
  metrics->requests.percentile_98th = requests_snapshot.percentile_98th;
  metrics->requests.percentile_99th = requests_snapshot.percentile_99th;
  metrics->requests.percentile_999th = requests_snapshot.percentile_999th;
  metrics->requests.one_minute_rate = internal_metrics->request_rates.one_minute_rate();
  metrics->requests.five_minute_rate = internal_metrics->request_rates.five_minute_rate();
  metrics->requests.fifteen_minute_rate = internal_metrics->request_rates.fifteen_minute_rate();
  metrics->requests.mean_rate = internal_metrics->request_rates.mean_rate();

  metrics->stats.total_connections = internal_metrics->total_connections.sum();
  metrics->stats.available_connections = metrics->stats.total_connections; // Deprecated
  metrics->stats.exceeded_write_bytes_water_mark = 0; // Deprecated
  metrics->stats.exceeded_pending_requests_water_mark = 0; // Deprecated

  metrics->errors.connection_timeouts = internal_metrics->connection_timeouts.sum();
  metrics->errors.pending_request_timeouts = internal_metrics->pending_request_timeouts.sum();
  metrics->errors.request_timeouts = internal_metrics->request_timeouts.sum();
}

void  cass_session_get_speculative_execution_metrics(const CassSession* session,
                                                     CassSpeculativeExecutionMetrics* metrics) {
  const cass::Metrics* internal_metrics = session->metrics();

  cass::Metrics::Histogram::Snapshot speculative_snapshot;
  internal_metrics->speculative_request_latencies.get_snapshot(&speculative_snapshot);

  metrics->min = speculative_snapshot.min;
  metrics->max = speculative_snapshot.max;
  metrics->mean = speculative_snapshot.mean;
  metrics->stddev = speculative_snapshot.stddev;
  metrics->median = speculative_snapshot.median;
  metrics->percentile_75th = speculative_snapshot.percentile_75th;
  metrics->percentile_95th = speculative_snapshot.percentile_95th;
  metrics->percentile_98th = speculative_snapshot.percentile_98th;
  metrics->percentile_99th = speculative_snapshot.percentile_99th;
  metrics->percentile_999th = speculative_snapshot.percentile_999th;
  metrics->count =
      internal_metrics->request_rates.speculative_request_count();
  metrics->percentage =
      internal_metrics->request_rates.speculative_request_percent();
}

} // extern "C"

namespace cass {

Session::Session()
  : state_(SESSION_STATE_CLOSED)
  , connect_error_code_(CASS_OK)
  , current_host_mark_(true) {
  uv_mutex_init(&state_mutex_);
  uv_mutex_init(&hosts_mutex_);
}

Session::~Session() {
  join();
  uv_mutex_destroy(&state_mutex_);
  uv_mutex_destroy(&hosts_mutex_);
}

void Session::clear(const Config& config) {
  config_ = config.new_instance();
  random_.reset();
  metrics_.reset(Memory::allocate<Metrics>(config_.thread_count_io() + 1));
  connect_future_.reset();
  close_future_.reset();
  { // Lock hosts
    ScopedMutex l(&hosts_mutex_);
    hosts_.clear();
  }
  manager_.reset();
  request_queue_.reset();
  event_loop_group_.reset();
  request_queue_manager_.reset();
  metadata_.clear();
  control_connection_.clear();
  connect_error_code_ = CASS_OK;
  connect_error_message_.clear();
  current_host_mark_ = true;
}

int Session::init() {
  int rc = EventLoop::init();
  request_queue_.reset(
        Memory::allocate<AsyncQueue<MPMCQueue<RequestHandler*> > >(config_.queue_size_io()));
  rc = request_queue_->init(loop(), this, &Session::on_execute);
  if (rc != 0) return rc;

  event_loop_group_.reset(Memory::allocate<RoundRobinEventLoopGroup>(config_.thread_count_io()));
  rc = event_loop_group_->init();
  if (rc != 0) return rc;

  request_queue_manager_.reset(Memory::allocate<RequestQueueManager>(event_loop_group_.get()));
  rc = request_queue_manager_->init(config().queue_size_io());
  if (rc != 0) return rc;

  return rc;
}

Host::Ptr Session::get_host(const Address& address) {
  // Lock hosts. This can be called on a non-session thread.
  ScopedMutex l(&hosts_mutex_);
  HostMap::iterator it = hosts_.find(address);
  if (it == hosts_.end()) {
    return Host::Ptr();
  }
  return it->second;
}

Host::Ptr Session::add_host(const Address& address) {
  LOG_DEBUG("Adding new host: %s", address.to_string().c_str());
  Host::Ptr host(Memory::allocate<Host>(address, !current_host_mark_));
  { // Lock hosts
    ScopedMutex l(&hosts_mutex_);
    hosts_[address] = host;
  }
  return host;
}

void Session::purge_hosts(bool is_initial_connection) {
  // Hosts lock not held for reading (only called on session thread)
  HostMap::iterator it = hosts_.begin();
  while (it != hosts_.end()) {
    if (it->second->mark() != current_host_mark_) {
      HostMap::iterator to_remove_it = it++;

      String address_str = to_remove_it->first.to_string();
      if (is_initial_connection) {
        LOG_WARN("Unable to reach contact point %s", address_str.c_str());
        { // Lock hosts
          ScopedMutex l(&hosts_mutex_);
          hosts_.erase(to_remove_it);
        }
      } else {
        LOG_WARN("Host %s removed", address_str.c_str());
        on_remove(to_remove_it->second);
      }
    } else {
      ++it;
    }
  }
  current_host_mark_ = !current_host_mark_;
}

bool Session::prepare_host(const Host::Ptr& host,
                           PrepareHostHandler::Callback callback) {
  if (config_.prepare_on_up_or_add_host()) {
    PrepareHostHandler::Ptr prepare_host_handler(
          Memory::allocate<PrepareHostHandler>(host,
                                               this,
                                               control_connection_.protocol_version()));
    prepare_host_handler->prepare(callback);
    return true;
  }
  return false;
}

void Session::on_prepare_host_add(const PrepareHostHandler* handler) {
  handler->session()->internal_on_add(handler->host());
}

void Session::on_prepare_host_up(const PrepareHostHandler* handler) {
  handler->session()->internal_on_up(handler->host());
}

void Session::notify_initialize_async(const ConnectionPoolManager::Ptr& manager,
                                      const ConnectionPoolConnector::Vec& failures) {
  add(Memory::allocate<NotifyInitalize>(manager, failures));
}

void Session::notify_up_async(const Address& address) {
  add(Memory::allocate<NotifyUp>(address));
}

void Session::notify_down_async(const Address& address) {
  add(Memory::allocate<NotifyDown>(address));
}

void Session::connect_async(const Config& config, const String& keyspace, const Future::Ptr& future) {
  ScopedMutex l(&state_mutex_);

  if (state_.load(MEMORY_ORDER_RELAXED) != SESSION_STATE_CLOSED) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                      "Already connecting, connected or closed");
    return;
  }

  clear(config);

  if (init() != 0) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT,
                      "Error initializing session");
    return;
  }

  state_.store(SESSION_STATE_CONNECTING, MEMORY_ORDER_RELAXED);
  connect_keyspace_ = keyspace;
  connect_future_ = future;

  add(Memory::allocate<NotifyConnect>());

  LOG_DEBUG("Issued connect event");

  // If this is a reconnect then the old thread needs to be
  // joined before creating a new thread.
  join();

  run();
}

void Session::close_async(const Future::Ptr& future) {
  ScopedMutex l(&state_mutex_);

  State state = state_.load(MEMORY_ORDER_RELAXED);
  if (state == SESSION_STATE_CLOSING || state == SESSION_STATE_CLOSED) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_CLOSE,
                      "Already closing or closed");
    return;
  }

  state_.store(SESSION_STATE_CLOSING, MEMORY_ORDER_RELAXED);
  close_future_ = future;

  internal_close();
}

void Session::internal_connect() {
  if (hosts_.empty()) { // No hosts lock necessary (only called on session thread)
    notify_connect_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                         "No hosts provided or no hosts resolved");
    return;
  }
  control_connection_.connect(this);
}

void Session::internal_close() {
  while (!request_queue_->enqueue(NULL)) {
    // Keep trying
  }

  LOG_DEBUG("Issued close");
}

void Session::notify_connected() {
  LOG_DEBUG("Session is connected");

  ScopedMutex l(&state_mutex_);

  if (state_.load(MEMORY_ORDER_RELAXED) == SESSION_STATE_CONNECTING) {
    state_.store(SESSION_STATE_CONNECTED, MEMORY_ORDER_RELAXED);
  }
  if (connect_future_) {
    connect_future_->set();
    connect_future_.reset();
  }
}

void Session::notify_connect_error(CassError code, const String& message) {
  ScopedMutex l(&state_mutex_);

  State state = state_.load(MEMORY_ORDER_RELAXED);
  if (state == SESSION_STATE_CLOSING || state == SESSION_STATE_CLOSED) {
    return;
  }

  state_.store(SESSION_STATE_CLOSING, MEMORY_ORDER_RELAXED);
  connect_error_code_ = code;
  connect_error_message_ = message;

  internal_close();
}

void Session::notify_closed() {
  LOG_DEBUG("Session is disconnected");

  ScopedMutex l(&state_mutex_);

  state_.store(SESSION_STATE_CLOSED, MEMORY_ORDER_RELAXED);
  if (connect_future_) {
    connect_future_->set_error(connect_error_code_, connect_error_message_);
    connect_future_.reset();
  }
  if (close_future_) {
    close_future_->set();
    close_future_.reset();
  }
}

void Session::close_handles() {
  EventLoop::close_handles();
  event_loop_group_->close_handles();
  request_queue_manager_->close_handles();
  request_queue_->close_handles();
  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    (*it)->close_handles();
  }
}

void Session::on_run() {
  LOG_DEBUG("Creating %u IO worker threads",
            static_cast<unsigned int>(config_.thread_count_io()));
  event_loop_group_->run();
}

void Session::on_after_run() {
  event_loop_group_->join();
  notify_closed();
}

void Session::NotifyConnect::run(EventLoop* event_loop) {
  Session* session = static_cast<Session*>(event_loop);
  session->handle_notify_connect();
}

void Session::handle_notify_connect() {
  int port = config_.port();

  // This needs to be done on the session thread because it could pause
  // generating a new random seed.
  if (config_.use_randomized_contact_points()) {
    random_.reset(Memory::allocate<Random>());
  }

  MultiResolver<Session*>::Ptr resolver(
        Memory::allocate<MultiResolver<Session*> >(this, on_resolve,
                                                   on_resolve_done));

  const ContactPointList& contact_points = config_.contact_points();
  for (ContactPointList::const_iterator it = contact_points.begin(),
       end = contact_points.end();
       it != end; ++it) {
    const String& seed = *it;
    Address address;
    if (Address::from_string(seed, port, &address)) {
      add_host(address);
    } else {
      resolver->resolve(loop(), seed, port, config_.resolve_timeout_ms());
    }
  }
}

void Session::NotifyInitalize::run(EventLoop* event_loop) {
  Session* session = static_cast<Session*>(event_loop);
  session->handle_notify_initialize(manager_, failures_);
}

void Session::handle_notify_initialize(const ConnectionPoolManager::Ptr& manager,
                                       const ConnectionPoolConnector::Vec& failures) {
  bool is_keyspace_error = false;

  manager_ = manager;

  for (ConnectionPoolConnector::Vec::const_iterator it = failures.begin(),
       end = failures.end(); it != end; ++it) {
    ConnectionPoolConnector::Ptr connector(*it);
    if (connector->is_keyspace_error()) {
      is_keyspace_error = true;
      break;
    } else { // Lock hosts
      ScopedMutex l(&hosts_mutex_);
      hosts_.erase(connector->address());
    }
  }

  if (is_keyspace_error) {
    notify_connect_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                         "Keyspace '" + manager_->keyspace() + "' does not exist");
  } else if (hosts_.empty()) {
    notify_connect_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                         "Unable to connect to any hosts");
  } else {
    for (HostMap::iterator it = hosts_.begin(), end = hosts_.end();
         it != end; ++it) {
      it->second->set_up();
    }
    notify_connected();
  }
}

void Session::NotifyUp::run(EventLoop* event_loop) {
  Session* session = static_cast<Session*>(event_loop);
  session->control_connection_.on_up(address_);
}

void Session::NotifyDown::run(EventLoop* event_loop) {
  Session* session = static_cast<Session*>(event_loop);
  session->control_connection_.on_down(address_);
}

void Session::on_resolve(MultiResolver<Session*>::Resolver* resolver) {
  Session* session = resolver->data()->data();
  if (resolver->is_success()) {
    AddressVec addresses = resolver->addresses();
    for (AddressVec::iterator it = addresses.begin(); it != addresses.end(); ++it) {
      Host::Ptr host = session->add_host(*it);
      host->set_hostname(resolver->hostname());
    }
  } else if (resolver->is_timed_out()) {
    LOG_ERROR("Timed out attempting to resolve address for %s:%d\n",
              resolver->hostname().c_str(), resolver->port());
  } else {
    LOG_ERROR("Unable to resolve address for %s:%d\n",
              resolver->hostname().c_str(), resolver->port());
  }
}

void Session::on_resolve_done(MultiResolver<Session*>* resolver) {
  resolver->data()->internal_connect();
}

void Session::execute(const RequestHandler::Ptr& request_handler) {
  if (state_.load(MEMORY_ORDER_ACQUIRE) != SESSION_STATE_CONNECTED) {
    request_handler->set_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                               "Session is not connected");
    return;
  }

  request_handler->inc_ref(); // Queue reference
  if (!request_queue_->enqueue(request_handler.get())) {
    request_handler->dec_ref();
    request_handler->set_error(CASS_ERROR_LIB_REQUEST_QUEUE_FULL,
                               "The request queue has reached capacity");
  }
}

void Session::on_control_connection_ready() {
  // No hosts lock necessary (only called on session thread and read-only)
  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    (*it)->init(control_connection_.connected_host(), hosts_, random_.get());
    (*it)->register_handles(loop());
  }

  AddressVec addresses;
  addresses.reserve(hosts_.size());

  // If addresses aren't ignored by all the load balancing policies then add
  // them to be initialized on the thread pool.
  for (HostMap::iterator i = hosts_.begin(), end = hosts_.end();
       i != end; ++i) {
    Host::Ptr host(i->second);
    const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
    for (LoadBalancingPolicy::Vec::const_iterator j = policies.begin(),
         end = policies.end(); j != end; ++j) {
      if ((*j)->distance(host) != CASS_HOST_DISTANCE_IGNORE) {
        addresses.push_back(host->address());
        break;
      }
    }
  }

  if (addresses.empty()) {
    notify_connect_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                         "No hosts available for connection using the current load balancing policy");
    return;
  }

  ConnectionPoolManagerInitializer::Ptr initializer(Memory::allocate<ConnectionPoolManagerInitializer>(request_queue_manager_.get(),
                                                                                                       control_connection_.protocol_version(),
                                                                                                       this,
                                                                                                       on_initialize));
  initializer
      ->with_settings(ConnectionPoolManagerSettings(config_))
      ->with_listener(this)
      ->with_keyspace(connect_keyspace_)
      ->with_metrics(metrics_.get())
      ->initialize(addresses);

  // TODO: We really can't do this anymore
  if (config().core_connections_per_host() == 0) {
    // Special case for internal testing. Not allowed by API
    LOG_DEBUG("Session connected with no core IO connections");
  }
}

void Session::on_control_connection_error(CassError code, const String& message) {
  notify_connect_error(code, message);
}

Future::Ptr Session::prepare(const char* statement, size_t length) {
  PrepareRequest::Ptr prepare(Memory::allocate<PrepareRequest>(String(statement, length)));

  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>(metadata_.schema_snapshot(cassandra_version())));
  future->prepare_request = PrepareRequest::ConstPtr(prepare);

  execute(RequestHandler::Ptr(Memory::allocate<RequestHandler>(prepare, future, manager_.get(), metrics_.get(), this)));

  return future;
}

Future::Ptr Session::prepare(const Statement* statement) {
  String query;

  if (statement->opcode() == CQL_OPCODE_QUERY) { // Simple statement
    query = statement->query();
  } else { // Bound statement
    query = static_cast<const ExecuteRequest*>(statement)->prepared()->query();
  }

  PrepareRequest::Ptr prepare(Memory::allocate<PrepareRequest>(query));

  // Inherit the settings of the existing statement. These will in turn be
  // inherited by bound statements.
  prepare->set_settings(statement->settings());

  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>(metadata_.schema_snapshot(cassandra_version())));
  future->prepare_request = PrepareRequest::ConstPtr(prepare);

  execute(RequestHandler::Ptr(Memory::allocate<RequestHandler>(prepare, future, manager_.get(), metrics_.get(), this)));

  return future;
}

void Session::on_add(Host::Ptr host) {
  host->set_up(); // Set the host as up immediately (to avoid duplicate actions)

  if (!prepare_host(host, on_prepare_host_add)) {
    internal_on_add(host);
  }
}

void Session::internal_on_add(Host::Ptr host) {
  // Verify that the host is still available
  if (host->is_down()) return;

  bool is_host_ignored = true;
  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    if ((*it)->distance(host) != CASS_HOST_DISTANCE_IGNORE) {
      is_host_ignored = false;
      (*it)->on_add(host);
    }
  }
  if (is_host_ignored) {
    LOG_DEBUG("Host %s will be ignored by all query plans",
              host->address_string().c_str());
    return;
  }

  if (manager_) manager_->add(host->address());
}

void Session::on_remove(Host::Ptr host) {
  host->set_down();

  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    (*it)->on_remove(host);
  }
  { // Lock hosts
    ScopedMutex l(&hosts_mutex_);
    hosts_.erase(host->address());
  }

  if (manager_) manager_->remove(host->address());
}

void Session::on_up(Host::Ptr host) {
  host->set_up(); // Set the host as up immediately (to avoid duplicate actions)

  if (!prepare_host(host, on_prepare_host_up)) {
    internal_on_up(host);
  }
}

void Session::internal_on_up(Host::Ptr host) {
  // Verify that the host is still available
  if (host->is_down()) return;

  bool is_host_ignored = true;
  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    if ((*it)->distance(host) != CASS_HOST_DISTANCE_IGNORE) {
      is_host_ignored = false;
      (*it)->on_up(host);
    }
  }
  if (is_host_ignored) {
    LOG_DEBUG("Host %s will be ignored by all query plans",
              host->address_string().c_str());
    return;
  }

  if (manager_) manager_->add(host->address());
}

void Session::on_down(Host::Ptr host) {
  host->set_down();

  bool is_host_ignored = true;
  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin();
       it != policies.end(); ++it) {
    (*it)->on_down(host);
    if ((*it)->distance(host) != CASS_HOST_DISTANCE_IGNORE) {
      is_host_ignored = false;
    }
  }

  if (is_host_ignored) {
    // This permanently removes a host from all IO workers by stopping
    // any attempt to reconnect to that host if all load balancing policies
    // ignore the host distance.
    LOG_DEBUG("Host %s will be ignored by all query plans",
              host->address_string().c_str());
    if (manager_) manager_->remove(host->address());
  }
}

void Session::on_initialize(ConnectionPoolManagerInitializer* initializer) {
  Session* session = static_cast<Session*>(initializer->data());
  session->handle_initialize(initializer);
}

void Session::handle_initialize(ConnectionPoolManagerInitializer* initializer) {
  // Not much can be done in here because this is running on a thread pool thread.
  notify_initialize_async(initializer->release_manager(), initializer->failures());
}

void Session::on_result_metadata_changed(const String& prepared_id,
                                         const String& query,
                                         const String& keyspace,
                                         const String& result_metadata_id,
                                         const ResultResponse::ConstPtr& result_response) {
  PreparedMetadata::Entry::Ptr entry(
        Memory::allocate<PreparedMetadata::Entry>(query,
                                                  keyspace,
                                                  result_metadata_id,
                                                  result_response));
  prepared_metadata_.set(prepared_id, entry);
}

void Session::on_keyspace_changed(const String& keyspace) {
  manager_->set_keyspace(keyspace);
}

bool Session::on_wait_for_schema_agreement(const RequestHandler::Ptr& request_handler,
                                           const Host::Ptr& current_host,
                                           const Response::Ptr& response) {
  SchemaAgreementHandler::Ptr handler(
        Memory::allocate<SchemaAgreementHandler>(
          request_handler, current_host, response,
          this, config_.max_schema_wait_time_ms()));

  PooledConnection::Ptr connection(manager_->find_least_busy(current_host->address()));
  if (connection && connection->write(handler->callback().get())) {
    return true;
  }

  return false;
}

bool Session::on_prepare_all(const RequestHandler::Ptr& request_handler,
                             const Host::Ptr& current_host,
                             const Response::Ptr& response) {
  if (!config_.prepare_on_all_hosts()) {
    return false;
  }

  AddressVec addresses = manager_->available();
  if (addresses.empty() ||
      (addresses.size() == 1 && addresses[0] == current_host->address())) {
    return false;
  }

  PrepareAllHandler::Ptr prepare_all_handler(
        new PrepareAllHandler(current_host,
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
    PrepareAllCallback::Ptr prepare_all_callback(
          new PrepareAllCallback(address, prepare_all_handler));

    PooledConnection::Ptr connection(manager_->find_least_busy(address));
    if (connection) {
      connection->write(prepare_all_callback.get());
    }
  }

  return true;
}

void Session::on_up(const Address& address) {
  notify_up_async(address);
}

void Session::on_down(const Address& address) {
  notify_down_async(address);
}

void Session::on_critical_error(const Address& address,
                                Connector::ConnectionError code,
                                const String& message) {
  notify_down_async(address);
}

bool Session::on_is_host_up(const Address& address) {
  Host::Ptr host(get_host(address));
  return host && host->is_up();
}

Future::Ptr Session::execute(const Request::ConstPtr& request,
                             const Address* preferred_address) {
  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>());

  execute(RequestHandler::Ptr(
            Memory::allocate<RequestHandler>(request, future, manager_.get(), metrics_.get(), this, preferred_address)));

  return future;
}

void Session::on_execute(Async* async) {
  Session* session = static_cast<Session*>(async->data());

  bool is_closing = false;

  RequestHandler* request_handler = NULL;
  while (session->request_queue_->dequeue(request_handler)) {
    if (request_handler) {
      const String& profile_name = request_handler->request()->execution_profile_name();
      ExecutionProfile profile;
      if (session->config().profile(profile_name, profile)) {
        if (!profile_name.empty()) {
          LOG_TRACE("Using execution profile '%s'", profile_name.c_str());
        }
        request_handler->init(session->config_,
                              profile,
                              session->manager_->keyspace(),
                              session->token_map_.get(),
                              session->prepared_metadata_);
        request_handler->execute();
      } else {
        request_handler->set_error(CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID,
                                   profile_name + " does not exist");
      }
      request_handler->dec_ref();
    } else {
      is_closing = true;
    }
  }

  if (is_closing) {
    if (session->manager_) {
      session->manager_->close();
    }
    session->control_connection_.close();
    session->close_handles();
  }
}

QueryPlan* Session::new_query_plan() {
  return config_.default_profile().load_balancing_policy()->new_query_plan("",
                                                                           NULL,
                                                                           token_map_.get());
}

} // namespace cass
