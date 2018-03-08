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
#include "cluster_config.hpp"
#include "connection_pool_manager_initializer.hpp"
#include "constants.hpp"
#include "execute_request.hpp"
#include "external.hpp"
#include "logger.hpp"
#include "metrics.hpp"
#include "prepare_all_handler.hpp"
#include "prepare_request.hpp"
#include "scoped_lock.hpp"
#include "statement.hpp"
#include "timer.hpp"

extern "C" {

CassSession* cass_session_new() {
  cass::Session* session = cass::Memory::allocate<cass::Session>();
  session->inc_ref();
  return CassSession::to(session);
}

void cass_session_free(CassSession* session) { // This attempts to close the session because the joining will
  // hang indefinitely otherwise. This causes minimal delay
  // if the session is already closed.
  cass::SharedRefPtr<cass::Future> future(cass::Memory::allocate<cass::SessionFuture>());
  session->close(future);
  future->wait();

  session->dec_ref();
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
  session->connect(cluster->config(), cass::String(keyspace, keyspace_length), connect_future);
  connect_future->inc_ref();
  return CassFuture::to(connect_future.get());
}

CassFuture* cass_session_close(CassSession* session) {
  cass::SessionFuture::Ptr close_future(cass::Memory::allocate<cass::SessionFuture>());
  session->close(close_future);
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
  return CassSchemaMeta::to(
        cass::Memory::allocate<cass::Metadata::SchemaSnapshot>(
          session->cluster()->schema_snapshot()));
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

Session::Session() {
  uv_rwlock_init(&policy_rwlock_);
}

Session::~Session() {
  if (request_queue_manager_) {
    request_queue_manager_->close_handles();
  }
  if (event_loop_group_) {
    event_loop_group_->close_handles();
    event_loop_group_->join();
  }
  uv_rwlock_destroy(&policy_rwlock_);
}

Future::Ptr Session::prepare(const char* statement, size_t length) {
  PrepareRequest::Ptr prepare(Memory::allocate<PrepareRequest>(String(statement, length)));

  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>(cluster()->schema_snapshot()));
  future->prepare_request = PrepareRequest::ConstPtr(prepare);

  execute(RequestHandler::Ptr(Memory::allocate<RequestHandler>(prepare, future,
                                                               connection_pool_manager_.get(), metrics_.get(),
                                                               this)));

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

  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>(cluster()->schema_snapshot()));
  future->prepare_request = PrepareRequest::ConstPtr(prepare);

  execute(RequestHandler::Ptr(Memory::allocate<RequestHandler>(prepare, future,
                                                               connection_pool_manager_.get(), metrics_.get(),
                                                               this)));

  return future;
}

Future::Ptr Session::execute(const Request::ConstPtr& request, const Address* preferred_address) {
  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>());

  execute(RequestHandler::Ptr(Memory::allocate<RequestHandler>(request, future,
                                                               connection_pool_manager_.get(), metrics_.get(),
                                                               this, preferred_address)));

  return future;
}

void Session::execute(const RequestHandler::Ptr& request_handler) {
  const String& profile_name = request_handler->request()->execution_profile_name();
  ExecutionProfile profile;
  if (config().profile(profile_name, profile)) {
    if (!profile_name.empty()) {
      LOG_TRACE("Using execution profile '%s'", profile_name.c_str());
    }

    QueryPlan* query_plan = NULL;
    SpeculativeExecutionPlan* execution_plan = NULL;
    {
      ScopedReadLock rl(&policy_rwlock_);
      query_plan = profile.load_balancing_policy()->new_query_plan(keyspace_, request_handler.get(), token_map_.get());
      execution_plan = config().speculative_execution_policy()->new_plan(keyspace_, request_handler->request());
    }

    PreparedMetadata::Entry::Ptr prepared_metadata_entry;
    if (request_handler->request()->opcode() == CQL_OPCODE_EXECUTE) {
      const ExecuteRequest* execute = static_cast<const ExecuteRequest*>(request_handler->request());
      prepared_metadata_entry = cluster()->prepared(execute->prepared()->id());
    }

    request_handler->init(config(),
                          profile,
                          prepared_metadata_entry,
                          query_plan,
                          execution_plan);

    request_handler->execute();
  } else {
    request_handler->set_error(CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID,
                               profile_name + " does not exist");
  }
}

void Session::on_connect(const Host::Ptr& connected_host,
                         int protocol_version,
                         const HostMap& hosts,
                         const TokenMap::Ptr& token_map) {
  int rc = 0;

  if (hosts.empty()) {
    notify_connect_failed(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                          "No hosts provided or no hosts resolved");
    return;
  }

  event_loop_group_.reset(Memory::allocate<RoundRobinEventLoopGroup>(config().thread_count_io()));
  rc = event_loop_group_->init();
  if (rc != 0) {
    notify_connect_failed(CASS_ERROR_LIB_UNABLE_TO_INIT,
                          "Unable to initialize event loop group");
    return;
  }

  rc = event_loop_group_->run();
  if (rc != 0) {
    notify_connect_failed(CASS_ERROR_LIB_UNABLE_TO_INIT,
                          "Unable to run event loop group");
    return;
  }

  request_queue_manager_.reset(Memory::allocate<RequestQueueManager>(event_loop_group_.get()));
  rc = request_queue_manager_->init(config().queue_size_io());
  if (rc != 0) {
    notify_connect_failed(CASS_ERROR_LIB_UNABLE_TO_INIT,
                          "Unable to run event loop group threads");
    return;
  }

  {
    ScopedWriteLock wl(&policy_rwlock_);
    const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
    for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(),
         end = policies.end(); it != end; ++it) {
      (*it)->init(connected_host, hosts, random());
    }
  }

  connection_pool_manager_initializer_.reset(Memory::allocate<ConnectionPoolManagerInitializer>(request_queue_manager_.get(),
                                                                                                protocol_version,
                                                                                                this,
                                                                                                on_initialize));

  metrics_.reset(Memory::allocate<Metrics>(config().thread_count_io() + 1));

  addresses_.clear();
  for (HostMap::const_iterator it = hosts.begin(),
       end = hosts.end(); it != end; ++it) {
    addresses_.insert(it->first);
  }

  connection_pool_manager_initializer_
      ->with_settings(ConnectionPoolManagerSettings(config()))
      ->with_listener(this)
      ->with_metrics(metrics_.get())
      ->with_keyspace(connect_keyspace())
      ->initialize(AddressVec(addresses_.begin(), addresses_.end()));
}

void Session::on_result_metadata_changed(const String& prepared_id,
                                         const String& query,
                                         const String& keyspace,
                                         const String& result_metadata_id,
                                         const ResultResponse::ConstPtr& result_response) {
  cluster()->prepared(prepared_id, query, keyspace, result_metadata_id, result_response);
}

void Session::on_keyspace_changed(const String& keyspace) {
  connection_pool_manager_->set_keyspace(keyspace);
  ScopedWriteLock wl(&policy_rwlock_);
  keyspace_ = keyspace;
}

bool Session::on_wait_for_schema_agreement(const RequestHandler::Ptr& request_handler,
                                           const Host::Ptr& current_host,
                                           const Response::Ptr& response)  {
  SchemaAgreementHandler::Ptr handler(
        Memory::allocate<SchemaAgreementHandler>(
          request_handler, current_host, response,
          this, config().max_schema_wait_time_ms()));

  PooledConnection::Ptr connection(connection_pool_manager_->find_least_busy(current_host->address()));
  if (connection && connection->write(handler->callback().get())) {
    return true;
  }

  return false;
}

bool Session::on_prepare_all(const RequestHandler::Ptr& request_handler,
                             const Host::Ptr& current_host,
                             const Response::Ptr& response)  {
  if (!config().prepare_on_all_hosts()) {
    return false;
  }

  AddressVec addresses = connection_pool_manager_->available();
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

    PooledConnection::Ptr connection(connection_pool_manager_->find_least_busy(address));
    if (connection) {
      connection->write(prepare_all_callback.get());
    }
  }

  return true;
}

bool Session::on_is_host_up(const Address& address) {
  return cluster()->host(address)->is_up();
}

void Session::on_up(const Host::Ptr& host) {
  ScopedWriteLock wl(&policy_rwlock_);
  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(),
       end = policies.end(); it != end; ++it) {
    (*it)->on_up(host);
  }
}

void Session::on_down(const Host::Ptr& host) {
  ScopedWriteLock wl(&policy_rwlock_);
  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(),
       end = policies.end(); it != end; ++it) {
    (*it)->on_down(host);
  }
}

void Session::on_add(const Host::Ptr& host) {
  connection_pool_manager_->add(host->address());
  ScopedWriteLock wl(&policy_rwlock_);
  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(),
       end = policies.end(); it != end; ++it) {
    (*it)->on_add(host);
  }
}

void Session::on_remove(const Host::Ptr& host)  {
  connection_pool_manager_->remove(host->address());
  ScopedWriteLock wl(&policy_rwlock_);
  const LoadBalancingPolicy::Vec& policies = config().load_balancing_policies();
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(),
       end = policies.end(); it != end; ++it) {
    (*it)->on_remove(host);
  }
}

void Session::on_update_token_map(const TokenMap::Ptr& token_map) {
  ScopedWriteLock wl(&policy_rwlock_);
  token_map_ = token_map;
}

void Session::on_close(Cluster* cluster) {
  if (connection_pool_manager_initializer_) {
    connection_pool_manager_initializer_->cancel();
  }
  if (connection_pool_manager_) {
    connection_pool_manager_->close();
  }
}

void Session::on_initialize(ConnectionPoolManagerInitializer* initializer) {
  Session* session = static_cast<Session*>(initializer->data());
  session->handle_initialize(initializer);
}

void Session::handle_initialize(ConnectionPoolManagerInitializer* initializer) {
  bool is_keyspace_error = false;

  // Prune hosts
  const ConnectionPoolConnector::Vec& failures = initializer->failures();
  for (ConnectionPoolConnector::Vec::const_iterator it = failures.begin(),
       end = failures.end(); it != end; ++it) {
    ConnectionPoolConnector::Ptr connector(*it);
    if (connector->is_keyspace_error()) {
      is_keyspace_error = true;
      break;
    } else {
      cluster()->notify_down(connector->address());
      addresses_.erase(connector->address());
    }
  }

  // Handle errors and set hosts as up
  if (is_keyspace_error) {
    notify_connect_failed(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                          "Keyspace '" + connect_keyspace() + "' does not exist");
  } else if (addresses_.empty()) {
    notify_connect_failed(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                          "Unable to connect to any hosts");
  } else {
    for (AddressSet::const_iterator it = addresses_.begin(),
         end = addresses_.end(); it != end; ++it) {
      cluster()->notify_up(*it);
    }
  }

  connection_pool_manager_ = initializer->release_manager();

  connection_pool_manager_initializer_.reset();
  addresses_.clear();
  notify_connected();
}

void Session::on_pool_up(const Address& address) {
  cluster()->notify_up(address);
}

void Session::on_pool_down(const Address& address) {
  cluster()->notify_down(address);
}

void Session::on_pool_critical_error(const Address& address,
                                Connector::ConnectionError code,
                                const String& message) {
  cluster()->notify_down(address);
}

void Session::on_close(ConnectionPoolManager* manager) {
  notify_closed();
}

} // namespace cass
