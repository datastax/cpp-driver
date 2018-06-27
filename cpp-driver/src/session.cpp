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
#include "constants.hpp"
#include "execute_request.hpp"
#include "external.hpp"
#include "logger.hpp"
#include "metrics.hpp"
#include "prepare_all_handler.hpp"
#include "prepare_request.hpp"
#include "request_processor_manager_initializer.hpp"
#include "scoped_lock.hpp"
#include "statement.hpp"

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

Session::~Session() {
  close_event_loop_group();
}

Future::Ptr Session::prepare(const char* statement, size_t length) {
  PrepareRequest::Ptr prepare(Memory::allocate<PrepareRequest>(String(statement, length)));

  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>(cluster()->schema_snapshot()));
  future->prepare_request = PrepareRequest::ConstPtr(prepare);

  execute(RequestHandler::Ptr(
            Memory::allocate<RequestHandler>(prepare, future, metrics_.get())));

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

  execute(RequestHandler::Ptr(
            Memory::allocate<RequestHandler>(prepare, future, metrics_.get())));

  return future;
}

void Session::close_event_loop_group() {
  if (event_loop_group_) {
    event_loop_group_->close_handles();
    event_loop_group_->join();
  }
}

Future::Ptr Session::execute(const Request::ConstPtr& request, const Address* preferred_address) {
  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>());

  RequestHandler::Ptr request_handler(
            Memory::allocate<RequestHandler>(request, future,
                                             metrics_.get(), preferred_address));


  if (request_handler->request()->opcode() == CQL_OPCODE_EXECUTE) {
    const ExecuteRequest* execute = static_cast<const ExecuteRequest*>(request_handler->request());
    request_handler->set_prepared_metadata(cluster()->prepared(execute->prepared()->id()));
  }

  execute(request_handler);

  return future;
}

void Session::execute(const RequestHandler::Ptr& request_handler) {
  if (state() != SESSION_STATE_CONNECTED) {
    request_handler->set_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                               "Session is not connected");
    return;
  }

  request_processor_manager_->process_request(request_handler);
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

  close_event_loop_group();
  event_loop_group_.reset(Memory::allocate<RoundRobinEventLoopGroup>(config().thread_count_io()));
  rc = event_loop_group_->init("Request Processor");
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

  metrics_.reset(Memory::allocate<Metrics>(config().thread_count_io() + 1));

  RequestProcessorManagerInitializer::Ptr initializer(
        Memory::allocate<RequestProcessorManagerInitializer>(connected_host,
                                                             protocol_version,
                                                             hosts,
                                                             bind_callback(&Session::on_initialize, this)));

  initializer
      ->with_settings(RequestProcessorSettings(config()))
      ->with_keyspace(connect_keyspace())
      ->with_listener(this)
      ->with_metrics(metrics_.get())
      ->with_random(random())
      ->with_token_map(token_map)
      ->initialize(event_loop_group_.get());
}

void Session::on_up(const Host::Ptr& host) {
}

void Session::on_down(const Host::Ptr& host) {
}

void Session::on_add(const Host::Ptr& host) {
  request_processor_manager_->notify_host_add(host);
}

void Session::on_remove(const Host::Ptr& host)  {
  request_processor_manager_->notify_host_remove(host);
}

void Session::on_update_token_map(const TokenMap::Ptr& token_map) {
  request_processor_manager_->notify_token_map_changed(token_map);
}

void Session::on_close(Cluster* cluster) {
  if (request_processor_manager_) {
    request_processor_manager_->close();
  }
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

void Session::on_keyspace_changed(const String& keyspace) {
}

void Session::on_prepared_metadata_changed(const String& id,
                                           const PreparedMetadata::Entry::Ptr& entry) {
  cluster()->prepared(id, entry);
}

void Session::on_close(RequestProcessorManager* manager) {
  notify_closed();
}

void Session::on_initialize(RequestProcessorManagerInitializer* initializer) {
  const RequestProcessorInitializer::Vec& failures = initializer->failures();

  if (!failures.empty()) {
    // All failures are likely to be the same, just pass the first error.
    RequestProcessorInitializer::Ptr failure(failures.front());

    CassError error_code;
    switch (failure->error_code()) {
      case RequestProcessorInitializer::REQUEST_PROCESSOR_ERROR_KEYSPACE:
        error_code = CASS_ERROR_LIB_NO_HOSTS_AVAILABLE;
        break;
      case RequestProcessorInitializer::REQUEST_PROCESSOR_ERROR_NO_HOSTS_AVAILABLE:
        error_code = CASS_ERROR_LIB_NO_HOSTS_AVAILABLE;
        break;
      case RequestProcessorInitializer::REQUEST_PROCESSOR_ERROR_UNABLE_TO_INIT_ASYNC:
        error_code = CASS_ERROR_LIB_UNABLE_TO_INIT;
        break;
      default:
        error_code = CASS_ERROR_LIB_INTERNAL_ERROR;
        break;
    }

    notify_connect_failed(error_code, failure->error_message());
  } else {
    request_processor_manager_ = initializer->release_manager();
    notify_connected();
  }
}

} // namespace cass
