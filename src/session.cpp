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
#include "request_processor_initializer.hpp"
#include "scoped_lock.hpp"
#include "statement.hpp"

extern "C" {

CassSession* cass_session_new() {
  cass::Session* session = cass::Memory::allocate<cass::Session>();
  return CassSession::to(session);
}

void cass_session_free(CassSession* session) {
  // This attempts to close the session because the joining will
  // hang indefinitely otherwise. This causes minimal delay
  // if the session is already closed.
  cass::SharedRefPtr<cass::Future> future(cass::Memory::allocate<cass::SessionFuture>());
  session->close(future);
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

void cass_session_get_metrics(const CassSession* session,
                               CassMetrics* metrics) {
  const cass::Metrics* internal_metrics = session->metrics();

  if (internal_metrics == NULL)  {
    LOG_WARN("Attempted to get metrics before connecting session object");
    memset(metrics, 0, sizeof(CassMetrics));
    return;
  }

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

  if (internal_metrics == NULL)  {
    LOG_WARN("Attempted to get speculative execution metrics before connecting session object");
    memset(metrics, 0, sizeof(CassSpeculativeExecutionMetrics));
    return;
  }

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

static inline bool least_busy_comp(const RequestProcessor::Ptr& a,
                                   const RequestProcessor::Ptr& b) {
  return a->request_count() < b->request_count();
}

/**
 * An initialize helper class for `Session`. This keeps the initialization
 * logic and data out of the core class itself.
 */
class SessionInitializer : public RefCounted<SessionInitializer> {
public:
  typedef SharedRefPtr<SessionInitializer> Ptr;

  SessionInitializer(Session* session)
    : session_(session)
    , remaining_(0)
    , error_code_(CASS_OK) {
    uv_mutex_init(&mutex_);
  }

  SessionInitializer() {
    uv_mutex_destroy(&mutex_);
  }

  void initialize(const Host::Ptr& connected_host,
                  int protocol_version,
                  const HostMap& hosts,
                  const TokenMap::Ptr& token_map) {
    inc_ref();
    const size_t thread_count_io = remaining_ = session_->config().thread_count_io();
    for (size_t i = 0; i < thread_count_io; ++i) {
      RequestProcessorInitializer::Ptr initializer(
            Memory::allocate<RequestProcessorInitializer>(connected_host,
                                                          protocol_version,
                                                          hosts,
                                                          token_map,
                                                          bind_callback(&SessionInitializer::on_initialize, this)));
      initializer
          ->with_settings(RequestProcessorSettings(session_->config()))
          ->with_listener(session_)
          ->with_keyspace(session_->connect_keyspace())
          ->with_metrics(session_->metrics())
          ->with_random(session_->random())
          ->initialize(session_->event_loop_group_->get(i));
    }
  }

private:
  void on_initialize(RequestProcessorInitializer* initializer) {
    // A lock is required because request processors are initialized on
    // different threads .
    ScopedMutex l(&mutex_);

    if (initializer->is_ok()) {
      request_processors_.push_back(initializer->release_processor());
    } else {
      switch (initializer->error_code()) {
        case RequestProcessorInitializer::REQUEST_PROCESSOR_ERROR_KEYSPACE:
          error_code_ = CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE;
          break;
        case RequestProcessorInitializer::REQUEST_PROCESSOR_ERROR_NO_HOSTS_AVAILABLE:
          error_code_ = CASS_ERROR_LIB_NO_HOSTS_AVAILABLE;
          break;
        case RequestProcessorInitializer::REQUEST_PROCESSOR_ERROR_UNABLE_TO_INIT:
          error_code_ = CASS_ERROR_LIB_UNABLE_TO_INIT;
          break;
        default:
          error_code_ = CASS_ERROR_LIB_INTERNAL_ERROR;
          break;
      }
      error_message_ = initializer->error_message();
    }

    if (--remaining_ == 0) {
      { // This requires locking because cluster events can happen during
        // initialization.
        ScopedMutex l(&session_->request_processor_mutex_);
        session_->request_processor_count_ = request_processors_.size();
        session_->request_processors_ = request_processors_;
      }
      if (error_code_ != CASS_OK) {
        session_->notify_connect_failed(error_code_, error_message_);
      } else {
        session_->notify_connected();
      }
      l.unlock(); // Unlock before destroying the object
      dec_ref();
    }
  }

private:
  uv_mutex_t mutex_;
  Session* session_;
  size_t remaining_;
  CassError error_code_;
  String error_message_;
  RequestProcessor::Vec request_processors_;
};

Session::Session()
  : request_processor_count_(0) {
  uv_mutex_init(&request_processor_mutex_);
}

Session::~Session() {
  join();
  uv_mutex_destroy(&request_processor_mutex_);
}

Future::Ptr Session::prepare(const char* statement, size_t length) {
  PrepareRequest::Ptr prepare(Memory::allocate<PrepareRequest>(String(statement, length)));

  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>(cluster()->schema_snapshot()));
  future->prepare_request = PrepareRequest::ConstPtr(prepare);

  execute(RequestHandler::Ptr(
            Memory::allocate<RequestHandler>(prepare, future, metrics())));

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
            Memory::allocate<RequestHandler>(prepare, future, metrics())));

  return future;
}

Future::Ptr Session::execute(const Request::ConstPtr& request, const Address* preferred_address) {
  ResponseFuture::Ptr future(Memory::allocate<ResponseFuture>());

  RequestHandler::Ptr request_handler(
            Memory::allocate<RequestHandler>(request, future,
                                             metrics(), preferred_address));


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


  // This intentionally doesn't lock the request processors. The processors will
  // be populated before the connect future returns and calling execute during
  // the connection process is undefined behavior. Locking would cause unnecessary
  // overhead for something that's constant once the session is connected.
  const RequestProcessor::Ptr& request_processor
      =  *std::min_element(request_processors_.begin(), request_processors_.end(),
                           least_busy_comp);
  request_processor->process_request(request_handler);
}

void Session::join() {
  if (event_loop_group_) {
    event_loop_group_->close_handles();
    event_loop_group_->join();
    event_loop_group_.reset();
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

  join();
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

  request_processors_.clear();
  request_processor_count_ = 0;
  SessionInitializer::Ptr initializer(Memory::allocate<SessionInitializer>(this));
  initializer->initialize(connected_host,
                          protocol_version,
                          hosts,
                          token_map);
}

void Session::on_close() {
  // If there are request processors still connected those need to be closed
  // first before sending the close notification.
  ScopedMutex l(&request_processor_mutex_);
  if (request_processor_count_ > 0) {
    for (RequestProcessor::Vec::const_iterator it = request_processors_.begin(),
         end = request_processors_.end(); it != end; ++it) {
      (*it)->close();
    }
  } else {
    notify_closed();
  }
}

void Session::on_up(const Host::Ptr& host) {
  // Ignore up events from the control connection. The connection pools will
  // reconnect themselves when the host becomes available.
}

void Session::on_down(const Host::Ptr& host) {
  // Ignore down events from the control connection. The connection pools can
  // determine if a host is down themselves. The control connection host
  // can become partitioned from the rest of the cluster and in that scenario a
  // down event from the control connection would be invalid.
}

void Session::on_add(const Host::Ptr& host) {
  ScopedMutex l(&request_processor_mutex_);
  for (RequestProcessor::Vec::const_iterator it = request_processors_.begin(),
       end = request_processors_.end(); it != end; ++it) {
    (*it)->notify_host_add(host);
  }
}

void Session::on_remove(const Host::Ptr& host)  {
  ScopedMutex l(&request_processor_mutex_);
  for (RequestProcessor::Vec::const_iterator it = request_processors_.begin(),
       end = request_processors_.end(); it != end; ++it) {
    (*it)->notify_host_remove(host);
  }
}

void Session::on_update_token_map(const TokenMap::Ptr& token_map) {
  ScopedMutex l(&request_processor_mutex_);
  for (RequestProcessor::Vec::const_iterator it = request_processors_.begin(),
       end = request_processors_.end(); it != end; ++it) {
    (*it)->notify_token_map_changed(token_map);
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

void Session::on_keyspace_changed(const String& keyspace,
                                  const KeyspaceChangedHandler::Ptr& handler) {
  ScopedMutex l(&request_processor_mutex_);
  for (RequestProcessor::Vec::const_iterator it = request_processors_.begin(),
       end = request_processors_.end(); it != end; ++it) {
    (*it)->set_keyspace(keyspace, handler);
  }
}

void Session::on_prepared_metadata_changed(const String& id,
                                           const PreparedMetadata::Entry::Ptr& entry) {
  cluster()->prepared(id, entry);
}

void Session::on_close(RequestProcessor* processor) {
  // Requires a lock because the close callback is called from several
  // different request processor threads.
  ScopedMutex l(&request_processor_mutex_);
  if (--request_processor_count_ == 0) {
    notify_closed();
  }
}

} // namespace cass
