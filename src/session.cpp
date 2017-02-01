/*
  Copyright (c) 2014-2016 DataStax

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
#include "constants.hpp"
#include "logger.hpp"
#include "prepare_request.hpp"
#include "scoped_lock.hpp"
#include "statement.hpp"
#include "timer.hpp"
#include "external.hpp"

extern "C" {

CassSession* cass_session_new() {
  return CassSession::to(new cass::Session());
}

void cass_session_free(CassSession* session) {
  // This attempts to close the session because the joining will
  // hang indefinitely otherwise. This causes minimal delay
  // if the session is already closed.
  cass::SharedRefPtr<cass::Future> future(new cass::SessionFuture());
  session->close_async(future);
  future->wait();

  delete session->from();
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
                                         strlen(keyspace));
}

CassFuture* cass_session_connect_keyspace_n(CassSession* session,
                                            const CassCluster* cluster,
                                            const char* keyspace,
                                            size_t keyspace_length) {
  cass::SessionFuture::Ptr connect_future(new cass::SessionFuture());
  session->connect_async(cluster->config(), std::string(keyspace, keyspace_length), connect_future);
  connect_future->inc_ref();
  return CassFuture::to(connect_future.get());
}

CassFuture* cass_session_close(CassSession* session) {
  cass::SessionFuture::Ptr close_future(new cass::SessionFuture());
  session->close_async(close_future);
  close_future->inc_ref();
  return CassFuture::to(close_future.get());
}

CassFuture* cass_session_prepare(CassSession* session, const char* query) {
  return cass_session_prepare_n(session, query, strlen(query));
}

CassFuture* cass_session_prepare_n(CassSession* session,
                                   const char* query,
                                   size_t query_length) {
  cass::Future::Ptr future(session->prepare(query, query_length));
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
  return CassSchemaMeta::to(new cass::Metadata::SchemaSnapshot(session->metadata().schema_snapshot(session->protocol_version(), session->cassandra_version())));
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
  metrics->stats.available_connections = internal_metrics->available_connections.sum();
  metrics->stats.exceeded_write_bytes_water_mark = internal_metrics->exceeded_write_bytes_water_mark.sum();
  metrics->stats.exceeded_pending_requests_water_mark = internal_metrics->exceeded_pending_requests_water_mark.sum();

  metrics->errors.connection_timeouts = internal_metrics->connection_timeouts.sum();
  metrics->errors.pending_request_timeouts = internal_metrics->pending_request_timeouts.sum();
  metrics->errors.request_timeouts = internal_metrics->request_timeouts.sum();
}

} // extern "C"

namespace cass {

Session::Session()
    : state_(SESSION_STATE_CLOSED)
    , connect_error_code_(CASS_OK)
    , current_host_mark_(true)
    , pending_pool_count_(0)
    , pending_workers_count_(0)
    , current_io_worker_(0)
    , keyspace_(new std::string){
  uv_mutex_init(&state_mutex_);
  uv_mutex_init(&hosts_mutex_);
}

Session::~Session() {
  join();
  uv_mutex_destroy(&state_mutex_);
  uv_mutex_destroy(&hosts_mutex_);
}

void Session::clear(const Config& config) {
  config_ = config;
  random_.reset();
  metrics_.reset(new Metrics(config_.thread_count_io() + 1));
  load_balancing_policy_.reset(config.load_balancing_policy());
  speculative_execution_policy_.reset(config.speculative_execution_policy());
  connect_future_.reset();
  close_future_.reset();
  { // Lock hosts
    ScopedMutex l(&hosts_mutex_);
    hosts_.clear();
  }
  io_workers_.clear();
  request_queue_.reset();
  metadata_.clear();
  control_connection_.clear();
  connect_error_code_ = CASS_OK;
  connect_error_message_.clear();
  current_host_mark_ = true;
  pending_pool_count_ = 0;
  pending_workers_count_ = 0;
  current_io_worker_ = 0;
}

int Session::init() {
  int rc = EventThread<SessionEvent>::init(config_.queue_size_event());
  if (rc != 0) return rc;
  request_queue_.reset(
      new AsyncQueue<MPMCQueue<RequestHandler*> >(config_.queue_size_io()));
  rc = request_queue_->init(loop(), this, &Session::on_execute);
  if (rc != 0) return rc;

  for (unsigned int i = 0; i < config_.thread_count_io(); ++i) {
    IOWorker::Ptr io_worker(new IOWorker(this));
    int rc = io_worker->init();
    if (rc != 0) return rc;
    io_workers_.push_back(io_worker);
  }

  return rc;
}

void Session::broadcast_keyspace_change(const std::string& keyspace,
                                        const IOWorker* calling_io_worker) {
  // This can run on an IO worker thread. This is thread-safe because the IO workers
  // vector never changes after initialization and IOWorker::set_keyspace() uses
  // copy-on-write. This also means that calling "USE <keyspace>" frequently is an
  // anti-pattern.
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    if (*it == calling_io_worker) continue;
      (*it)->set_keyspace(keyspace);
  }
  keyspace_ = CopyOnWritePtr<std::string>(new std::string(keyspace));
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
  Host::Ptr host(new Host(address, !current_host_mark_));
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

      std::string address_str = to_remove_it->first.to_string();
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

bool Session::notify_ready_async() {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_READY;
  return send_event_async(event);
}

bool Session::notify_keyspace_error_async() {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_KEYSPACE_ERROR;
  return send_event_async(event);
}

bool Session::notify_worker_closed_async() {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_WORKER_CLOSED;
  return send_event_async(event);
}

bool Session::notify_up_async(const Address& address) {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_UP;
  event.address = address;
  return send_event_async(event);
}

bool Session::notify_down_async(const Address& address) {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_DOWN;
  event.address = address;
  return send_event_async(event);
}

void Session::connect_async(const Config& config, const std::string& keyspace, const Future::Ptr& future) {
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

  SessionEvent event;
  event.type = SessionEvent::CONNECT;

  if (!send_event_async(event)) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                      "Unable to enqueue connected event");
    return;
  }

  LOG_DEBUG("Issued connect event");

  state_.store(SESSION_STATE_CONNECTING, MEMORY_ORDER_RELAXED);
  connect_future_ = future;

  if (!keyspace.empty()) {
    broadcast_keyspace_change(keyspace, NULL);
  }

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
  ScopedMutex l(&state_mutex_);

  if (state_.load(MEMORY_ORDER_RELAXED) == SESSION_STATE_CONNECTING) {
    state_.store(SESSION_STATE_CONNECTED, MEMORY_ORDER_RELAXED);
  }
  if (connect_future_) {
    connect_future_->set();
    connect_future_.reset();
  }
}

void Session::notify_connect_error(CassError code, const std::string& message) {
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
  EventThread<SessionEvent>::close_handles();
  request_queue_->close_handles();
  load_balancing_policy_->close_handles();
}

void Session::on_run() {
  LOG_DEBUG("Creating %u IO worker threads",
            static_cast<unsigned int>(io_workers_.size()));

  for (IOWorkerVec::iterator it = io_workers_.begin(), end = io_workers_.end();
       it != end; ++it) {
    (*it)->run();
  }
}

void Session::on_after_run() {
  for (IOWorkerVec::iterator it = io_workers_.begin(), end = io_workers_.end();
       it != end; ++it) {
    (*it)->join();
  }
  notify_closed();
}

void Session::on_event(const SessionEvent& event) {
  switch (event.type) {
    case SessionEvent::CONNECT: {
      int port = config_.port();

      // This needs to be done on the session thread because it could pause
      // generating a new random seed.
      if (config_.use_randomized_contact_points()) {
        random_.reset(new Random());
      }

      MultiResolver<Session*>::Ptr resolver(
            new MultiResolver<Session*>(this, on_resolve,
#if UV_VERSION_MAJOR >= 1
                                        on_resolve_name,
#endif
                                        on_resolve_done));

      const ContactPointList& contact_points = config_.contact_points();
      for (ContactPointList::const_iterator it = contact_points.begin(),
                                                    end = contact_points.end();
           it != end; ++it) {
        const std::string& seed = *it;
        Address address;
        if (Address::from_string(seed, port, &address)) {
#if UV_VERSION_MAJOR >= 1
          if (config_.use_hostname_resolution()) {
            resolver->resolve_name(loop(), address, config_.resolve_timeout_ms());
          } else {
#endif
            add_host(address);
#if UV_VERSION_MAJOR >= 1
          }
#endif
        } else {
          resolver->resolve(loop(), seed, port, config_.resolve_timeout_ms());
        }
      }

      break;
    }

    case SessionEvent::NOTIFY_READY:
      if (pending_pool_count_ > 0) {
        if (--pending_pool_count_ == 0) {
          LOG_DEBUG("Session is connected");
          notify_connected();
        }
        LOG_DEBUG("Session pending pool count %d", pending_pool_count_);
      }
      break;

    case SessionEvent::NOTIFY_KEYSPACE_ERROR: {
      // Currently, this is only called when the keyspace does not exist
      // and not for any other keyspace related errors.
      const CopyOnWritePtr<std::string> keyspace(keyspace_);
      notify_connect_error(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE,
                           "Keyspace '" + *keyspace + "' does not exist");
      break;
    }

    case SessionEvent::NOTIFY_WORKER_CLOSED:
      if (--pending_workers_count_ == 0) {
        LOG_DEBUG("Session is disconnected");
        control_connection_.close();
        close_handles();
      }
      break;

    case SessionEvent::NOTIFY_UP:
      control_connection_.on_up(event.address);
      break;

    case SessionEvent::NOTIFY_DOWN:
      control_connection_.on_down(event.address);
      break;

    default:
      assert(false);
      break;
  }
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

#if UV_VERSION_MAJOR >= 1
void Session::on_resolve_name(MultiResolver<Session*>::NameResolver* resolver) {
  Session* session = resolver->data()->data();
  if (resolver->is_success()) {
    Host::Ptr host = session->add_host(resolver->address());
    host->set_hostname(resolver->hostname());
  } else if (resolver->is_timed_out()) {
    LOG_ERROR("Timed out attempting to resolve hostname for host %s\n",
              resolver->address().to_string().c_str());
  } else {
    LOG_ERROR("Unable to resolve hostname for host %s\n",
              resolver->address().to_string().c_str());
  }
}

void Session::on_add_resolve_name(NameResolver* resolver) {
  ResolveNameData& data = resolver->data();
  if (resolver->is_success() && !resolver->hostname().empty()) {
    data.host->set_hostname(resolver->hostname());
  }
  data.session->internal_on_add(data.host, data.is_initial_connection);
}
#endif

void Session::on_control_connection_ready() {
  // No hosts lock necessary (only called on session thread and read-only)
  load_balancing_policy_->init(control_connection_.connected_host(), hosts_, random_.get());
  load_balancing_policy_->register_handles(loop());
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->set_protocol_version(control_connection_.protocol_version());
  }
  for (HostMap::iterator it = hosts_.begin(), hosts_end = hosts_.end();
       it != hosts_end; ++it) {
    on_add(it->second, true);
  }
  if (pending_pool_count_ == 0) {
    notify_connect_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                         "No hosts available for connection using the current load balancing policy");
  }
  if (config().core_connections_per_host() == 0) {
    // Special case for internal testing. Not allowed by API
    LOG_DEBUG("Session connected with no core IO connections");
  }
}

void Session::on_control_connection_error(CassError code, const std::string& message) {
  notify_connect_error(code, message);
}

Future::Ptr Session::prepare(const char* statement, size_t length) {
  SharedRefPtr<PrepareRequest> prepare(new PrepareRequest(std::string(statement, length)));

  ResponseFuture::Ptr future(new ResponseFuture(metadata_.schema_snapshot(protocol_version(), cassandra_version())));
  future->statement.assign(statement, length);

  execute(RequestHandler::Ptr(new RequestHandler(prepare, future, NULL)));

  return future;
}

void Session::on_add(Host::Ptr host, bool is_initial_connection) {
#if UV_VERSION_MAJOR >= 1
  if (config_.use_hostname_resolution() && host->hostname().empty()) {
    NameResolver::resolve(loop(),
                          host->address(),
                          ResolveNameData(this, host, is_initial_connection),
                          on_add_resolve_name, config_.resolve_timeout_ms());
  } else {
#endif
    internal_on_add(host, is_initial_connection);
#if UV_VERSION_MAJOR >= 1
  }
#endif
}

void Session::internal_on_add(Host::Ptr host, bool is_initial_connection) {
  host->set_up();

  if (load_balancing_policy_->distance(host) == CASS_HOST_DISTANCE_IGNORE) {
    return;
  }

  if (is_initial_connection) {
    pending_pool_count_ += io_workers_.size();
  } else {
    load_balancing_policy_->on_add(host);
  }

  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->add_pool_async(host, is_initial_connection);
  }
}

void Session::on_remove(Host::Ptr host) {
  load_balancing_policy_->on_remove(host);
  { // Lock hosts
    ScopedMutex l(&hosts_mutex_);
    hosts_.erase(host->address());
  }
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->remove_pool_async(host, true);
  }
}

void Session::on_up(Host::Ptr host) {
  host->set_up();

  if (load_balancing_policy_->distance(host) == CASS_HOST_DISTANCE_IGNORE) {
    return;
  }

  load_balancing_policy_->on_up(host);

  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->add_pool_async(host, false);
  }
}

void Session::on_down(Host::Ptr host) {
  host->set_down();
  load_balancing_policy_->on_down(host);

  bool cancel_reconnect = false;
  if (load_balancing_policy_->distance(host) == CASS_HOST_DISTANCE_IGNORE) {
    // This permanently removes a host from all IO workers by stopping
    // any attempt to reconnect to that host.
    cancel_reconnect = true;
  }
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->remove_pool_async(host, cancel_reconnect);
  }
}

Future::Ptr Session::execute(const Request::ConstPtr& request,
                             const Address* preferred_address) {
  ResponseFuture::Ptr future(new ResponseFuture());

  RetryPolicy* retry_policy
      = request->retry_policy() != NULL ? request->retry_policy()
                                        : config().retry_policy();

  RequestHandler::Ptr request_handler(new RequestHandler(request,
                                                         future,
                                                         retry_policy));
  if (preferred_address) {
    request_handler->set_preferred_address(*preferred_address);
  }

  execute(request_handler);

  return future;
}

#if UV_VERSION_MAJOR == 0
void Session::on_execute(uv_async_t* data, int status) {
#else
void Session::on_execute(uv_async_t* data) {
#endif
  Session* session = static_cast<Session*>(data->data);

  bool is_closing = false;

  RequestHandler* temp = NULL;
  while (session->request_queue_->dequeue(temp)) {
    RequestHandler::Ptr request_handler(temp);
    if (request_handler) {
      request_handler->dec_ref(); // Queue reference

      request_handler->set_query_plan(session->new_query_plan(request_handler));
      request_handler->set_execution_plan(session->new_execution_plan(request_handler->request()));

      if (request_handler->timestamp() == CASS_INT64_MIN) {
        request_handler->set_timestamp(session->config_.timestamp_gen()->next());
      }

      bool is_done = false;
      while (!is_done) {
        request_handler->next_host();

        if (!request_handler->current_host()) {
          request_handler->set_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                                     "All connections on all I/O threads are busy");
          break;
        }

        size_t start = session->current_io_worker_;
        for (size_t i = 0, size = session->io_workers_.size(); i < size; ++i) {
          const IOWorker::Ptr& io_worker = session->io_workers_[start % size];
          if (io_worker->is_host_available(request_handler->current_host()->address()) &&
              io_worker->execute(request_handler)) {
            session->current_io_worker_ = (start + 1) % size;
            is_done = true;
            break;
          }
          start++;
        }
      }
    } else {
      is_closing = true;
    }
  }

  if (is_closing) {
    session->pending_workers_count_ = session->io_workers_.size();
    for (IOWorkerVec::iterator it = session->io_workers_.begin(),
                               end = session->io_workers_.end();
         it != end; ++it) {
      (*it)->close_async();
    }
  }
}

QueryPlan* Session::new_query_plan(const RequestHandler::Ptr& request_handler) {
  const CopyOnWritePtr<std::string> keyspace(keyspace_);
  return load_balancing_policy_->new_query_plan(*keyspace, request_handler.get(), token_map_.get());
}

SpeculativeExecutionPlan* Session::new_execution_plan(const Request* request) {
  const CopyOnWritePtr<std::string> keyspace(keyspace_);
  return speculative_execution_policy_->new_plan(*keyspace, request);
}

} // namespace cass
