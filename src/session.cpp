/*
  Copyright (c) 2014-2015 DataStax

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

#include "config.hpp"
#include "logger.hpp"
#include "prepare_request.hpp"
#include "request_handler.hpp"
#include "resolver.hpp"
#include "scoped_lock.hpp"
#include "timer.hpp"
#include "types.hpp"

extern "C" {

CassSession* cass_session_new() {
  return CassSession::to(new cass::Session());
}

void cass_session_free(CassSession* session) {
  // This attempts to close the session because the joining will
  // hang indefinately otherwise. This causes minimal delay
  // if the session is already closed.
  cass::SharedRefPtr<cass::Future> future(new cass::SessionFuture());
  session->close_async(future.get(), true);
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
  cass::SessionFuture* connect_future = new cass::SessionFuture();
  connect_future->inc_ref();
  session->connect_async(cluster->config(), std::string(keyspace, keyspace_length), connect_future);
  return CassFuture::to(connect_future);
}

CassFuture* cass_session_close(CassSession* session) {
  cass::SessionFuture* close_future = new cass::SessionFuture();
  close_future->inc_ref();
  session->close_async(close_future);
  return CassFuture::to(close_future);
}

CassFuture* cass_session_prepare(CassSession* session, const char* query) {
  return cass_session_prepare_n(session, query, strlen(query));
}

CassFuture* cass_session_prepare_n(CassSession* session,
                                   const char* query,
                                   size_t query_length) {
  return CassFuture::to(session->prepare(query, query_length));
}

CassFuture* cass_session_execute(CassSession* session,
                                 const CassStatement* statement) {
  return CassFuture::to(session->execute(statement->from()));
}

CassFuture* cass_session_execute_batch(CassSession* session, const CassBatch* batch) {
  return CassFuture::to(session->execute(batch->from()));
}

const CassSchema* cass_session_get_schema(CassSession* session) {
  return CassSchema::to(session->copy_schema());
}

void  cass_session_get_metrics(CassSession* session,
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
    , current_host_mark_(true)
    , pending_resolve_count_(0)
    , pending_pool_count_(0)
    , pending_workers_count_(0)
    , current_io_worker_(0) {
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
  metrics_.reset(new Metrics(config_.thread_count_io() + 1));
  load_balancing_policy_.reset(config.load_balancing_policy());
  connect_future_.reset();
  close_future_.reset();
  { // Lock hosts
    ScopedMutex l(&hosts_mutex_);
    hosts_.clear();
  }
  io_workers_.clear();
  request_queue_.reset();
  cluster_meta_.clear();
  control_connection_.clear();
  current_host_mark_ = true;
  pending_resolve_count_ = 0;
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
    SharedRefPtr<IOWorker> io_worker(new IOWorker(this));
    int rc = io_worker->init();
    if (rc != 0) return rc;
    io_workers_.push_back(io_worker);
  }

  return rc;
}

void Session::broadcast_keyspace_change(const std::string& keyspace,
                                        const IOWorker* calling_io_worker) {
  // This can run on an IO worker thread. This is thread-safe because the IO workers
  // vector never changes after initialization and IOWorker::set_keyspace() uses a mutex.
  // This also means that calling "USE <keyspace>" frequently is an anti-pattern.
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    if (*it == calling_io_worker) continue;
      (*it)->set_keyspace(keyspace);
  }
}

SharedRefPtr<Host> Session::get_host(const Address& address) {
  // Lock hosts. This can be called on a non-session thread.
  ScopedMutex l(&hosts_mutex_);
  HostMap::iterator it = hosts_.find(address);
  if (it == hosts_.end()) {
    return SharedRefPtr<Host>();
  }
  return it->second;
}

SharedRefPtr<Host> Session::add_host(const Address& address) {
  LOG_DEBUG("Adding new host: %s", address.to_string().c_str());
  SharedRefPtr<Host> host(new Host(address, !current_host_mark_));
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

void Session::connect_async(const Config& config, const std::string& keyspace, Future* future) {
  ScopedMutex l(&state_mutex_);

  if (state_ != SESSION_STATE_CLOSED) {
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

  state_ = SESSION_STATE_CONNECTING;
  connect_future_.reset(future);

  if (!keyspace.empty()) {
    broadcast_keyspace_change(keyspace, NULL);
  }

  // If this is a reconnect then the old thread needs to be
  // joined before creating a new thread.
  join();

  run();
}

void Session::close_async(Future* future, bool force) {
  ScopedMutex l(&state_mutex_);

  bool wait_for_connect_to_finish = (force && state_ == SESSION_STATE_CONNECTING);
  if (state_ != SESSION_STATE_CONNECTED && !wait_for_connect_to_finish) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_CLOSE,
                      "Already closing or closed");
    return;
  }

  state_ = SESSION_STATE_CLOSING;
  close_future_.reset(future);

  if (!wait_for_connect_to_finish) {
    internal_close();
  }
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
  if (state_ == SESSION_STATE_CONNECTING) {
    state_ = SESSION_STATE_CONNECTED;
  } else { // We recieved a 'force' close event
    internal_close();
  }
  connect_future_->set();
  connect_future_.reset();
}

void Session::notify_connect_error(CassError code, const std::string& message) {
  ScopedMutex l(&state_mutex_);
  state_ = SESSION_STATE_CLOSING;
  internal_close();
  connect_future_->set_error(code, message);
  connect_future_.reset();
}

void Session::notify_closed() {
  ScopedMutex l(&state_mutex_);
  state_ = SESSION_STATE_CLOSED;
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
  LOG_INFO("Creating %u IO worker threads",
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

      const Config::ContactPointList& contact_points = config_.contact_points();
      for (Config::ContactPointList::const_iterator it = contact_points.begin(),
                                                    end = contact_points.end();
           it != end; ++it) {
        const std::string& seed = *it;
        Address address;
        if (Address::from_string(seed, port, &address)) {
          add_host(address);
        } else {
          pending_resolve_count_++;
          Resolver::resolve(loop(), seed, port, this, on_resolve);
        }
      }

      if (pending_resolve_count_ == 0) {
        internal_connect();
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

void Session::on_resolve(Resolver* resolver) {
  Session* session = static_cast<Session*>(resolver->data());
  if (resolver->is_success()) {
    session->add_host(resolver->address());
  } else {
    LOG_ERROR("Unable to resolve host %s:%d\n",
              resolver->host().c_str(), resolver->port());
  }
  if (--session->pending_resolve_count_ == 0) {
    session->internal_connect();
  }
}

void Session::execute(RequestHandler* request_handler) {
  if (!request_queue_->enqueue(request_handler)) {
    request_handler->on_error(CASS_ERROR_LIB_REQUEST_QUEUE_FULL,
                              "The request queue has reached capacity");
  }
}

void Session::on_control_connection_ready() {
  // No hosts lock necessary (only called on session thread and read-only)
  load_balancing_policy_->init(control_connection_.connected_host(), hosts_);
  load_balancing_policy_->register_handles(loop());
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->set_protocol_version(control_connection_.protocol_version());
  }
  for (HostMap::iterator it = hosts_.begin(), hosts_end = hosts_.end();
       it != hosts_end; ++it) {
    on_add(it->second, true);
  }
  if (config().core_connections_per_host() == 0) {
    // Special case for internal testing. Not allowed by API
    LOG_DEBUG("Session connected with no core IO connections");
  }
}

void Session::on_control_connection_error(CassError code, const std::string& message) {
  notify_connect_error(code, message);
}

Future* Session::prepare(const char* statement, size_t length) {
  PrepareRequest* prepare = new PrepareRequest();
  prepare->set_query(statement, length);

  ResponseFuture* future = new ResponseFuture(cluster_meta_.schema());
  future->inc_ref(); // External reference
  future->statement.assign(statement, length);

  RequestHandler* request_handler = new RequestHandler(prepare, future);
  request_handler->inc_ref(); // IOWorker reference

  execute(request_handler);

  return future;
}

void Session::on_add(SharedRefPtr<Host> host, bool is_initial_connection) {
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
    (*it)->add_pool_async(host->address(), is_initial_connection);
  }
}

void Session::on_remove(SharedRefPtr<Host> host) {
  load_balancing_policy_->on_remove(host);
  { // Lock hosts
    ScopedMutex l(&hosts_mutex_);
    hosts_.erase(host->address());
  }
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->remove_pool_async(host->address(), true);
  }
}

void Session::on_up(SharedRefPtr<Host> host) {
  host->set_up();

  if (load_balancing_policy_->distance(host) == CASS_HOST_DISTANCE_IGNORE) {
    return;
  }

  load_balancing_policy_->on_up(host);

  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->add_pool_async(host->address(), false);
  }
}

void Session::on_down(SharedRefPtr<Host> host) {
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
    (*it)->remove_pool_async(host->address(), cancel_reconnect);
  }
}

Future* Session::execute(const RoutableRequest* request) {
  ResponseFuture* future = new ResponseFuture(cluster_meta_.schema());
  future->inc_ref(); // External reference

  RequestHandler* request_handler = new RequestHandler(request, future);
  request_handler->inc_ref(); // IOWorker reference

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

  RequestHandler* request_handler = NULL;
  while (session->request_queue_->dequeue(request_handler)) {
    if (request_handler != NULL) {
      request_handler->set_query_plan(session->new_query_plan(request_handler->request()));

      bool is_done = false;
      while (!is_done) {
        request_handler->next_host();

        Address address;
        if (!request_handler->get_current_host_address(&address)) {
          request_handler->on_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                                    "All connections on all I/O threads are busy");
          break;
        }

        size_t start = session->current_io_worker_;
        for (size_t i = 0, size = session->io_workers_.size(); i < size; ++i) {
          const SharedRefPtr<IOWorker>& io_worker = session->io_workers_[start % size];
          if (io_worker->is_host_available(address) &&
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

QueryPlan* Session::new_query_plan(const Request* request) {
  std::string connected_keyspace;
  if (!io_workers_.empty()) {
    connected_keyspace = io_workers_[0]->keyspace();
  }
  return load_balancing_policy_->new_query_plan(connected_keyspace, request, cluster_meta_.token_map());
}

} // namespace cass
