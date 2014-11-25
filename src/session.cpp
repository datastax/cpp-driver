/*
  Copyright (c) 2014 DataStax

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
#include "round_robin_policy.hpp"
#include "timer.hpp"
#include "types.hpp"

#include <boost/bind.hpp>

extern "C" {

CassFuture* cass_session_close(CassSession* session) {
  // TODO(mpenick): Make sure this handles close during the middle of a connect
  cass::SessionCloseFuture* close_future = new cass::SessionCloseFuture(session);
  close_future->inc_ref();
  session->close_async(close_future);
  return CassFuture::to(close_future);
}

CassFuture* cass_session_prepare(CassSession* session, CassString query) {
  return CassFuture::to(session->prepare(query.data, query.length));
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

} // extern "C"

namespace cass {

Session::Session(const Config& config)
    : config_(config)
    , close_future_(NULL)
    , current_host_mark_(true)
    , load_balancing_policy_(config.load_balancing_policy())
    , pending_resolve_count_(0)
    , pending_pool_count_(0)
    , pending_workers_count_(0)
    , current_io_worker_(0) {}

int Session::init() {
  int rc = cluster_meta_.init();
  if (rc != 0) return rc;
  rc = EventThread<SessionEvent>::init(config_.queue_size_event());
  if (rc != 0) return rc;
  request_queue_.reset(
      new AsyncQueue<MPMCQueue<RequestHandler*> >(config_.queue_size_io()));
  rc = request_queue_->init(loop(), this, &Session::on_execute);
  if (rc != 0) return rc;

  unsigned num_threads = config_.thread_count_io();
  if(num_threads == 0) {
    num_threads = 1; // Default if we can't determine the number of cores
    uv_cpu_info_t* cpu_infos;
    int cpu_count;
    if (uv_cpu_info(&cpu_infos, &cpu_count).code == 0 && cpu_count > 0) {
      num_threads = cpu_count;
      uv_free_cpu_info(cpu_infos, cpu_count);
    }
  }

  for (unsigned i = 0; i < num_threads; ++i) {
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

SharedRefPtr<Host> Session::get_host(const Address& address, bool should_mark) {
  HostMap::iterator it = hosts_.find(address);
  if (it == hosts_.end()) {
    return SharedRefPtr<Host>();
  }
  if (should_mark) {
    it->second->set_mark(current_host_mark_);
  }
  return it->second;
}

SharedRefPtr<Host> Session::add_host(const Address& address, bool should_mark) {
  Logger::debug("Session: Adding new host: %s", address.to_string().c_str());

  bool mark = should_mark ? current_host_mark_ : !current_host_mark_;
  SharedRefPtr<Host> host(new Host(address, mark));
  hosts_[address] = host;
  return host;
}

void Session::purge_hosts(bool is_initial_connection) {
  HostMap::iterator it = hosts_.begin();
  while (it != hosts_.end()) {
    if (it->second->mark() != current_host_mark_) {
      HostMap::iterator to_remove_it = it++;

      std::string address_str = to_remove_it->first.to_string();
      if (is_initial_connection) {
        Logger::warn("Session: Unable to reach contact point %s",
                      address_str.c_str());
        hosts_.erase(to_remove_it);
      } else {
        Logger::info("Session: Host %s removed",
                      address_str.c_str());
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

bool Session::notify_closed_async() {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_CLOSED;
  return send_event_async(event);
}

bool Session::notify_up_async(const Address& address) {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_UP;
  event.address = address;
  return send_event_async(event);
}

bool Session::notify_down_async(const Address& address, bool is_critical_failure) {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_DOWN;
  event.address = address;
  event.is_critical_failure = is_critical_failure;
  return send_event_async(event);
}

bool Session::connect_async(const std::string& keyspace, Future* future) {
  if (init() != 0) {
    return false;
  }

  SessionEvent event;
  event.type = SessionEvent::CONNECT;
  if (!send_event_async(event)) {
    return false;
  }

  Logger::debug("Session: issued connect event");

  if (!keyspace.empty()) {
    broadcast_keyspace_change(keyspace, NULL);
  }

  connect_future_.reset(future);

  run();

  return true;
}

void Session::close_async(Future* future) {
  assert(future != NULL);
  close_future_ = future;
  while (!request_queue_->enqueue(NULL)) {
    // Keep trying
  }
  Logger::debug("Session: issued shutdown");
}

void Session::internal_connect() {
  if (hosts_.empty()) {
    connect_future_->set_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                               "No hosts provided or no hosts resolved");
    connect_future_.reset();
    return;
  }
  control_connection_.connect(this);
}

void Session::close_handles() {
  EventThread<SessionEvent>::close_handles();
  request_queue_->close_handles();
}

void Session::on_run() {
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
  close_future_->set();
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
          hosts_[address] = SharedRefPtr<Host>(new Host(address, !current_host_mark_));
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
          Logger::debug("Session is connected");
          connect_future_->set();
          connect_future_.reset();
        }
        Logger::debug("Session pending pool count %d", pending_pool_count_);
      }
      break;

    case SessionEvent::NOTIFY_CLOSED:
      if (--pending_workers_count_ == 0) {
        control_connection_.close();
        close_handles();
      }
      break;

    case SessionEvent::NOTIFY_UP:
      control_connection_.on_up(event.address);
      break;

    case SessionEvent::NOTIFY_DOWN:
      control_connection_.on_down(event.address, event.is_critical_failure);
      break;

    default:
      assert(false);
      break;
  }
}

void Session::on_resolve(Resolver* resolver) {
  Session* session = static_cast<Session*>(resolver->data());
  if (resolver->is_success()) {
    const Address& address = resolver->address();
    session->hosts_[address]
        = SharedRefPtr<Host>(new Host(address, !session->current_host_mark_));
  } else {
    Logger::error("Unable to resolve host %s:%d\n",
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
  load_balancing_policy_->init(control_connection_.connected_host(), hosts_);
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->set_protocol_version(control_connection_.protocol_version());
  }
  for (HostMap::iterator it = hosts_.begin(), hosts_end = hosts_.end();
       it != hosts_end; ++it) {
    on_add(it->second, true);
  }
  if (config().core_connections_per_host() > 0) {
    pending_pool_count_ = hosts_.size() * io_workers_.size();
  } else {
    // Special case for internal testing. Not allowed by API
    Logger::debug("Session connected with no core IO connections");
    connect_future_->set();
    connect_future_.reset();
  }
}

void Session::on_control_connection_error(CassError code, const std::string& message) {
  connect_future_->set_error(code, message);
  connect_future_.reset();
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
  if (!is_initial_connection) {
    load_balancing_policy_->on_add(host);
  }

  if (load_balancing_policy_->distance(host) == CASS_HOST_DISTANCE_IGNORE) {
    return;
  }

  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->add_pool_async(host->address(), is_initial_connection);
  }
}

void Session::on_remove(SharedRefPtr<Host> host) {
  load_balancing_policy_->on_remove(host);

  hosts_.erase(host->address());
  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->remove_pool_async(host->address());
  }
}

void Session::on_up(SharedRefPtr<Host> host) {
  host->set_up();
  load_balancing_policy_->on_up(host);

  for (IOWorkerVec::iterator it = io_workers_.begin(),
       end = io_workers_.end(); it != end; ++it) {
    (*it)->add_pool_async(host->address(), false);
  }
}

void Session::on_down(SharedRefPtr<Host> host, bool is_critical_failure) {
  host->set_down();
  load_balancing_policy_->on_down(host);

  if (load_balancing_policy_->distance(host) == CASS_HOST_DISTANCE_IGNORE ||
      is_critical_failure) {
    // This permanently removes a host from all IO workers and stops
    // any attempt to reconnect to that host.
    for (IOWorkerVec::iterator it = io_workers_.begin(),
         end = io_workers_.end(); it != end; ++it) {
      (*it)->remove_pool_async(host->address());
    }
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

void Session::on_execute(uv_async_t* data, int status) {
  Session* session = static_cast<Session*>(data->data);

  bool is_closing = false;

  RequestHandler* request_handler = NULL;
  while (session->request_queue_->dequeue(request_handler)) {
    if (request_handler != NULL) {
      request_handler->set_query_plan(session->new_query_plan(request_handler->request()));

      bool is_done = false;
      while(!is_done) {
        request_handler->next_host();

        Address address;
        if(!request_handler->get_current_host_address(&address)) {
          request_handler->on_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                                    "Session: No hosts available");
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
