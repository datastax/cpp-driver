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
#include "io_worker.hpp"
#include "prepare_request.hpp"
#include "request_handler.hpp"
#include "resolver.hpp"
#include "round_robin_policy.hpp"
#include "types.hpp"

extern "C" {

void cass_session_free(CassSession* session) {
  delete session->from();
}

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

} // extern "C"

namespace cass {

Session::Session(const Config& config)
    : close_future_(NULL)
    , config_(config)
    , load_balancing_policy_(new RoundRobinPolicy())
    , pending_resolve_count_(0)
    , pending_pool_count_(0)
    , pending_workers_count_(0)
    , current_io_worker_(0) {
  uv_mutex_init(&keyspace_mutex_);
}

Session::~Session() {
  uv_mutex_destroy(&keyspace_mutex_);
  for (IOWorkerVec::iterator it = io_workers_.begin(), end = io_workers_.end();
       it != end; ++it) {
    delete (*it);
  }
}

int Session::init() {
  int rc = EventThread<SessionEvent>::init(config_.queue_size_event());
  if (rc != 0) return rc;
  request_queue_.reset(
      new AsyncQueue<MPMCQueue<RequestHandler*> >(config_.queue_size_io()));
  rc = request_queue_->init(loop(), this, &Session::on_execute);
  if (rc != 0) return rc;
  for (unsigned i = 0; i < config_.thread_count_io(); ++i) {
    ScopedPtr<IOWorker> io_worker(new IOWorker(this, logger_.get(), config_));
    int rc = io_worker->init();
    if (rc != 0) return rc;
    io_workers_.push_back(io_worker.release());
  }
  return rc;
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

bool Session::connect_async(const std::string& keyspace, Future* future) {
  logger_.reset(new Logger(config_));
  if (logger_->init() != 0 || init() != 0) {
    return false;
  }

  SessionEvent event;
  event.type = SessionEvent::CONNECT;
  if (!send_event_async(event)) {
    return false;
  }

  if (!keyspace.empty()) {
    set_keyspace(keyspace);
  }

  connect_future_.reset(future);

  run();

  return true;
}

void Session::close_async(Future* future) {
  close_future_ = future;
  if (close_future_ != NULL) {
    close_future_->inc_ref();
  }
  while (!request_queue_->enqueue(NULL)) {
    // Keep trying
  }
}

void Session::init_pools() {
  if (hosts_.empty()) {
    connect_future_->set_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                               "No hosts provided or no hosts resolved");
    connect_future_.reset();
    return;
  }

  pending_pool_count_ = hosts_.size() * io_workers_.size();
  for (HostSet::iterator hosts_it = hosts_.begin(), hosts_end = hosts_.end();
       hosts_it != hosts_end; ++hosts_it) {
    for (IOWorkerVec::iterator it = io_workers_.begin(),
                               end = io_workers_.end();
         it != end; ++it) {
      (*it)->add_pool_async(*hosts_it);
    }
  }
}

void Session::close_handles() {
  EventThread<SessionEvent>::close_handles();
  request_queue_->close_handles();
}

void Session::on_run() {
  if (config_.log_level() != CASS_LOG_DISABLED) {
    logger_->run();
  }
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
  logger_->join();

  if (close_future_ == NULL) {
    // If no future, 'this' is the last pointer
    delete this;
  } else {
    // 'this' will be cleaned up by the waiting thread
    close_future_->set();
    close_future_->dec_ref();
  }
}

void Session::on_event(const SessionEvent& event) {
  if (event.type == SessionEvent::CONNECT) {
    int port = config_.port();

    const Config::ContactPointList& contact_points = config_.contact_points();
    for (Config::ContactPointList::const_iterator it = contact_points.begin(),
                                                  end = contact_points.end();
         it != end; ++it) {
      const std::string& seed = *it;
      Address address;
      if (Address::from_string(seed, port, &address)) {
        Host host(address);
        hosts_.insert(host);
      } else {
        pending_resolve_count_++;
        Resolver::resolve(loop(), seed, port, this, on_resolve);
      }
    }

    if (pending_resolve_count_ == 0) {
      init_pools();
    }
  } else if (event.type == SessionEvent::NOTIFY_READY) {
    if (--pending_pool_count_ == 0) {
      logger_->debug("Session is connected");
      load_balancing_policy_->init(hosts_);
      connect_future_->set();
      connect_future_.reset();
    }
    logger_->debug("Session pending pool count %d", pending_pool_count_);
  } else if (event.type == SessionEvent::NOTIFY_CLOSED) {
    if (--pending_workers_count_ == 0) {
      logger_->close_async();
      close_handles();
    }
  }
}

void Session::on_resolve(Resolver* resolver) {
  Session* session = reinterpret_cast<Session*>(resolver->data());
  if (resolver->is_success()) {
    Host host(resolver->address());
    session->hosts_.insert(host);
  } else {
    session->logger_->error("Unable to resolve %s:%d\n",
                            resolver->host().c_str(), resolver->port());
  }
  if (--session->pending_resolve_count_ == 0) {
    session->init_pools();
  }
}

void Session::execute(RequestHandler* request_handler) {
  if (!request_queue_->enqueue(request_handler)) {
    request_handler->on_error(CASS_ERROR_LIB_REQUEST_QUEUE_FULL,
                              "The request queue has reached capacity");
    request_handler->dec_ref();
  }
}

Future* Session::prepare(const char* statement, size_t length) {
  PrepareRequest* prepare = new PrepareRequest();
  prepare->set_query(statement, length);

  ResponseFuture* future = new ResponseFuture();
  future->inc_ref(); // External reference
  future->set_loop(loop());
  future->statement.assign(statement, length);

  RequestHandler* request_handler = new RequestHandler(prepare, future);
  request_handler->inc_ref(); // IOWorker reference

  execute(request_handler);

  return future;
}

Future* Session::execute(const Request* statement) {
  ResponseFuture* future = new ResponseFuture();
  future->inc_ref(); // External reference
  future->set_loop(loop());

  RequestHandler* request_handler = new RequestHandler(statement, future);
  request_handler->inc_ref(); // IOWorker reference

  execute(request_handler);

  return future;
}

void Session::on_execute(uv_async_t* data, int status) {
  Session* session = reinterpret_cast<Session*>(data->data);

  bool is_closing = false;

  RequestHandler* request_handler = NULL;
  while (session->request_queue_->dequeue(request_handler)) {
    if (request_handler != NULL) {
      request_handler->keyspace = session->keyspace();
      session->load_balancing_policy_->new_query_plan(&request_handler->hosts);

      size_t start = session->current_io_worker_;
      size_t remaining = session->io_workers_.size();
      const size_t size = session->io_workers_.size();
      while (remaining != 0) {
        // TODO(mpenick): Make this something better than RR
        IOWorker* io_worker = session->io_workers_[start % size];
        if (io_worker->execute(request_handler)) {
          session->current_io_worker_ = (start + 1) % size;
          break;
        }
        start++;
        remaining--;
      }

      if (remaining == 0) {
        request_handler->on_error(CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD,
                                  "All workers are busy");
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

} // namespace cass
