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

#include "io_worker.hpp"

#include "config.hpp"
#include "logger.hpp"
#include "pool.hpp"
#include "request_handler.hpp"
#include "session.hpp"
#include "scoped_lock.hpp"
#include "timer.hpp"

namespace cass {

IOWorker::IOWorker(Session* session)
    : session_(session)
    , config_(session->config())
    , metrics_(session->metrics())
    , protocol_version_(-1)
    , is_closing_(false)
    , pending_request_count_(0)
    , request_queue_(config_.queue_size_io()) {
  prepare_.data = this;
  uv_mutex_init(&keyspace_mutex_);
  uv_mutex_init(&unavailable_addresses_mutex_);
}

IOWorker::~IOWorker() {
  uv_mutex_destroy(&keyspace_mutex_);
  uv_mutex_destroy(&unavailable_addresses_mutex_);
}

int IOWorker::init() {
  int rc = EventThread<IOWorkerEvent>::init(config_.queue_size_event());
  if (rc != 0) return rc;
  rc = request_queue_.init(loop(), this, &IOWorker::on_execute);
  if (rc != 0) return rc;
  rc = uv_prepare_init(loop(), &prepare_);
  if (rc != 0) return rc;
  rc = uv_prepare_start(&prepare_, on_prepare);
  if (rc != 0) return rc;
  return rc;
}

std::string IOWorker::keyspace() {
  // Not returned by reference on purpose. This memory can't be shared
  // because it could be updated as a result of a "USE <keyspace" query.
  ScopedMutex lock(&keyspace_mutex_);
  return keyspace_;
}

void IOWorker::set_keyspace(const std::string& keyspace) {
  ScopedMutex lock(&keyspace_mutex_);
  keyspace_ = keyspace;
}

bool IOWorker::is_current_keyspace(const std::string& keyspace) {
  // This mutex is locked on the query request path, but it should
  // almost never have any contention. That could only happen when
  // there is a "USE <keyspace>" query.  These *should* happen
  // infrequently. This is preferred over IOWorker::keyspace()
  // because it doesn't allocate memory.
  ScopedMutex lock(&keyspace_mutex_);
  return keyspace_ == keyspace;
}

void IOWorker::broadcast_keyspace_change(const std::string& keyspace) {
  set_keyspace(keyspace);
  session_->broadcast_keyspace_change(keyspace, this);
}

bool IOWorker::is_host_up(const Address& address) const {
  PoolMap::const_iterator it = pools_.find(address);
  return it != pools_.end() && it->second->is_ready();
}

void IOWorker::set_host_is_available(const Address& address, bool is_available) {
  ScopedMutex lock(&unavailable_addresses_mutex_);
  if (is_available) {
    unavailable_addresses_.erase(address);
  } else {
    unavailable_addresses_.insert(address);
  }
}

bool IOWorker::is_host_available(const Address& address) {
  ScopedMutex lock(&unavailable_addresses_mutex_);
  return unavailable_addresses_.count(address) == 0;
}

bool IOWorker::add_pool_async(const Address& address, bool is_initial_connection) {
  IOWorkerEvent event;
  event.type = IOWorkerEvent::ADD_POOL;
  event.address = address;
  event.is_initial_connection = is_initial_connection;
  return send_event_async(event);
}

bool IOWorker::remove_pool_async(const Address& address, bool cancel_reconnect) {
  IOWorkerEvent event;
  event.type = IOWorkerEvent::REMOVE_POOL;
  event.address = address;
  event.cancel_reconnect = cancel_reconnect;
  return send_event_async(event);
}

void IOWorker::close_async() {
  while (!request_queue_.enqueue(NULL)) {
    // Keep trying
  }
}

void IOWorker::add_pool(const Address& address, bool is_initial_connection) {
  if (!is_closing_ && pools_.count(address) == 0) {
    LOG_INFO("Adding pool for host %s io_worker(%p)",
             address.to_string(true).c_str(), static_cast<void*>(this));

    set_host_is_available(address, false);

    SharedRefPtr<Pool> pool(new Pool(this, address, is_initial_connection));
    pools_[address] = pool;
    pool->connect();
  }
}

bool IOWorker::execute(RequestHandler* request_handler) {
  return request_queue_.enqueue(request_handler);
}

void IOWorker::retry(RequestHandler* request_handler, RetryType retry_type) {
  if (retry_type == RETRY_WITH_NEXT_HOST) {
    request_handler->next_host();
  }

  Address address;
  if (!request_handler->get_current_host_address(&address)) {
    request_handler->on_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                              "All hosts in current policy attempted "
                              "and were either unavailable or failed");
    return;
  }

  PoolMap::iterator it = pools_.find(address);
  if (it != pools_.end() && it->second->is_ready()) {
    const SharedRefPtr<Pool>& pool = it->second;
    Connection* connection = pool->borrow_connection();
    if (connection != NULL) {
      if (!pool->write(connection, request_handler)) {
        retry(request_handler, RETRY_WITH_NEXT_HOST);
      }
    } else { // Too busy, or no connections
      pool->wait_for_connection(request_handler);
    }
  } else {
    retry(request_handler, RETRY_WITH_NEXT_HOST);
  }
}

void IOWorker::request_finished(RequestHandler* request_handler) {
  pending_request_count_--;
  maybe_close();
  request_queue_.send();
}

void IOWorker::notify_pool_ready(Pool* pool) {
  if (pool->is_initial_connection()) {
    session_->notify_ready_async();
  } else if (!is_closing_ && pool->is_ready()){
    session_->notify_up_async(pool->address());
  }
}

void IOWorker::notify_pool_closed(Pool* pool) {
  Address address = pool->address(); // Not a reference on purpose

  bool is_critical_failure = pool->is_critical_failure();
  bool cancel_reconnect = pool->cancel_reconnect();

  LOG_INFO("Pool for host %s closed: pool(%p) io_worker(%p)",
           address.to_string().c_str(),
           static_cast<void*>(pool),
           static_cast<void*>(this));

  // All non-shared pointers to this pool are invalid after this call
  // and it must be done before maybe_notify_closed().
  pools_.erase(address);

  if (is_closing_) {
    maybe_notify_closed();
  } else {
    session_->notify_down_async(address);
    if (!is_critical_failure && !cancel_reconnect) {
      schedule_reconnect(address);
    }
  }
}

void IOWorker::add_pending_flush(Pool* pool) {
  pools_pending_flush_.push_back(SharedRefPtr<Pool>(pool));
}

void IOWorker::maybe_close() {
  if (is_closing_ && pending_request_count_ <= 0) {
    if (config_.core_connections_per_host() > 0) {
      for (PoolMap::iterator it = pools_.begin(); it != pools_.end(); ++it) {
        it->second->close();
      }
      maybe_notify_closed();
    } else {
      // Pool::close is intertwined with this class via notify_pool_closed.
      // Requires special handling to avoid iterator invalidation and double closing
      // other resources.
      // This path only possible for internal config. API does not allow.
      while (!pools_.empty()) pools_.begin()->second->close();
    }
  }
}

void IOWorker::maybe_notify_closed() {
  if (pools_.empty()) {
    session_->notify_worker_closed_async();
    close_handles();
  }
}

void IOWorker::close_handles() {
  EventThread<IOWorkerEvent>::close_handles();
  request_queue_.close_handles();
  uv_prepare_stop(&prepare_);
  uv_close(copy_cast<uv_prepare_t*, uv_handle_t*>(&prepare_), NULL);

  for (PendingReconnectMap::iterator it = pending_reconnects_.begin(),
       end = pending_reconnects_.end(); it != end; ++it) {
    LOG_DEBUG("Stopping reconnect timers reconnect(%p timer(%p)) io_worker(%p)",
              static_cast<void*>(&it->second),
              static_cast<void*>(it->second.timer),
              static_cast<void*>(this));
    it->second.stop_timer();
  }
  LOG_DEBUG("Active handles following close: %d", loop()->active_handles);
}

void IOWorker::on_pending_pool_reconnect(Timer* timer) {
  PendingReconnect* pending_reconnect =
      static_cast<PendingReconnect*>(timer->data());

  IOWorker* io_worker = pending_reconnect->io_worker;

  LOG_DEBUG("Reconnecting pool reconnect(%p timer(%p)) io_worker(%p)",
            static_cast<void*>(pending_reconnect),
            static_cast<void*>(timer),
            static_cast<void*>(io_worker));

  const Address& address = pending_reconnect->address;
  io_worker->add_pool(address, false);
  io_worker->pending_reconnects_.erase(address);
}

void IOWorker::on_event(const IOWorkerEvent& event) {
  switch (event.type) {
    case IOWorkerEvent::ADD_POOL: {
      // Stop any attempts to reconnect because add_pool() is going to attempt
      // reconnection right away.
      cancel_reconnect(event.address);
      add_pool(event.address, event.is_initial_connection);
      break;
    }

    case IOWorkerEvent::REMOVE_POOL: {
      if (event.cancel_reconnect) {
        LOG_DEBUG("REMOVE_POOL event for %s cancelling reconnect io_worker(%p)",
                  event.address.to_string().c_str(),
                  static_cast<void*>(this));
        cancel_reconnect(event.address);
      }

      PoolMap::iterator it = pools_.find(event.address);
      if (it != pools_.end()) {
        LOG_DEBUG("REMOVE_POOL event for %s closing pool(%p) io_worker(%p)",
                  event.address.to_string().c_str(),
                  static_cast<void*>(it->second.get()),
                  static_cast<void*>(this));
        it->second->close(event.cancel_reconnect);
      }
      break;
    }

    default:
      assert(false);
      break;
  }
}

#if UV_VERSION_MAJOR == 0
void IOWorker::on_execute(uv_async_t* async, int status) {
#else
void IOWorker::on_execute(uv_async_t* async) {
#endif
  IOWorker* io_worker = static_cast<IOWorker*>(async->data);

  RequestHandler* request_handler = NULL;
  size_t remaining = io_worker->config().max_requests_per_flush();
  while (remaining != 0 && io_worker->request_queue_.dequeue(request_handler)) {
    if (request_handler != NULL) {
      io_worker->pending_request_count_++;
      request_handler->set_io_worker(io_worker);
      request_handler->retry(RETRY_WITH_CURRENT_HOST);
    } else {
      io_worker->is_closing_ = true;
    }
    remaining--;
  }

  io_worker->maybe_close();
}

#if UV_VERSION_MAJOR == 0
void IOWorker::on_prepare(uv_prepare_t* prepare, int status) {
#else
void IOWorker::on_prepare(uv_prepare_t* prepare) {
#endif
  IOWorker* io_worker = static_cast<IOWorker*>(prepare->data);

  for (PoolVec::iterator it = io_worker->pools_pending_flush_.begin(),
       end = io_worker->pools_pending_flush_.end(); it != end; ++it) {
    (*it)->flush();
  }
  io_worker->pools_pending_flush_.clear();
}

void IOWorker::schedule_reconnect(const Address& address) {
  if (is_closing_ || pending_reconnects_.count(address) > 0) {
    LOG_DEBUG("Reconnect already pending for host %s io_worker(%p)",
              address.to_string().c_str(), static_cast<void*>(this));
    return;
  }

  PendingReconnect& pr = pending_reconnects_[address];
  pr.io_worker = this;
  pr.address = address;
  pr.timer = Timer::start(loop(),
                          config_.reconnect_wait_time_ms(),
                          &pr,
                          IOWorker::on_pending_pool_reconnect);
  LOG_DEBUG("Scheduling reconnect(%p timer(%p)) for host %s io_worker(%p)",
            static_cast<void*>(&pr),
            static_cast<void*>(pr.timer),
            address.to_string().c_str(),
            static_cast<void*>(this));
}

void IOWorker::cancel_reconnect(const Address& address) {
  PendingReconnectMap::iterator it = pending_reconnects_.find(address);
  if (it != pending_reconnects_.end()) {
    LOG_DEBUG("Cancelling reconnect(%p timer(%p)) for host %s io_worker(%p)",
              static_cast<void*>(&it->second),
              static_cast<void*>(it->second.timer),
              address.to_string().c_str(),
              static_cast<void*>(this));
    it->second.stop_timer();
    pending_reconnects_.erase(it);
  }
}

void IOWorker::PendingReconnect::stop_timer() {
  if (timer != NULL) {
    Timer::stop(timer);
    timer = NULL;
  }
}

} // namespace cass
