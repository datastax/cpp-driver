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

#include "pool.hpp"

#include "connection.hpp"
#include "error_response.hpp"
#include "io_worker.hpp"
#include "logger.hpp"
#include "prepare_handler.hpp"
#include "session.hpp"
#include "set_keyspace_handler.hpp"
#include "request_handler.hpp"
#include "result_response.hpp"
#include "timer.hpp"

namespace cass {

static bool least_busy_comp(Connection* a, Connection* b) {
  return a->pending_request_count() < b->pending_request_count();
}

Pool::Pool(IOWorker* io_worker,
           const Address& address,
           bool is_initial_connection)
    : io_worker_(io_worker)
    , address_(address)
    , loop_(io_worker->loop())
    , config_(io_worker->config())
    , metrics_(io_worker->metrics())
    , state_(POOL_STATE_NEW)
    , available_connection_count_(0)
    , is_available_(false)
    , is_initial_connection_(is_initial_connection)
    , is_critical_failure_(false)
    , is_pending_flush_(false)
    , cancel_reconnect_(false) {}

Pool::~Pool() {
  LOG_DEBUG("Pool dtor with %u pending requests pool(%p)",
            static_cast<unsigned int>(pending_requests_.size()),
            static_cast<void*>(this));
  while (!pending_requests_.is_empty()) {
    RequestHandler* request_handler
        = static_cast<RequestHandler*>(pending_requests_.front());
    pending_requests_.remove(request_handler);
    request_handler->stop_timer();
    request_handler->retry(RETRY_WITH_NEXT_HOST);
  }
}

void Pool::connect() {
  LOG_DEBUG("Connect %s pool(%p)",
            address_.to_string().c_str(), static_cast<void*>(this));
  if (state_ == POOL_STATE_NEW) {
    for (unsigned i = 0; i < config_.core_connections_per_host(); ++i) {
      spawn_connection();
    }
    state_ = POOL_STATE_CONNECTING;
    maybe_notify_ready();
  }
}

void Pool::close(bool cancel_reconnect) {
  if (state_ != POOL_STATE_CLOSING && state_ != POOL_STATE_CLOSED) {
    LOG_DEBUG("Closing pool(%p)", static_cast<void*>(this));
    // We're closing before we've connected (likely because of an error), we need
    // to notify we're "ready"
    if (state_ == POOL_STATE_CONNECTING) {
      state_ = POOL_STATE_CLOSING;
      io_worker_->notify_pool_ready(this);
    } else {
      state_ = POOL_STATE_CLOSING;
    }

    set_is_available(false);
    cancel_reconnect_ = cancel_reconnect;

    for (ConnectionVec::iterator it = connections_.begin(),
                                 end = connections_.end();
         it != end; ++it) {
      (*it)->close();
    }
    for (ConnectionSet::iterator it = connections_pending_.begin(),
                                 end = connections_pending_.end();
         it != end; ++it) {
      (*it)->close();
    }
  }

  maybe_close();
}

Connection* Pool::borrow_connection() {
  if (connections_.empty()) {
    for (unsigned i = 0; i < config_.core_connections_per_host(); ++i) {
      maybe_spawn_connection();
    }
    return NULL;
  }

  Connection* connection = find_least_busy();

  if (connection == NULL ||
      connection->pending_request_count() >=
        config_.max_concurrent_requests_threshold()) {
      maybe_spawn_connection();
  }

  return connection;
}

void Pool::return_connection(Connection* connection) {
  if (!connection->is_ready() || pending_requests_.is_empty()) return;
  RequestHandler* request_handler
      = static_cast<RequestHandler*>(pending_requests_.front());
  remove_pending_request(request_handler);
  request_handler->stop_timer();
  if (!write(connection, request_handler)) {
    request_handler->retry(RETRY_WITH_NEXT_HOST);
  }
}

void Pool::add_pending_request(RequestHandler* request_handler) {
  pending_requests_.add_to_back(request_handler);

  if (pending_requests_.size() % 10 == 0) {
    LOG_DEBUG("%u request%s pending on %s pool(%p)",
              static_cast<unsigned int>(pending_requests_.size() + 1),
              pending_requests_.size() > 0 ? "s":"",
              address_.to_string().c_str(),
              static_cast<void*>(this));
  }

  if (pending_requests_.size() > config_.pending_requests_high_water_mark()) {
    LOG_WARN("Exceeded pending requests water mark (current: %u water mark: %u) for host %s",
             static_cast<unsigned int>(pending_requests_.size()),
             config_.pending_requests_high_water_mark(),
             address_.to_string().c_str());
    set_is_available(false);
    metrics_->exceeded_pending_requests_water_mark.inc();
  }
}

void Pool::remove_pending_request(RequestHandler* request_handler) {
  pending_requests_.remove(request_handler);
  set_is_available(true);
}

void Pool::set_is_available(bool is_available) {
  if (is_available) {
    if (!is_available_ &&
        available_connection_count_ > 0 &&
        pending_requests_.size() < config_.pending_requests_low_water_mark()) {
      io_worker_->set_host_is_available(address_, true);
      is_available_ = true;
    }
  } else {
    if (is_available_) {
      io_worker_->set_host_is_available(address_, false);
      is_available_ = false;
    }
  }
}

bool Pool::write(Connection* connection, RequestHandler* request_handler) {
  request_handler->set_pool(this);
  if (io_worker_->is_current_keyspace(connection->keyspace())) {
    if (!connection->write(request_handler, false)) {
      return false;
    }
  } else {
    LOG_DEBUG("Setting keyspace %s on connection(%p) pool(%p)",
              io_worker_->keyspace().c_str(),
              static_cast<void*>(connection),
              static_cast<void*>(this));
    if (!connection->write(new SetKeyspaceHandler(connection, io_worker_->keyspace(),
                                                 request_handler), false)) {
      return false;
    }
  }
  if (!is_pending_flush_) {
    io_worker_->add_pending_flush(this);
  }
  is_pending_flush_ = true;
  return true;
}

void Pool::flush() {
  is_pending_flush_ = false;
  for (ConnectionVec::iterator it = connections_.begin(),
       end = connections_.end(); it != end; ++it) {
    (*it)->flush();
  }
}

void Pool::maybe_notify_ready() {
  // This will notify ready even if all the connections fail.
  // it is up to the holder to inspect state
  if (state_ == POOL_STATE_CONNECTING && connections_pending_.empty()) {
    state_ = POOL_STATE_READY;
    io_worker_->notify_pool_ready(this);
  }
}

void Pool::maybe_close() {
  if (state_ == POOL_STATE_CLOSING && connections_.empty() &&
      connections_pending_.empty()) {
    state_ = POOL_STATE_CLOSED;
    io_worker_->notify_pool_closed(this);
  }
}

void Pool::spawn_connection() {
  if (state_ != POOL_STATE_CLOSING && state_ != POOL_STATE_CLOSED) {
    Connection* connection =
        new Connection(loop_, config_, metrics_,
                       address_,
                       io_worker_->keyspace(),
                       io_worker_->protocol_version(),
                       this);

    LOG_INFO("Spawning new connection to host %s", address_.to_string(true).c_str());
    connection->connect();

    connections_pending_.insert(connection);
  }
}

void Pool::maybe_spawn_connection() {
  if (connections_pending_.size() >= config_.max_concurrent_creation()) {
    return;
  }

  if (connections_.size() + connections_pending_.size() >=
      config_.max_connections_per_host()) {
    return;
  }

  if (state_ != POOL_STATE_READY) {
    return;
  }

  spawn_connection();
}

Connection* Pool::find_least_busy() {
  ConnectionVec::iterator it = std::min_element(
      connections_.begin(), connections_.end(), least_busy_comp);
  if ((*it)->is_ready() && (*it)->available_streams() > 0) {
    return *it;
  }
  return NULL;
}

void Pool::on_ready(Connection* connection) {
  connections_pending_.erase(connection);
  connections_.push_back(connection);
  return_connection(connection);

  maybe_notify_ready();

  metrics_->total_connections.inc();
}

void Pool::on_close(Connection* connection) {
  connections_pending_.erase(connection);

  ConnectionVec::iterator it =
      std::find(connections_.begin(), connections_.end(), connection);
  if (it != connections_.end()) {
    connections_.erase(it);
    metrics_->total_connections.dec();
  }

  if (connection->is_defunct()) {
    // If at least one connection has a critical failure then don't try to
    // reconnect automatically.
    if (connection->is_critical_failure()) {
      is_critical_failure_ = true;
    }
    close();
  } else {
    maybe_notify_ready();
    maybe_close();
  }
}

void Pool::on_availability_change(Connection* connection) {
  if (connection->is_available()) {
    ++available_connection_count_;
    set_is_available(true);

    metrics_->available_connections.inc();
  } else {
    --available_connection_count_;
    assert(available_connection_count_ >= 0);
    if (available_connection_count_ == 0) {
      set_is_available(false);
    }

    metrics_->available_connections.dec();
  }
}

void Pool::on_pending_request_timeout(RequestTimer* timer) {
  RequestHandler* request_handler = static_cast<RequestHandler*>(timer->data());
  Pool* pool = request_handler->pool();
  pool->metrics_->pending_request_timeouts.inc();
  pool->remove_pending_request(request_handler);
  request_handler->retry(RETRY_WITH_NEXT_HOST);
  LOG_DEBUG("Timeout waiting for connection to %s pool(%p)",
            pool->address_.to_string().c_str(),
            static_cast<void*>(pool));
  pool->maybe_close();
}

void Pool::wait_for_connection(RequestHandler* request_handler) {
  request_handler->set_pool(this);
  request_handler->start_timer(loop_,
                               config_.connect_timeout_ms(),
                               request_handler,
                               Pool::on_pending_request_timeout);
  add_pending_request(request_handler);
}

} // namespace cass
