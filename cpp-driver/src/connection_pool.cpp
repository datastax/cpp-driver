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

#include "connection_pool.hpp"

#include "connection_pool_manager.hpp"
#include "memory.hpp"
#include "metrics.hpp"
#include "scoped_lock.hpp"

#include <algorithm>


namespace cass {

static inline bool least_busy_comp(const PooledConnection::Ptr& a,
                                   const PooledConnection::Ptr& b) {
  return a->total_request_count() < b->total_request_count();
}

ConnectionPool::ConnectionPool(ConnectionPoolManager* manager,
                               const Address& address)
  : manager_(manager)
  , address_(address)
  , close_state_(CLOSE_STATE_OPEN)
  , notify_state_(NOTIFY_STATE_NEW) {
  inc_ref(); // Reference for the lifetime of the pooled connections
  uv_rwlock_init(&rwlock_);
}

ConnectionPool::~ConnectionPool() {
  uv_rwlock_destroy(&rwlock_);
}

PooledConnection::Ptr ConnectionPool::find_least_busy() const {
  ScopedReadLock rl(&rwlock_);
  if (connections_.empty()) {
    return PooledConnection::Ptr();
  }
  return *std::min_element(connections_.begin(),
                           connections_.end(), least_busy_comp);
}

void ConnectionPool::close() {
  ScopedWriteLock wl(&rwlock_);
  internal_close(wl);
}

void ConnectionPool::schedule_reconnect(EventLoop* event_loop, Protected) {
  ScopedWriteLock wl(&rwlock_);
  internal_schedule_reconnect(wl, event_loop);
}

void ConnectionPool::add_connection(const PooledConnection::Ptr& connection, Protected) {
  ScopedWriteLock wl(&rwlock_);
  internal_add_connection(wl, connection);
}

void ConnectionPool::close_connection(PooledConnection* connection, Protected) {
  ScopedWriteLock wl(&rwlock_);
  if (manager_->metrics()) {
    manager_->metrics()->total_connections.dec();
  }
  connections_.erase(std::remove(connections_.begin(), connections_.end(), connection),
                     connections_.end());

  if (close_state_ != CLOSE_STATE_OPEN) {
    maybe_closed(wl);
    return;
  }

  // When there are no more connections available then notify that the host
  // is down.
  internal_notify_up_or_down(wl);
  internal_schedule_reconnect(wl, connection->event_loop());
}

void ConnectionPool::notify_up_or_down(ConnectionPool::Protected) {
  ScopedWriteLock wl(&rwlock_);
  internal_notify_up_or_down(wl);
}

void ConnectionPool::notify_critical_error(Connector::ConnectionError code,
                                           const String& message,
                                           ConnectionPool::Protected) {
  ScopedWriteLock wl(&rwlock_);
  internal_notify_critical_error(wl, code, message);
}

// Lock must be held to call this method
void ConnectionPool::internal_schedule_reconnect(ScopedWriteLock& wl,
                                                 EventLoop* event_loop) {
  // Reschedule a new connection on the same event loop
  LOG_INFO("Scheduling reconnect for host %s in %llu ms on connection pool (%p)",
           address_.to_string().c_str(),
           static_cast<unsigned long long>(manager_->settings().reconnect_wait_time_ms),
           static_cast<void*>(this));
  PooledConnector::Ptr connector(Memory::allocate<PooledConnector>(this, this, on_reconnect));
  pending_connections_.push_back(connector);
  connector->delayed_connect(event_loop,
                             manager_->settings().reconnect_wait_time_ms,
                             PooledConnector::Protected());
}

void ConnectionPool::internal_notify_up_or_down(ScopedWriteLock& wl) {
  if ((notify_state_ == NOTIFY_STATE_NEW || notify_state_ == NOTIFY_STATE_UP) &&
      connections_.empty()) {
    notify_state_ = NOTIFY_STATE_DOWN;
    manager_->notify_down(this, ConnectionPoolManager::Protected());
  } else if ((notify_state_ == NOTIFY_STATE_NEW || notify_state_ == NOTIFY_STATE_DOWN) &&
             !connections_.empty()) {
    notify_state_ = NOTIFY_STATE_UP;
    manager_->notify_up(this, ConnectionPoolManager::Protected());
  }
}

void ConnectionPool::internal_notify_critical_error(ScopedWriteLock& wl,
                                                    Connector::ConnectionError code,
                                                    const String& message) {
  if (notify_state_ != NOTIFY_STATE_CRITICAL) {
    notify_state_ = NOTIFY_STATE_CRITICAL;
    manager_->notify_critical_error(this, code, message,
                                    ConnectionPoolManager::Protected());
  }
}

void ConnectionPool::internal_add_connection(ScopedWriteLock& wl,
                                             const PooledConnection::Ptr& connection) {
  if (manager_->metrics()) {
    manager_->metrics()->total_connections.inc();
  }
  connections_.push_back(connection);
}


void ConnectionPool::internal_close(ScopedWriteLock& wl) {
  if (close_state_ == CLOSE_STATE_OPEN) {
    close_state_ = CLOSE_STATE_CLOSING;
    for (PooledConnection::Vec::iterator it = connections_.begin(),
         end = connections_.end(); it != end; ++it) {
      (*it)->close();
    }
    for (PooledConnector::Vec::iterator it = pending_connections_.begin(),
         end = pending_connections_.end(); it != end; ++it) {
      (*it)->cancel();
    }
  }
  maybe_closed(wl);
}

void ConnectionPool::maybe_closed(ScopedWriteLock& wl) {
  // Remove the pool once all current connections and pending connections
  // are terminated.
  if (close_state_ == CLOSE_STATE_CLOSING && connections_.empty() && pending_connections_.empty()) {
    close_state_ = CLOSE_STATE_CLOSED;
    // Only mark DOWN if it's UP otherwise we might get multiple DOWN events
    // when connecting the pool.
    bool should_notify_down = (notify_state_ == NOTIFY_STATE_UP);
    wl.unlock(); // The pool is destroyed in this step it must be unlocked
    manager_->notify_closed(this, should_notify_down, ConnectionPoolManager::Protected());
    dec_ref();
  }
}

void ConnectionPool::on_reconnect(PooledConnector* connector, EventLoop* event_loop) {
  ConnectionPool* pool = static_cast<ConnectionPool*>(connector->data());
  pool->handle_reconnect(connector, event_loop);
}

void ConnectionPool::handle_reconnect(PooledConnector* connector, EventLoop* event_loop) {
  ScopedWriteLock wl(&rwlock_);
  pending_connections_.erase(std::remove(pending_connections_.begin(), pending_connections_.end(), connector),
                             pending_connections_.end());

  if (close_state_ !=  CLOSE_STATE_OPEN) {
    maybe_closed(wl);
    return;
  }

  if (connector->is_ok()) {
    internal_add_connection(wl,
                            connector->release_connection());
    internal_notify_up_or_down(wl);
  } else if (!connector->is_cancelled()) {
    if(connector->is_critical_error()) {
      LOG_ERROR("Closing established connection pool to host %s because of the following error: %s",
                address().to_string().c_str(),
                connector->error_message().c_str());
      internal_notify_critical_error(wl,
                                     connector->error_code(),
                                     connector->error_message());
      internal_close(wl);
    } else {
      LOG_WARN("Connection pool was unable to reconnect to host %s because of the following error: %s",
               address().to_string().c_str(),
               connector->error_message().c_str());
      internal_schedule_reconnect(wl, event_loop);
    }
  }
}

} // namespace cass
