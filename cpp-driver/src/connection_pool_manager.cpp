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

#include "connection_pool_manager.hpp"

#include "config.hpp"
#include "memory.hpp"
#include "request_queue.hpp"
#include "scoped_lock.hpp"

namespace cass {

ConnectionPoolManagerSettings::ConnectionPoolManagerSettings(const Config& config)
  : connection_settings(config)
  , num_connections_per_host(config.core_connections_per_host())
  , reconnect_wait_time_ms(config.reconnect_wait_time_ms()) { }

ConnectionPoolManager::ConnectionPoolManager(RequestQueueManager* request_queue_manager,
                                             int protocol_version,
                                             const String& keyspace,
                                             ConnectionPoolManagerListener* listener,
                                             Metrics* metrics,
                                             const ConnectionPoolManagerSettings& settings)
  : request_queue_manager_(request_queue_manager)
  , protocol_version_(protocol_version)
  , listener_(listener)
  , settings_(settings)
  , close_state_(CLOSE_STATE_OPEN)
  , keyspace_(keyspace)
  , metrics_(metrics) {
  inc_ref(); // Reference for the lifetime of the connection pools
  uv_rwlock_init(&rwlock_);
  uv_rwlock_init(&keyspace_rwlock_);
  pools_.set_empty_key(Address::EMPTY_KEY);
  pools_.set_deleted_key(Address::DELETED_KEY);
}

ConnectionPoolManager::~ConnectionPoolManager() {
  uv_rwlock_destroy(&rwlock_);
  uv_rwlock_destroy(&keyspace_rwlock_);
}

PooledConnection::Ptr ConnectionPoolManager::find_least_busy(const Address& address) const {
  ConnectionPool::Ptr pool;
  { // Locked
    ScopedReadLock rl(&rwlock_);
    ConnectionPool::Map::const_iterator it = pools_.find(address);
    if (it == pools_.end()) {
      return PooledConnection::Ptr();
    }
    pool = it->second;
  }
  return pool->find_least_busy();
}

AddressVec ConnectionPoolManager::available() const {
  AddressVec result;
  ScopedReadLock rl(&rwlock_);
  result.reserve(pools_.size());
  for (ConnectionPool::Map::const_iterator it = pools_.begin(),
       end = pools_.end(); it != end; ++it) {
    result.push_back(it->first);
  }
  return result;
}

void ConnectionPoolManager::add(const Address& address) {
  // TODO: Potentially used double check here to minimize what's in the lock
  ScopedWriteLock wl(&rwlock_);
  ConnectionPool::Map::iterator it = pools_.find(address);
  if (it != pools_.end()) return;

  for (ConnectionPoolConnector::Vec::iterator it = pending_pools_.begin(),
       end = pending_pools_.end(); it != end; ++it) {
    if ((*it)->address() == address) return;
  }

  ConnectionPoolConnector::Ptr connector(
        Memory::allocate<ConnectionPoolConnector>(this,
                                                  address,
                                                  this,
                                                  on_connect));
  pending_pools_.push_back(connector);
  connector->connect(request_queue_manager_->event_loop_group());
}

void ConnectionPoolManager::remove(const Address& address) {
  ScopedReadLock rl(&rwlock_);
  ConnectionPool::Map::iterator it = pools_.find(address);
  if (it == pools_.end()) return;
  // The connection pool will remove itself from the manager when all of its
  // connections are closed.
  it->second->close();
}

void ConnectionPoolManager::close() {
  ScopedWriteLock wl(&rwlock_);
  if (close_state_ == CLOSE_STATE_OPEN) {
    close_state_ = CLOSE_STATE_CLOSING;
    for (ConnectionPool::Map::iterator it = pools_.begin(),
         end = pools_.end(); it != end; ++it) {
      it->second->close();
    }

    for (ConnectionPoolConnector::Vec::iterator it = pending_pools_.begin(),
         end = pending_pools_.end(); it != end; ++it) {
      (*it)->cancel();
    }
  }
  maybe_closed(wl);
}

EventLoopGroup* ConnectionPoolManager::event_loop_group() const {
  return request_queue_manager_->event_loop_group();
}

String ConnectionPoolManager::keyspace() const {
  ScopedReadLock rl(&keyspace_rwlock_);
  return keyspace_;
}

void ConnectionPoolManager::set_keyspace(const String& keyspace) {
  ScopedWriteLock wl(&keyspace_rwlock_);
  keyspace_ = keyspace;
}

void ConnectionPoolManager::add_pool(const ConnectionPool::Ptr& pool, Protected) {
  ScopedWriteLock wl(&rwlock_);
  internal_add_pool(pool);
}

void ConnectionPoolManager::notify_closed(ConnectionPool* pool, bool should_notify_down, Protected) {
  ScopedWriteLock wl(&rwlock_);
  pools_.erase(pool->address());
  if (should_notify_down && listener_ != NULL) {
    listener_->on_down(pool->address());
  }
  maybe_closed(wl);
}

void ConnectionPoolManager::notify_up(ConnectionPool* pool, Protected) {
  if (listener_ != NULL) {
    listener_->on_up(pool->address());
  }
}

void ConnectionPoolManager::notify_down(ConnectionPool* pool, Protected) {
  if (listener_ != NULL) {
    listener_->on_down(pool->address());
  }
}

void ConnectionPoolManager::notify_critical_error(ConnectionPool* pool,
                                                  Connector::ConnectionError code,
                                                  const String& message,
                                                  Protected) {
  if (listener_ != NULL) {
    listener_->on_critical_error(pool->address(), code, message);
  }
}

void ConnectionPoolManager::internal_add_pool(const ConnectionPool::Ptr& pool) {
  LOG_DEBUG("Adding pool for host %s", pool->address().to_string().c_str());
  pools_[pool->address()] = pool;
}

// This must be the last call in a function because it can potentially
// deallocate the manager.
void ConnectionPoolManager::maybe_closed(ScopedWriteLock& wl) {
  if (close_state_ == CLOSE_STATE_CLOSING && pools_.empty()) {
    close_state_ = CLOSE_STATE_CLOSED;
    wl.unlock(); // The manager is destroyed in this step it must be unlocked
    if (listener_ != NULL) {
      listener_->on_close();
    }
    dec_ref();
  }
}

void ConnectionPoolManager::on_connect(ConnectionPoolConnector* pool_connector) {
  ConnectionPoolManager* manager = static_cast<ConnectionPoolManager*>(pool_connector->data());
  manager->handle_connect(pool_connector);
}

void ConnectionPoolManager::handle_connect(ConnectionPoolConnector* pool_connector) {
  ScopedWriteLock wl(&rwlock_);
  pending_pools_.erase(std::remove(pending_pools_.begin(), pending_pools_.end(), pool_connector),
                       pending_pools_.end());
  if (pool_connector->is_ok()) {
    internal_add_pool(pool_connector->release_pool());
  } else if (listener_) {
      listener_->on_critical_error(pool_connector->address(),
                                   pool_connector->error_code(),
                                   pool_connector->error_message());
  }
}

} // namespace cass
