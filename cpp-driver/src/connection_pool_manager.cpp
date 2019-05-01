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

#include "scoped_lock.hpp"
#include "utils.hpp"

using namespace datastax;
using namespace datastax::internal::core;

class NopConnectionPoolManagerListener : public ConnectionPoolManagerListener {
public:
  virtual void on_pool_up(const Address& address) {}

  virtual void on_pool_down(const Address& address) {}

  virtual void on_pool_critical_error(const Address& address, Connector::ConnectionError code,
                                      const String& message) {}

  virtual void on_close(ConnectionPoolManager* manager) {}
};

static NopConnectionPoolManagerListener nop_connection_pool_manager_listener__;

ConnectionPoolManager::ConnectionPoolManager(const ConnectionPool::Map& pools, uv_loop_t* loop,
                                             ProtocolVersion protocol_version,
                                             const String& keyspace,
                                             ConnectionPoolManagerListener* listener,
                                             Metrics* metrics,
                                             const ConnectionPoolSettings& settings)
    : loop_(loop)
    , protocol_version_(protocol_version)
    , settings_(settings)
    , listener_(listener ? listener : &nop_connection_pool_manager_listener__)
    , close_state_(CLOSE_STATE_OPEN)
    , keyspace_(keyspace)
    , metrics_(metrics)
#ifdef CASS_INTERNAL_DIAGNOSTICS
    , flush_bytes_("flushed")
#endif
{
  inc_ref(); // Reference for the lifetime of the connection pools
  set_pointer_keys(to_flush_);

  for (ConnectionPool::Map::const_iterator it = pools.begin(), end = pools.end(); it != end; ++it) {
    it->second->set_listener(this);
    add_pool(it->second);
  }
}

PooledConnection::Ptr ConnectionPoolManager::find_least_busy(const Address& address) const {
  ConnectionPool::Map::const_iterator it = pools_.find(address);
  if (it == pools_.end()) {
    return PooledConnection::Ptr();
  }
  return it->second->find_least_busy();
}

bool ConnectionPoolManager::has_connections(const Address& address) const {
  ConnectionPool::Map::const_iterator it = pools_.find(address);
  return it != pools_.end() && it->second->has_connections();
}

void ConnectionPoolManager::flush() {
  for (DenseHashSet<ConnectionPool*>::const_iterator it = to_flush_.begin(), end = to_flush_.end();
       it != end; ++it) {
    (*it)->flush();
  }
  to_flush_.clear();
}

AddressVec ConnectionPoolManager::available() const {
  AddressVec result;
  result.reserve(pools_.size());
  for (ConnectionPool::Map::const_iterator it = pools_.begin(), end = pools_.end(); it != end;
       ++it) {
    result.push_back(it->first);
  }
  return result;
}

void ConnectionPoolManager::add(const Host::Ptr& host) {
  ConnectionPool::Map::iterator it = pools_.find(host->address());
  if (it != pools_.end()) return;

  for (ConnectionPoolConnector::Vec::iterator it = pending_pools_.begin(),
                                              end = pending_pools_.end();
       it != end; ++it) {
    if ((*it)->address() == host->address()) return;
  }

  ConnectionPoolConnector::Ptr connector(new ConnectionPoolConnector(
      host, protocol_version_, bind_callback(&ConnectionPoolManager::on_connect, this)));
  pending_pools_.push_back(connector);
  connector->with_listener(this)
      ->with_keyspace(keyspace_)
      ->with_metrics(metrics_)
      ->with_settings(settings_)
      ->connect(loop_);
}

void ConnectionPoolManager::remove(const Address& address) {
  ConnectionPool::Map::iterator it = pools_.find(address);
  if (it == pools_.end()) return;
  // The connection pool will remove itself from the manager when all of its
  // connections are closed.
  it->second->close();
}

void ConnectionPoolManager::close() {
  if (close_state_ == CLOSE_STATE_OPEN) {
    close_state_ = CLOSE_STATE_CLOSING;

    // Make copies of pool/connector data structures to prevent iterator
    // invalidation.

    ConnectionPool::Map pools(pools_);
    for (ConnectionPool::Map::iterator it = pools.begin(), end = pools.end(); it != end; ++it) {
      it->second->close();
    }

    ConnectionPoolConnector::Vec pending_pools(pending_pools_);
    for (ConnectionPoolConnector::Vec::iterator it = pending_pools.begin(),
                                                end = pending_pools.end();
         it != end; ++it) {
      (*it)->cancel();
    }

    close_state_ = CLOSE_STATE_WAITING_FOR_POOLS;
    maybe_closed();
  }
}

void ConnectionPoolManager::attempt_immediate_connect(const Address& address) {
  ConnectionPool::Map::iterator it = pools_.find(address);
  if (it == pools_.end()) return;
  it->second->attempt_immediate_connect();
}

void ConnectionPoolManager::set_listener(ConnectionPoolManagerListener* listener) {
  listener_ = listener ? listener : &nop_connection_pool_manager_listener__;
}

void ConnectionPoolManager::set_keyspace(const String& keyspace) {
  keyspace_ = keyspace;
  for (ConnectionPool::Map::iterator it = pools_.begin(), end = pools_.end(); it != end; ++it) {
    it->second->set_keyspace(keyspace);
  }
}

void ConnectionPoolManager::on_pool_up(const Address& address) { listener_->on_pool_up(address); }

void ConnectionPoolManager::on_pool_down(const Address& address) {
  listener_->on_pool_down(address);
}

void ConnectionPoolManager::on_pool_critical_error(const Address& address,
                                                   Connector::ConnectionError code,
                                                   const String& message) {
  listener_->on_pool_critical_error(address, code, message);
}

void ConnectionPoolManager::on_requires_flush(ConnectionPool* pool) {
  if (to_flush_.empty()) {
    listener_->on_requires_flush();
  }
  to_flush_.insert(pool);
}

void ConnectionPoolManager::on_close(ConnectionPool* pool) {
  pools_.erase(pool->address());
  to_flush_.erase(pool);
  maybe_closed();
}

void ConnectionPoolManager::add_pool(const ConnectionPool::Ptr& pool) {
  LOG_DEBUG("Adding pool for host %s", pool->address().to_string().c_str());
  pools_[pool->address()] = pool;
}

// This must be the last call in a function because it can potentially
// deallocate the manager.
void ConnectionPoolManager::maybe_closed() {
  // Close the manager once all current and pending pools are terminated.
  if (close_state_ == CLOSE_STATE_WAITING_FOR_POOLS && pools_.empty() && pending_pools_.empty()) {
    close_state_ = CLOSE_STATE_CLOSED;
    listener_->on_close(this);
    dec_ref();
  }
}

void ConnectionPoolManager::on_connect(ConnectionPoolConnector* pool_connector) {
  pending_pools_.erase(std::remove(pending_pools_.begin(), pending_pools_.end(), pool_connector),
                       pending_pools_.end());

  if (close_state_ != CLOSE_STATE_OPEN) {
    maybe_closed();
    return;
  }

  if (pool_connector->is_ok()) {
    add_pool(pool_connector->release_pool());
  } else {
    listener_->on_pool_critical_error(pool_connector->address(), pool_connector->error_code(),
                                      pool_connector->error_message());
  }
}
