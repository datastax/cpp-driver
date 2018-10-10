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

#include "config.hpp"
#include "connection_pool_manager.hpp"
#include "memory.hpp"
#include "metrics.hpp"
#include "utils.hpp"

#include <algorithm>


namespace cass {

static inline bool least_busy_comp(const PooledConnection::Ptr& a,
                                   const PooledConnection::Ptr& b) {
  return a->inflight_request_count() < b->inflight_request_count();
}

ConnectionPoolSettings::ConnectionPoolSettings()
  : num_connections_per_host(CASS_DEFAULT_NUM_CONNECTIONS_PER_HOST)
  , reconnect_wait_time_ms(CASS_DEFAULT_RECONNECT_WAIT_TIME_MS) { }

ConnectionPoolSettings::ConnectionPoolSettings(const Config& config)
  : connection_settings(config)
  , num_connections_per_host(config.core_connections_per_host())
  , reconnect_wait_time_ms(config.reconnect_wait_time_ms()) { }

class NopConnectionPoolListener : public ConnectionPoolListener {
public:
  virtual void on_pool_up(const Address& address) { }

  virtual void on_pool_down(const Address& address) { }

  virtual void on_pool_critical_error(const Address& address,
                                 Connector::ConnectionError code,
                                 const String& message) { }

  virtual void on_close(ConnectionPool* pool) { }
};

NopConnectionPoolListener nop_connection_pool_listener__;

ConnectionPool::ConnectionPool(const Connection::Vec& connections,
                               ConnectionPoolListener* listener,
                               const String& keyspace,
                               uv_loop_t* loop,
                               const Address& address,
                               int protocol_version,
                               const ConnectionPoolSettings& settings,
                               Metrics* metrics)
  : listener_(listener ? listener : &nop_connection_pool_listener__)
  , keyspace_(keyspace)
  , loop_(loop)
  , address_(address)
  , protocol_version_(protocol_version)
  , settings_(settings)
  , metrics_(metrics)
  , close_state_(CLOSE_STATE_OPEN)
  , notify_state_(NOTIFY_STATE_NEW) {
  inc_ref(); // Reference for the lifetime of the pooled connections
  set_pointer_keys(to_flush_);

  for (Connection::Vec::const_iterator it = connections.begin(),
       end = connections.end(); it != end; ++it) {
    const Connection::Ptr& connection(*it);
    if (!connection->is_closing()) {
      add_connection(PooledConnection::Ptr(
                       Memory::allocate<PooledConnection>(this, connection)));
    }
  }

  notify_up_or_down();

  // We had non-critical errors or some connections closed
  assert(connections.size() <= settings_.num_connections_per_host);
  size_t needed = settings_.num_connections_per_host - connections_.size();
  for (size_t i = 0; i < needed; ++i) {
    schedule_reconnect();
  }
}

PooledConnection::Ptr ConnectionPool::find_least_busy() const {
  if (connections_.empty()) {
    return PooledConnection::Ptr();
  }
  return *std::min_element(connections_.begin(),
                           connections_.end(), least_busy_comp);
}

void ConnectionPool::flush() {
  for (DenseHashSet<PooledConnection*>::const_iterator it = to_flush_.begin(),
       end = to_flush_.end(); it != end; ++it) {
    (*it)->flush();
  }
  to_flush_.clear();
}

void ConnectionPool::close() {
  internal_close();
}

void ConnectionPool::set_listener(ConnectionPoolListener* listener) {
  listener_ = listener ? listener : &nop_connection_pool_listener__;
}

void ConnectionPool::set_keyspace(const String& keyspace) {
  keyspace_ = keyspace;
}

void ConnectionPool::requires_flush(PooledConnection* connection, ConnectionPool::Protected) {
  if (to_flush_.empty()) {
    listener_->on_requires_flush(this);
  }
  to_flush_.insert(connection);
}

void ConnectionPool::close_connection(PooledConnection* connection, Protected) {
  if (metrics_) {
    metrics_->total_connections.dec();
  }
  connections_.erase(std::remove(connections_.begin(), connections_.end(), connection),
                     connections_.end());
  to_flush_.erase(connection);

  if (close_state_ != CLOSE_STATE_OPEN) {
    maybe_closed();
    return;
  }

  // When there are no more connections available then notify that the host
  // is down.
  notify_up_or_down();
  schedule_reconnect();
}

void ConnectionPool::add_connection(const PooledConnection::Ptr& connection) {
  if (metrics_) {
    metrics_->total_connections.inc();
  }
  connections_.push_back(connection);
}

void ConnectionPool::notify_up_or_down() {
  if ((notify_state_ == NOTIFY_STATE_NEW || notify_state_ == NOTIFY_STATE_UP) &&
      connections_.empty()) {
    notify_state_ = NOTIFY_STATE_DOWN;
    listener_->on_pool_down(address_);
  } else if ((notify_state_ == NOTIFY_STATE_NEW || notify_state_ == NOTIFY_STATE_DOWN) &&
             !connections_.empty()) {
    notify_state_ = NOTIFY_STATE_UP;
    listener_->on_pool_up(address_);
  }
}

void ConnectionPool::notify_critical_error(Connector::ConnectionError code,
                                                    const String& message) {
  if (notify_state_ != NOTIFY_STATE_CRITICAL) {
    notify_state_ = NOTIFY_STATE_CRITICAL;
    listener_->on_pool_critical_error(address_, code, message);
  }
}

void ConnectionPool::schedule_reconnect() {
  LOG_INFO("Scheduling reconnect for host %s in %llu ms on connection pool (%p)",
           address_.to_string().c_str(),
           static_cast<unsigned long long>(settings_.reconnect_wait_time_ms),
           static_cast<void*>(this));
  DelayedConnector::Ptr connector(
        Memory::allocate<DelayedConnector>(address_,
                                           protocol_version_,
                                           bind_callback(&ConnectionPool::on_reconnect, this)));
  pending_connections_.push_back(connector);
  connector
      ->with_keyspace(keyspace())
      ->with_metrics(metrics_)
      ->with_settings(settings_.connection_settings)
      ->delayed_connect(loop_, settings_.reconnect_wait_time_ms);
}

void ConnectionPool::internal_close() {
  if (close_state_ == CLOSE_STATE_OPEN) {
    close_state_ = CLOSE_STATE_CLOSING;

    // Make copies of connection/connector data structures to prevent iterator
    // invalidation.

    PooledConnection::Vec connections(connections_);
    for (PooledConnection::Vec::iterator it = connections.begin(),
         end = connections.end(); it != end; ++it) {
      (*it)->close();
    }

    DelayedConnector::Vec pending_connections(pending_connections_);
    for (DelayedConnector::Vec::iterator it = pending_connections.begin(),
         end = pending_connections.end(); it != end; ++it) {
      (*it)->cancel();
    }

    close_state_ = CLOSE_STATE_WAITING_FOR_CONNECTIONS;
    maybe_closed();
  }
}

void ConnectionPool::maybe_closed() {
  // Remove the pool once all current connections and pending connections
  // are terminated.
  if (close_state_ == CLOSE_STATE_WAITING_FOR_CONNECTIONS && connections_.empty() && pending_connections_.empty()) {
    close_state_ = CLOSE_STATE_CLOSED;
    // Only mark DOWN if it's UP otherwise we might get multiple DOWN events
    // when connecting the pool.
    if (notify_state_ == NOTIFY_STATE_UP) {
      listener_->on_pool_down(address_);
    }
    listener_->on_close(this);
    dec_ref();
  }
}

void ConnectionPool::on_reconnect(DelayedConnector* connector) {
  pending_connections_.erase(std::remove(pending_connections_.begin(), pending_connections_.end(), connector),
                             pending_connections_.end());

  if (close_state_ != CLOSE_STATE_OPEN) {
    maybe_closed();
    return;
  }

  if (connector->is_ok()) {
    add_connection(
          PooledConnection::Ptr(
            Memory::allocate<PooledConnection>(this, connector->release_connection())));
    notify_up_or_down();
  } else if (!connector->is_canceled()) {
    if(connector->is_critical_error()) {
      LOG_ERROR("Closing established connection pool to host %s because of the following error: %s",
                address().to_string().c_str(),
                connector->error_message().c_str());
      notify_critical_error(connector->error_code(),
                                     connector->error_message());
      internal_close();
    } else {
      LOG_WARN("Connection pool was unable to reconnect to host %s because of the following error: %s",
               address().to_string().c_str(),
               connector->error_message().c_str());
      schedule_reconnect();
    }
  }
}

} // namespace cass
