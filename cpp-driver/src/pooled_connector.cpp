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

#include "pooled_connector.hpp"

#include "connection_pool.hpp"
#include "connection_pool_manager.hpp"
#include "event_loop.hpp"
#include "memory.hpp"

namespace cass {

PooledConnector::PooledConnector(ConnectionPool* pool, const Callback& callback)
  : pool_(pool)
  , connector_(Memory::allocate<Connector>(pool->address(), pool->manager()->protocol_version(),
                                           bind_callback(&PooledConnector::on_connect, this)))
  , callback_(callback)
  , is_canceled_(false) { }

void PooledConnector::connect() {
  inc_ref();
  internal_connect();
}

void PooledConnector::cancel() {
  is_canceled_ = true;
  if (delayed_connect_timer_.is_running()) {
    delayed_connect_timer_.stop();
    callback_(this);
    dec_ref();
  } else {
    connector_->cancel();
  }
}

PooledConnection::Ptr PooledConnector::release_connection() {
  PooledConnection::Ptr temp = connection_;
  connection_.reset();
  return temp;
}

bool PooledConnector::is_canceled() const {
  return is_canceled_;
}

bool PooledConnector::is_ok() const {
  return !is_canceled() && connector_->is_ok();
}

bool PooledConnector::is_critical_error() const {
  return connector_->is_auth_error() ||
      connector_->is_ssl_error() ||
      connector_->is_invalid_protocol() ||
      connector_->is_keyspace_error();
}

bool PooledConnector::is_keyspace_error() const {
  return connector_->is_keyspace_error();
}

void PooledConnector::delayed_connect(uint64_t wait_time_ms, Protected) {
  if (is_canceled_) {
    callback_(this);
  } else {
    inc_ref();
    if (wait_time_ms > 0) {
      delayed_connect_timer_.start(pool_->manager()->loop(), wait_time_ms,
                                   bind_callback(&PooledConnector::on_delayed_connect, this));
    } else {
      internal_connect();
    }
  }
}

void PooledConnector::internal_connect() {
  connector_
      ->with_keyspace(pool_->manager()->keyspace())
      ->with_settings(pool_->manager()->settings().connection_settings)
      ->with_metrics(pool_->manager()->metrics())
      ->connect(pool_->manager()->loop());
}

void PooledConnector::on_connect(Connector* connector) {
  if (!is_canceled_ && connector_->is_ok()) {
    connection_.reset(Memory::allocate<PooledConnection>(pool_,
                                                         connector->release_connection()));
  }
  callback_(this);
  // If the connection hasn't been released then close it.
  if (connection_) connection_->close();
  dec_ref();
}

void PooledConnector::on_delayed_connect(Timer* timer) {
  if (is_canceled_) {
    callback_(this);
    dec_ref();
  } else {
    internal_connect();
  }
}

} // namespace cass
