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
#include "request_queue.hpp"
#include "scoped_lock.hpp"

namespace cass {

/**
 * A task for starting the connection process from the event loop thread.
 */
class RunConnect : public Task {
public:
  RunConnect(const PooledConnector::Ptr& connector)
    : connector_(connector) { }

  void run(EventLoop* event_loop) {
    connector_->connect(PooledConnector::Protected());
  }

private:
  PooledConnector::Ptr connector_;
};

/**
 * A task for canceling the connection process from the event loop thread.
 */
class RunCancel : public Task {
public:
  RunCancel(const PooledConnector::Ptr& connector)
    : connector_(connector) { }

  void run(EventLoop* event_loop) {
    connector_->cancel(PooledConnector::Protected());
  }

private:
  PooledConnector::Ptr connector_;
};

PooledConnector::PooledConnector(ConnectionPool* pool,
                                 void* data, Callback callback)
  : pool_(pool)
  , connector_(Memory::allocate<Connector>(pool->address(), pool->manager()->protocol_version(), this, on_connect))
  , data_(data)
  , callback_(callback)
  , is_cancelled_(false)
  , event_loop_(NULL) {
  uv_mutex_init(&lock_);
}

PooledConnector::~PooledConnector() {
  uv_mutex_destroy(&lock_);
}

void PooledConnector::connect() {
  ScopedMutex l(&lock_);
  event_loop_ = pool_->manager()->event_loop();
  event_loop_->add(Memory::allocate<RunConnect>(Ptr(this)));
}

void PooledConnector::cancel() {
  ScopedMutex l(&lock_);
  if (event_loop_ != NULL) {
    event_loop_->add(Memory::allocate<RunCancel>(Ptr(this)));
  }
}

PooledConnection::Ptr PooledConnector::release_connection() {
  PooledConnection::Ptr temp = connection_;
  connection_.reset();
  return temp;
}

bool PooledConnector::is_cancelled() const {
  return is_cancelled_;
}

bool PooledConnector::is_ok() const {
  return !is_cancelled() && connector_->is_ok();
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

void PooledConnector::connect(Protected) {
  ScopedMutex l(&lock_);
  if (is_cancelled_) {
    callback_(this, event_loop_);
  } else {
    inc_ref();
    internal_connect();
  }
}

void PooledConnector::delayed_connect(EventLoop* event_loop, uint64_t wait_time_ms, Protected) {
  ScopedMutex l(&lock_);
  event_loop_ = event_loop;
  if (is_cancelled_) {
    callback_(this, event_loop_);
  } else {
    inc_ref();
    if (wait_time_ms > 0) {
      delayed_connect_timer_.start(event_loop_->loop(), wait_time_ms, this, on_delayed_connect);
    } else {
      internal_connect();
    }
  }
}

void PooledConnector::cancel(Protected) {
  ScopedMutex l(&lock_);
  if (delayed_connect_timer_.is_running()) {
    delayed_connect_timer_.stop();
    callback_(this, event_loop_);
    dec_ref();
  } else {
    connector_->cancel();
  }
}

void PooledConnector::internal_connect() {
  connector_
      ->with_keyspace(pool_->manager()->keyspace())
      ->with_settings(pool_->manager()->settings().connection_settings)
      ->with_metrics(pool_->manager()->metrics())
      ->connect(event_loop_->loop());
}

void PooledConnector::on_connect(Connector* connector) {
  PooledConnector* pooled_connector = static_cast<PooledConnector*>(connector->data());
  pooled_connector->handle_connect(connector);
}

void PooledConnector::handle_connect(Connector* connector) {
  if (!is_cancelled_ && connector_->is_ok()) {
    connection_.reset(Memory::allocate<PooledConnection>(pool_,
                                                         event_loop_,
                                                         connector->release_connection()));
  }
  callback_(this, event_loop_);
  // If the connection hasn't been released then close it.
  if (connection_) connection_->close();
  dec_ref();
}

void PooledConnector::on_delayed_connect(Timer* timer) {
  PooledConnector* pooled_connector = static_cast<PooledConnector*>(timer->data());
  pooled_connector->handle_delayed_connect(timer);
}

void PooledConnector::handle_delayed_connect(Timer* timer) {
  if (is_cancelled_) {
    callback_(this, event_loop_);
    dec_ref();
  } else {
    internal_connect();
  }
}

} // namespace cass
