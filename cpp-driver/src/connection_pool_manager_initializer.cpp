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

#include "connection_pool_manager_initializer.hpp"

using namespace datastax;
using namespace datastax::internal::core;

ConnectionPoolManagerInitializer::ConnectionPoolManagerInitializer(ProtocolVersion protocol_version,
                                                                   const Callback& callback)
    : callback_(callback)
    , is_canceled_(false)
    , remaining_(0)
    , protocol_version_(protocol_version)
    , listener_(NULL)
    , metrics_(NULL) {}

void ConnectionPoolManagerInitializer::initialize(uv_loop_t* loop, const HostMap& hosts) {
  inc_ref();
  loop_ = loop;
  remaining_ = hosts.size();
  for (HostMap::const_iterator it = hosts.begin(), end = hosts.end(); it != end; ++it) {
    ConnectionPoolConnector::Ptr pool_connector(new ConnectionPoolConnector(
        it->second, protocol_version_,
        bind_callback(&ConnectionPoolManagerInitializer::on_connect, this)));
    pending_pools_.push_back(pool_connector);
    pool_connector->with_listener(this)
        ->with_keyspace(keyspace_)
        ->with_metrics(metrics_)
        ->with_settings(settings_)
        ->connect(loop);
  }
}

void ConnectionPoolManagerInitializer::cancel() {
  is_canceled_ = true;
  if (manager_) {
    manager_->close();
  } else {
    for (ConnectionPoolConnector::Vec::const_iterator it = pending_pools_.begin(),
                                                      end = pending_pools_.end();
         it != end; ++it) {
      (*it)->cancel();
    }
    for (ConnectionPool::Map::iterator it = pools_.begin(), end = pools_.end(); it != end; ++it) {
      it->second->close();
    }
  }
}

ConnectionPoolManagerInitializer*
ConnectionPoolManagerInitializer::with_keyspace(const String& keyspace) {
  keyspace_ = keyspace;
  return this;
}

ConnectionPoolManagerInitializer*
ConnectionPoolManagerInitializer::with_listener(ConnectionPoolManagerListener* listener) {
  listener_ = listener;
  return this;
}

ConnectionPoolManagerInitializer* ConnectionPoolManagerInitializer::with_metrics(Metrics* metrics) {
  metrics_ = metrics;
  return this;
}

ConnectionPoolManagerInitializer*
ConnectionPoolManagerInitializer::with_settings(const ConnectionPoolSettings& settings) {
  settings_ = settings;
  return this;
}

ConnectionPoolConnector::Vec ConnectionPoolManagerInitializer::failures() const {
  return failures_;
}

void ConnectionPoolManagerInitializer::on_pool_up(const Address& address) {
  if (listener_) {
    listener_->on_pool_up(address);
  }
}

void ConnectionPoolManagerInitializer::on_pool_down(const Address& address) {
  if (listener_) {
    listener_->on_pool_down(address);
  }
}

void ConnectionPoolManagerInitializer::on_pool_critical_error(const Address& address,
                                                              Connector::ConnectionError code,
                                                              const String& message) {
  if (listener_) {
    listener_->on_pool_critical_error(address, code, message);
  }
}

void ConnectionPoolManagerInitializer::on_close(ConnectionPool* pool) {
  // Ignore
}

void ConnectionPoolManagerInitializer::on_connect(ConnectionPoolConnector* pool_connector) {
  pending_pools_.erase(std::remove(pending_pools_.begin(), pending_pools_.end(), pool_connector),
                       pending_pools_.end());

  if (!is_canceled_) {
    if (pool_connector->is_ok()) {
      ConnectionPool::Ptr pool = pool_connector->release_pool();
      pools_[pool->address()] = pool;
    } else {
      failures_.push_back(ConnectionPoolConnector::Ptr(pool_connector));
    }
  }

  if (--remaining_ == 0) {
    if (!is_canceled_) {
      manager_.reset(new ConnectionPoolManager(pools_, loop_, protocol_version_, keyspace_,
                                               listener_, metrics_, settings_));
    }
    callback_(this);
    // If the manager hasn't been released then close it.
    if (manager_) {
      // If the callback doesn't take possession of the manager then we should
      // also clear the listener.
      manager_->set_listener();
      manager_->close();
    }
    dec_ref();
  }
}
