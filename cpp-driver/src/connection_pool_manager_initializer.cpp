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

#include "memory.hpp"

namespace cass {

ConnectionPoolManagerInitializer::ConnectionPoolManagerInitializer(int protocol_version,
                                                                   const Callback& callback)
  : callback_(callback)
  , is_canceled_(false)
  , remaining_(0)
  , protocol_version_(protocol_version)
  , listener_(NULL)
  , metrics_(NULL) { }

void ConnectionPoolManagerInitializer::initialize(uv_loop_t* loop,
                                                  const AddressVec& addresses) {
  inc_ref();
  remaining_ = addresses.size();
  manager_.reset(Memory::allocate<ConnectionPoolManager>(loop,
                                                         protocol_version_,
                                                         keyspace_,
                                                         listener_,
                                                         metrics_,
                                                         settings_));
  for (AddressVec::const_iterator it = addresses.begin(),
       end = addresses.end(); it != end; ++it) {
    ConnectionPoolConnector::Ptr pool_connector(
          Memory::allocate<ConnectionPoolConnector>(manager_.get(), *it,
                                                    bind_callback(&ConnectionPoolManagerInitializer::on_connect, this)));
    connectors_.push_back(pool_connector);
    pool_connector->connect();
  }
}

void ConnectionPoolManagerInitializer::cancel() {
  is_canceled_ = true;
  for (ConnectionPoolConnector::Vec::const_iterator it = connectors_.begin(),
       end = connectors_.end(); it != end; ++it) {
    (*it)->cancel();
  }
}

ConnectionPoolManagerInitializer* ConnectionPoolManagerInitializer::with_keyspace(const String& keyspace) {
  keyspace_ = keyspace;
  return this;
}

ConnectionPoolManagerInitializer* ConnectionPoolManagerInitializer::with_listener(ConnectionPoolManagerListener* listener) {
  listener_ = listener;
  return this;
}

ConnectionPoolManagerInitializer* ConnectionPoolManagerInitializer::with_metrics(Metrics* metrics) {
  metrics_ = metrics;
  return this;
}

ConnectionPoolManagerInitializer* ConnectionPoolManagerInitializer::with_settings(const ConnectionPoolManagerSettings& settings) {
  settings_ = settings;
  return this;
}

ConnectionPoolConnector::Vec ConnectionPoolManagerInitializer::failures() const {
  return failures_;
}

bool ConnectionPoolManagerInitializer::is_canceled() {
  return is_canceled_;
}

void ConnectionPoolManagerInitializer::on_connect(ConnectionPoolConnector* pool_connector) {
  if (!is_canceled_) {
    if (pool_connector->is_ok()) {
      manager_->add_pool(pool_connector->release_pool(), ConnectionPoolManager::Protected());
    } else {
      failures_.push_back(ConnectionPoolConnector::Ptr(pool_connector));
    }
  }

  if (--remaining_ == 0) {
    if (manager_) manager_->set_listener(NULL);
    callback_(this);
    // If the manager hasn't been released then close it.
    if (manager_) manager_->close();
    dec_ref();
  }
}

} // namespace cass
