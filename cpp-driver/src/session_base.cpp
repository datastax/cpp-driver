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

#include "session_base.hpp"

#include "cluster_config.hpp"
#include "metrics.hpp"
#include "prepare_all_handler.hpp"
#include "random.hpp"
#include "request_handler.hpp"

namespace cass {

SessionBase::SessionBase()
  : state_(SESSION_STATE_CLOSED) {
  uv_mutex_init(&mutex_);
}

SessionBase::~SessionBase() {
  uv_mutex_destroy(&mutex_);
}

void SessionBase::connect(const Config& config,
                          const String& keyspace,
                          const Future::Ptr& future) {
  ScopedMutex l(&mutex_);
  if (state_ != SESSION_STATE_CLOSED) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                      "Already connecting, closing, or connected");
    return;
  }

  if (!event_loop_) {
    int rc = 0;
    event_loop_.reset(Memory::allocate<EventLoop>());

    rc = event_loop_->init("Session/Control Connection");
    if (rc != 0) {
      future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT,
                        "Unable to initialize cluster event loop");
      return;
    }

    rc = event_loop_->run();
    if (rc != 0) {
      future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT,
                        "Unable to run cluster event loop");
      return;
    }
  }

  config_ = config.new_instance();
  connect_keyspace_ = keyspace;
  connect_future_ = future;
  state_ = SESSION_STATE_CONNECTING;

  if (config.use_randomized_contact_points()) {
    random_.reset(Memory::allocate<Random>());
  } else {
    random_.reset();
  }

  metrics_.reset(Memory::allocate<Metrics>(config.thread_count_io() + 1));

  cluster_connector_.reset(
        Memory::allocate<ClusterConnector>(config_.contact_points(),
                                           config_.protocol_version(),
                                           bind_callback(&SessionBase::on_initialize, this)));

  cluster_connector_
      ->with_listener(this)
      ->with_settings(ClusterSettings(config_))
      ->with_random(random_.get())
      ->with_metrics(metrics_.get())
      ->connect(event_loop_.get());
}

void SessionBase::close(const Future::Ptr& future) {
  ScopedMutex l(&mutex_);
  if (state_ == SESSION_STATE_CLOSED ||
      state_ == SESSION_STATE_CLOSING) {
    future->set();
    return;
  }
  state_ = SESSION_STATE_CLOSING;
  close_future_ = future;
  cluster_->close();
}

void SessionBase::join() {
  if (event_loop_) {
    event_loop_->close_handles();
    event_loop_->join();
  }
}

void SessionBase::notify_connected() {
  ScopedMutex l(&mutex_);
  if (state_ == SESSION_STATE_CONNECTING) {
    state_ = SESSION_STATE_CONNECTED;
    connect_future_->set();
    connect_future_.reset();
  }
}

void SessionBase::notify_connect_failed(CassError code, const String& message) {
  ScopedMutex l(&mutex_);
  if (state_ == SESSION_STATE_CONNECTING) {
    state_ = SESSION_STATE_CLOSED;
    connect_future_->set_error(code, message);
    connect_future_.reset();
  }
}

void SessionBase::notify_closed() {
  ScopedMutex l(&mutex_);
  if (state_ == SESSION_STATE_CLOSING) {
    state_ = SESSION_STATE_CLOSED;
    close_future_->set();
    close_future_.reset();
    l.unlock();
  }
}

void SessionBase::on_connect(const Host::Ptr& connected_host,
                             int protocol_version,
                             const HostMap& hosts,
                             const TokenMap::Ptr& token_map) {
  notify_connected();
}

void SessionBase::on_connect_failed(CassError code, const String& message) {
  notify_connect_failed(code, message);
}

void SessionBase::on_close(Cluster* cluster) {
  notify_closed();
}

void SessionBase::on_initialize(ClusterConnector* connector) {
  if (connector->is_ok()) {
    cluster_ = connector->release_cluster();
    on_connect(cluster_->connected_host(),
               cluster_->protocol_version(),
               cluster_->hosts(),
               cluster_->token_map());
  } else {
    assert(!connector->is_canceled() && "Cluster connection process canceled");
    switch (connector->error_code()) {
      case ClusterConnector::CLUSTER_ERROR_INVALID_PROTOCOL:
        on_connect_failed(CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL,
                          connector->error_message());
        break;
      case ClusterConnector::CLUSTER_ERROR_SSL_ERROR:
        on_connect_failed(connector->ssl_error_code(),
                          connector->error_message());
        break;
      case ClusterConnector::CLUSTER_ERROR_AUTH_ERROR:
        on_connect_failed(CASS_ERROR_SERVER_BAD_CREDENTIALS,
                          connector->error_message());
        break;
      case ClusterConnector::CLUSTER_ERROR_NO_HOSTS_AVILABLE:
        on_connect_failed(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                          connector->error_message());
        break;
      default:
        // This shouldn't happen, but let's handle it
        on_connect_failed(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                          connector->error_message());
        break;
    }
  }
}

} // namespace cass
