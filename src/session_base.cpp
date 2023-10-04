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
#include "utils.hpp"
#include "uuids.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

class SessionFuture : public Future {
public:
  typedef SharedRefPtr<SessionFuture> Ptr;

  SessionFuture()
      : Future(FUTURE_TYPE_SESSION) {}
};

SessionBase::SessionBase()
    : state_(SESSION_STATE_CLOSED) {
  uv_mutex_init(&mutex_);

  UuidGen generator;
  generator.generate_random(&client_id_);
  generator.generate_random(&session_id_);
}

SessionBase::~SessionBase() {
  if (event_loop_) {
    event_loop_->close_handles();
    event_loop_->join();
  }
  uv_mutex_destroy(&mutex_);
}

Future::Ptr SessionBase::connect(const Config& config, const String& keyspace) {
  Future::Ptr future(new SessionFuture());

  ScopedMutex l(&mutex_);
  if (state_ != SESSION_STATE_CLOSED) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                      "Already connecting, closing, or connected");
    return future;
  }

  if (!event_loop_) {
    int rc = 0;
    event_loop_.reset(new EventLoop());

    rc = event_loop_->init("Session/Control Connection");
    if (rc != 0) {
      future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT, "Unable to initialize cluster event loop");
      return future;
    }

    rc = event_loop_->run();
    if (rc != 0) {
      future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT, "Unable to run cluster event loop");
      return future;
    }
  }

  if (config.is_client_id_set()) {
    client_id_ = config.client_id();
  }
  LOG_INFO("Client id is %s", to_string(client_id_).c_str());
  LOG_INFO("Session id is %s", to_string(session_id_).c_str());

  config_ = config.new_instance();
  connect_keyspace_ = keyspace;
  connect_future_ = future;
  state_ = SESSION_STATE_CONNECTING;

  if (config.use_randomized_contact_points()) {
    random_.reset(new Random());
  } else {
    random_.reset();
  }

  metrics_.reset(new Metrics(config.thread_count_io() + 1, config.cluster_histogram_refresh_interval()));

  cluster_.reset();
  ClusterConnector::Ptr connector(
      new ClusterConnector(config_.contact_points(), config_.protocol_version(),
                           bind_callback(&SessionBase::on_initialize, this)));

  ClusterSettings settings(config_);
  settings.control_connection_settings.connection_settings.client_id = to_string(client_id_);
  settings.disable_events_on_startup = true;

  connector->with_listener(this)
      ->with_settings(settings)
      ->with_random(random_.get())
      ->with_metrics(metrics_.get())
      ->connect(event_loop_.get());

  return future;
}

Future::Ptr SessionBase::close() {
  Future::Ptr future(new SessionFuture());

  ScopedMutex l(&mutex_);
  if (state_ == SESSION_STATE_CLOSED || state_ == SESSION_STATE_CLOSING) {
    future->set_error(CASS_ERROR_LIB_UNABLE_TO_CLOSE, "Already closing or closed");
    return future;
  }
  state_ = SESSION_STATE_CLOSING;
  close_future_ = future;
  on_close();
  return future;
}

void SessionBase::notify_connected() {
  ScopedMutex l(&mutex_);
  if (state_ == SESSION_STATE_CONNECTING) {
    state_ = SESSION_STATE_CONNECTED;
    connect_future_->set();
    connect_future_.reset();
    cluster_->start_events();
  }
}

void SessionBase::notify_connect_failed(CassError code, const String& message) {
  if (cluster_) {
    connect_error_code_ = code;
    connect_error_message_ = message;
    on_close();
  } else {
    ScopedMutex l(&mutex_);
    state_ = SESSION_STATE_CLOSED;
    connect_future_->set_error(code, message);
    connect_future_.reset();
  }
}

void SessionBase::notify_closed() {
  if (cluster_) cluster_->close();
}

void SessionBase::on_connect(const Host::Ptr& connected_host, ProtocolVersion protocol_version,
                             const HostMap& hosts, const TokenMap::Ptr& token_map,
                             const String& local_dc) {
  notify_connected();
}

void SessionBase::on_connect_failed(CassError code, const String& message) {
  notify_connect_failed(code, message);
}

void SessionBase::on_close() { notify_closed(); }

void SessionBase::on_close(Cluster* cluster) {
  ScopedMutex l(&mutex_);
  if (state_ == SESSION_STATE_CLOSING) {
    state_ = SESSION_STATE_CLOSED;
    close_future_->set();
    close_future_.reset();
  } else if (state_ == SESSION_STATE_CONNECTING) {
    state_ = SESSION_STATE_CLOSED;
    connect_future_->set_error(connect_error_code_, connect_error_message_);
    connect_future_.reset();
  }
}

void SessionBase::on_initialize(ClusterConnector* connector) {
  if (connector->is_ok()) {
    cluster_ = connector->release_cluster();

    // Handle default consistency level for DBaaS
    StringMultimap::const_iterator it = cluster_->supported_options().find("PRODUCT_TYPE");
    if (it != cluster_->supported_options().end() && it->second[0] == CASS_DBAAS_PRODUCT_TYPE) {
      config_.set_default_consistency(CASS_DEFAULT_DBAAS_CONSISTENCY);

      if (it->second.size() > 1) {
        LOG_DEBUG("PRODUCT_TYPE has more than one type: %s", implode(it->second).c_str());
      }
    } else {
      config_.set_default_consistency(CASS_DEFAULT_CONSISTENCY);
    }

    on_connect(cluster_->connected_host(), cluster_->protocol_version(),
               cluster_->available_hosts(), cluster_->token_map(), cluster_->local_dc());
  } else {
    assert(!connector->is_canceled() && "Cluster connection process canceled");
    switch (connector->error_code()) {
      case ClusterConnector::CLUSTER_ERROR_INVALID_PROTOCOL:
        on_connect_failed(CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL, connector->error_message());
        break;
      case ClusterConnector::CLUSTER_ERROR_SSL_ERROR:
        on_connect_failed(connector->ssl_error_code(), connector->error_message());
        break;
      case ClusterConnector::CLUSTER_ERROR_AUTH_ERROR:
        on_connect_failed(CASS_ERROR_SERVER_BAD_CREDENTIALS, connector->error_message());
        break;
      case ClusterConnector::CLUSTER_ERROR_NO_HOSTS_AVAILABLE:
        on_connect_failed(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connector->error_message());
        break;
      default:
        // This shouldn't happen, but let's handle it
        on_connect_failed(CASS_ERROR_LIB_UNABLE_TO_CONNECT, connector->error_message());
        break;
    }
  }
}
