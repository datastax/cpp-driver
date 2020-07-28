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

#include "cluster_connector.hpp"
#include "dc_aware_policy.hpp"
#include "protocol.hpp"
#include "random.hpp"
#include "round_robin_policy.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

namespace datastax { namespace internal { namespace core {

/**
 * A task for running the connection process.
 */
class RunResolveAndConnectCluster : public Task {
public:
  RunResolveAndConnectCluster(const ClusterConnector::Ptr& connector)
      : connector_(connector) {}

  void run(EventLoop* event_loop) { connector_->internal_resolve_and_connect(); }

private:
  ClusterConnector::Ptr connector_;
};

/**
 * A task for canceling the connection process.
 */
class RunCancelCluster : public Task {
public:
  RunCancelCluster(const ClusterConnector::Ptr& connector)
      : connector_(connector) {}

  void run(EventLoop* event_loop) { connector_->internal_cancel(); }

private:
  ClusterConnector::Ptr connector_;
};

}}} // namespace datastax::internal::core

ClusterConnector::ClusterConnector(const AddressVec& contact_points,
                                   ProtocolVersion protocol_version, const Callback& callback)
    : remaining_connector_count_(0)
    , contact_points_(contact_points)
    , protocol_version_(protocol_version)
    , listener_(NULL)
    , event_loop_(NULL)
    , random_(NULL)
    , metrics_(NULL)
    , callback_(callback)
    , error_code_(CLUSTER_OK)
    , ssl_error_code_(CASS_OK) {}

ClusterConnector* ClusterConnector::with_listener(ClusterListener* listener) {
  listener_ = listener;
  return this;
}

ClusterConnector* ClusterConnector::with_random(Random* random) {
  random_ = random;
  return this;
}

ClusterConnector* ClusterConnector::with_metrics(Metrics* metrics) {
  metrics_ = metrics;
  return this;
}

void ClusterConnector::connect(EventLoop* event_loop) {
  event_loop_ = event_loop;
  event_loop_->add(new RunResolveAndConnectCluster(Ptr(this)));
}

void ClusterConnector::cancel() {
  if (event_loop_) {
    event_loop_->add(new RunCancelCluster(Ptr(this)));
  }
}

Cluster::Ptr ClusterConnector::release_cluster() {
  Cluster::Ptr temp(cluster_);
  cluster_.reset();
  return temp;
}

void ClusterConnector::internal_resolve_and_connect() {
  inc_ref();

  if (random_ && !contact_points_.empty()) {
    random_shuffle(contact_points_.begin(), contact_points_.end(), random_);
  }

  resolver_ = settings_.cluster_metadata_resolver_factory->new_instance(settings_);

  resolver_->resolve(event_loop_->loop(), contact_points_,
                     bind_callback(&ClusterConnector::on_resolve, this));
}

void ClusterConnector::internal_connect(const Address& address, ProtocolVersion version) {
  Host::Ptr host(new Host(address));
  ControlConnector::Ptr connector(
      new ControlConnector(host, version, bind_callback(&ClusterConnector::on_connect, this)));
  connectors_[address] = connector; // Keep track of the connectors so they can be canceled.
  connector->with_metrics(metrics_)
      ->with_settings(settings_.control_connection_settings)
      ->connect(event_loop_->loop());
}

void ClusterConnector::internal_cancel() {
  error_code_ = CLUSTER_CANCELED;
  if (resolver_) resolver_->cancel();
  for (ConnectorMap::iterator it = connectors_.begin(), end = connectors_.end(); it != end; ++it) {
    it->second->cancel();
  }
  if (cluster_) cluster_->close();
}

void ClusterConnector::finish() {
  callback_(this);
  if (cluster_) {
    // If the callback doesn't take possession of the cluster then we should
    // also clear the listener.
    cluster_->set_listener();
    cluster_->close();
  }
  // Explicitly release resources on the event loop thread.
  resolver_.reset();
  connectors_.clear();
  cluster_.reset();
  dec_ref();
}

void ClusterConnector::maybe_finish() {
  if (remaining_connector_count_ > 0 && --remaining_connector_count_ == 0) {
    finish();
  }
}

void ClusterConnector::on_error(ClusterConnector::ClusterError code, const String& message) {
  assert(code != CLUSTER_OK && "Notified error without an error");
  error_message_ = message;
  error_code_ = code;
  maybe_finish();
}

void ClusterConnector::on_resolve(ClusterMetadataResolver* resolver) {
  if (is_canceled()) {
    finish();
    return;
  }

  const AddressVec& resolved_contact_points(resolver->resolved_contact_points());

  if (resolved_contact_points.empty()) {
    error_code_ = CLUSTER_ERROR_NO_HOSTS_AVAILABLE;
    error_message_ = "Unable to connect to any contact points";
    finish();
    return;
  }

  local_dc_ = resolver->local_dc();
  remaining_connector_count_ = resolved_contact_points.size();
  for (AddressVec::const_iterator it = resolved_contact_points.begin(),
                                  end = resolved_contact_points.end();
       it != end; ++it) {
    internal_connect(*it, protocol_version_);
  }
}

void ClusterConnector::on_connect(ControlConnector* connector) {
  // Ignore logging protocol errors here because they're handled below
  if (!connector->is_ok() && !connector->is_canceled() && !connector->is_invalid_protocol()) {
    LOG_ERROR(
        "Unable to establish a control connection to host %s because of the following error: %s",
        connector->address().to_string().c_str(), connector->error_message().c_str());
  }

  // If the cluster object is successfully initialized or if the connector is
  // canceled then attempt to finish the connection process.
  if (cluster_ || is_canceled()) {
    maybe_finish();
    return;
  }

  // Otherwise, initialize the cluster and handle errors.
  if (connector->is_ok()) {
    const HostMap& hosts(connector->hosts());
    LoadBalancingPolicy::Vec policies;

    HostMap::const_iterator host_it = hosts.find(connector->address());
    if (host_it == hosts.end()) {
      // This error is unlikely to happen and it likely means that the local
      // metadata is corrupt or missing and the control connection process
      // would've probably failed before this happens.
      LOG_ERROR("Current control connection host %s not found in hosts metadata",
                connector->address().to_string().c_str());
      on_error(CLUSTER_ERROR_NO_HOSTS_AVAILABLE,
               "Control connection host is not found in hosts metadata");
      return;
    }
    Host::Ptr connected_host(host_it->second);

    // Build policies list including the default policy
    LoadBalancingPolicy::Ptr default_policy(settings_.load_balancing_policy->new_instance());
    policies.push_back(default_policy);
    for (LoadBalancingPolicy::Vec::const_iterator it = settings_.load_balancing_policies.begin(),
                                                  end = settings_.load_balancing_policies.end();
         it != end; ++it) {
      policies.push_back(LoadBalancingPolicy::Ptr((*it)->new_instance()));
    }

    // Initialize all created policies
    for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(), end = policies.end();
         it != end; ++it) {
      LoadBalancingPolicy::Ptr policy(*it);
      policy->init(connected_host, hosts, random_, local_dc_);
      policy->register_handles(event_loop_->loop());
    }

    ScopedPtr<QueryPlan> query_plan(default_policy->new_query_plan("", NULL, NULL));
    if (!query_plan->compute_next()) { // No hosts in the query plan
      LOG_ERROR("Current control connection host %s has no hosts available in "
                "it's query plan for the configured load balancing policy. If "
                "using DC-aware check to see if the local datacenter is valid.",
                connector->address().to_string().c_str());

      const char* message;
      if (dynamic_cast<DCAwarePolicy::DCAwareQueryPlan*>(query_plan.get()) !=
          NULL) { // Check if DC-aware
        message = "No hosts available for the control connection using the "
                  "DC-aware load balancing policy. "
                  "Check to see if the configured local datacenter is valid";
      } else {
        message = "No hosts available for the control connection using the "
                  "configured load balancing policy";
      }
      on_error(CLUSTER_ERROR_NO_HOSTS_AVAILABLE, message);
      return;
    }

    cluster_.reset(new Cluster(connector->release_connection(), listener_, event_loop_,
                               connected_host, hosts, connector->schema(), default_policy, policies,
                               local_dc_, connector->supported_options(), settings_));

    // Clear any connection errors and set the final negotiated protocol version.
    error_code_ = CLUSTER_OK;
    error_message_.clear();
    protocol_version_ = connector->protocol_version();

    // The cluster is initialized so the rest of the connectors can be canceled.
    for (ConnectorMap::iterator it = connectors_.begin(), end = connectors_.end(); it != end;
         ++it) {
      if (it->first != connector->address()) { // Not the current connector.
        it->second->cancel();
      }
    }

    maybe_finish();
  } else if (connector->is_invalid_protocol()) {
    ProtocolVersion lower_version(connector->protocol_version().previous());
    if (!lower_version.is_valid()) {
      LOG_ERROR(
          "Host %s does not support any valid protocol version (lowest supported version is %s)",
          connector->address().to_string().c_str(),
          ProtocolVersion::lowest_supported().to_string().c_str());
      on_error(CLUSTER_ERROR_INVALID_PROTOCOL, "Unable to find supported protocol version");
      return;
    }
    LOG_INFO("Host %s does not support protocol version %s. "
             "Trying protocol version %s...",
             connector->address().to_string().c_str(),
             connector->protocol_version().to_string().c_str(), lower_version.to_string().c_str());
    internal_connect(connector->address(), lower_version);
  } else if (connector->is_ssl_error()) {
    ssl_error_code_ = connector->ssl_error_code();
    on_error(CLUSTER_ERROR_SSL_ERROR, connector->error_message());
  } else if (connector->is_auth_error()) {
    on_error(CLUSTER_ERROR_AUTH_ERROR, connector->error_message());
  } else {
    assert(!connector->is_canceled() &&
           "The control connector should have an error and not be canceled");
    on_error(CLUSTER_ERROR_NO_HOSTS_AVAILABLE, connector->error_message());
  }
}
