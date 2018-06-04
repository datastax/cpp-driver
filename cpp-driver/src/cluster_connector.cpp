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
#include "random.hpp"
#include "round_robin_policy.hpp"

namespace cass {

/**
 * A task for running the connection process.
 */
class RunResolveAndConnectCluster : public Task {
public:
  RunResolveAndConnectCluster(const ClusterConnector::Ptr& connector)
    : connector_(connector) { }

  void run(EventLoop* event_loop) {
    connector_->internal_resolve_and_connect();
  }

private:
  ClusterConnector::Ptr connector_;
};

/**
 * A task for canceling the connection process.
 */
class RunCancelCluster : public Task {
public:
  RunCancelCluster(const ClusterConnector::Ptr& connector)
    : connector_(connector) { }

  void run(EventLoop* event_loop) {
    connector_->internal_cancel();
  }

private:
  ClusterConnector::Ptr connector_;
};

ClusterConnector::ClusterConnector(const ContactPointList& contact_points,
                                   int protocol_version,
                                   const Callback& callback)
  : contact_points_(contact_points)
  , protocol_version_(protocol_version)
  , listener_(NULL)
  , event_loop_(NULL)
  , random_(NULL)
  , callback_(callback)
  , error_code_(CLUSTER_OK) { }

void ClusterConnector::connect(EventLoop* event_loop) {
  event_loop_ = event_loop;
  event_loop_->add(Memory::allocate<RunResolveAndConnectCluster>(Ptr(this)));

}

void ClusterConnector::cancel() {
  if (event_loop_) {
    event_loop_->add(Memory::allocate<RunCancelCluster>(Ptr(this)));
  }
}

Cluster::Ptr ClusterConnector::release_cluster() {
  Cluster::Ptr temp(cluster_);
  cluster_.reset();
  return temp;
}

void ClusterConnector::internal_resolve_and_connect() {
  inc_ref();

  if (random_) {
    random_shuffle(contact_points_.begin(), contact_points_.end(), random_);
  }

  for (ContactPointList::const_iterator it = contact_points_.begin(),
       end = contact_points_.end(); it != end; ++it) {
    const String& contact_point = *it;
    Address address;
    // Attempt to parse the contact point string. If it's an IP address
    // then immediately add it to our resolved contact points, otherwise
    // attempt to resolve the string as a hostname.
    if (Address::from_string(contact_point, settings_.port, &address)) {
      contact_points_resolved_.push_back(address);
    } else {
      if (!resolver_) {
        resolver_.reset(
              Memory::allocate<MultiResolver>(
                bind_member_func(&ClusterConnector::on_resolve, this)));
      }
      resolver_->resolve(event_loop_->loop(),
                         contact_point,
                         settings_.port,
                         settings_.control_connection_settings.connection_settings.socket_settings.resolve_timeout_ms);
    }
  }

  if (!resolver_) {
    contact_points_resolved_it_ = contact_points_resolved_.begin();
    internal_connect();
  }
}

void ClusterConnector::internal_connect() {
  if (contact_points_resolved_it_ == contact_points_resolved_.end()) {
    on_error(CLUSTER_ERROR_NO_HOSTS_AVILABLE, "Unable to connect to any contact points");
    return;
  }
  connector_.reset(Memory::allocate<ControlConnector>(*contact_points_resolved_it_,
                                                      protocol_version_,
                                                      bind_member_func(&ClusterConnector::on_connect, this)));
  connector_
      ->with_settings(settings_.control_connection_settings)
      ->connect(event_loop_->loop());
}

void ClusterConnector::internal_cancel() {
  error_code_ = CLUSTER_CANCELED;
  if (resolver_) resolver_->cancel();
  if (connector_) connector_->cancel();
}

void ClusterConnector::finish() {
  callback_(this);
  if (cluster_) cluster_->close();
  dec_ref();
}

void ClusterConnector::on_error(ClusterConnector::ClusterError code,
                                const String& message) {
  assert(code != CLUSTER_OK && "Notified error without an error");
  if (error_code_ == CLUSTER_OK) { // Only perform this once
    error_message_ = message;
    error_code_ = code;
    finish();
  }
}

void ClusterConnector::on_resolve(MultiResolver* resolver) {
  if (is_canceled())  {
    finish();
    return;
  }

  const Resolver::Vec& resolvers = resolver->resolvers();
  for (Resolver::Vec::const_iterator it = resolvers.begin(),
       end = resolvers.end(); it != end; ++it) {
    const Resolver::Ptr resolver(*it);
    if (resolver->is_success()) {
      const AddressVec& addresses = resolver->addresses();
      if (!addresses.empty()) {
        for (AddressVec::const_iterator it = addresses.begin(),
             end = addresses.end(); it != end;  ++it) {
          contact_points_resolved_.push_back(*it);
        }
      } else {
        LOG_ERROR("No addresses resolved for %s:%d\n",
                  resolver->hostname().c_str(), resolver->port());
      }
    } else if (resolver->is_timed_out()) {
      LOG_ERROR("Timed out attempting to resolve address for %s:%d\n",
                resolver->hostname().c_str(), resolver->port());
    } else if (!resolver->is_canceled()) {
      LOG_ERROR("Unable to resolve address for %s:%d\n",
                resolver->hostname().c_str(), resolver->port());
    }
  }

  contact_points_resolved_it_ = contact_points_resolved_.begin();
  internal_connect();
}

void ClusterConnector::on_connect(ControlConnector* connector) {
  if (is_canceled()) {
    finish();
    return;
  }

  if (connector->is_ok()) {
    cluster_.reset(Memory::allocate<Cluster>(connector,
                                             listener_,
                                             event_loop_,
                                             random_,
                                             settings_));
    finish();
  } else if (connector->is_invalid_protocol()) {
    if (protocol_version_ <= 1) {
      LOG_ERROR("Host %s does not support any valid protocol version",
                contact_points_resolved_it_->to_string().c_str());
      on_error(CLUSTER_ERROR_INVALID_PROTOCOL, "Not even protocol version 1 is supported");
      return;
    }

    int previous_version = protocol_version_;
    bool is_dse_version = protocol_version_ & DSE_PROTOCOL_VERSION_BIT;
    if (is_dse_version) {
      int dse_version = protocol_version_ & DSE_PROTOCOL_VERSION_MASK;
      if (dse_version <= 1) {
        // Start trying Cassandra protocol versions
        protocol_version_ = CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION;
      } else {
        protocol_version_--;
      }
    } else {
      protocol_version_--;
    }

    LOG_WARN("Host %s does not support protocol version %s. "
             "Trying protocol version %s...",
             contact_points_resolved_it_->to_string().c_str(),
             protocol_version_to_string(previous_version).c_str(),
             protocol_version_to_string(protocol_version_).c_str());

    internal_connect();
  } else if(connector->is_ssl_error()) {
    on_error(CLUSTER_ERROR_SSL_ERROR, connector->error_message());
  } else if(connector->is_auth_error()) {
    on_error(CLUSTER_ERROR_AUTH_ERROR, connector->error_message());
  } else {
    LOG_ERROR("Unable to establish a control connection to host %s because of the following error: %s",
              connector->address().to_string().c_str(),
              connector->error_message().c_str());

    // Possibly a recoverable error or timeout try the next host
    ++contact_points_resolved_it_; // Move to the next contact point
    internal_connect();
  }
}

} // namespace cass
