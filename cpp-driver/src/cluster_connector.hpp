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

#ifndef __CASS_CLUSTER_CONNECTOR_HPP_INCLUDED__
#define __CASS_CLUSTER_CONNECTOR_HPP_INCLUDED__

#include "cluster.hpp"

#include "resolver.hpp"

namespace cass {

class Random;

/**
 * A connector that handles connecting to a cluster. It handles DNS and error
 * handling for multiple contact points. It attempts to connect to multiple
 * contact points and returns after the first successful connection or it fails
 * if no connections can be made. It also handles negotiating the highest
 * supported server-side protocol version.
 */
class ClusterConnector : public RefCounted<ClusterConnector> {
public:
  typedef SharedRefPtr<ClusterConnector> Ptr;
  typedef void (*Callback)(ClusterConnector*);

  enum ClusterError {
    CLUSTER_OK,
    CLUSTER_CANCELLED,
    CLUSTER_ERROR_INVALID_PROTOCOL,
    CLUSTER_ERROR_SSL_ERROR,
    CLUSTER_ERROR_AUTH_ERROR,
    CLUSTER_ERROR_NO_HOSTS_AVILABLE
  };

  /**
   * Constructor.
   *
   * @param contact_points A list of hosts (IP addresses or hostname strings)
   * that seed connection to the cluster.
   * @param protocol_version The initial protocol version to try.
   * @param data User data that is available from the callback.
   * @param callback A callback that is called when a connection to a contact
   * point is established, if an error occurred, or all contact points failed.
   */
  ClusterConnector(const ContactPointList& contact_points,
                   int protocol_version,
                   void* data,
                   Callback callback);

  /**
   * Set the cluster listener to use for handle cluster events.
   *
   * @param listener A listener that handles cluster events.
   * @return The connector to chain calls.
   */
  ClusterConnector* with_listener(ClusterListener*  listener) {
    listener_ = listener;
    return this;
  }

  /**
   * Set the random object to use for shuffling the contact points and load
   * balancing policies. The random object must live for the duration of the
   * connection process.
   *
   * @param random A random object.
   * @return The connector to chain calls.
   */
  ClusterConnector* with_random(Random* random) {
    random_ = random;
    return this;
  }

  /**
   * Set the cluster and underlying control connection settings.
   *
   * @param The settings to use for connecting the cluster.
   * @return The connector to chain calls.
   */
  ClusterConnector* with_settings(const ClusterSettings& settings) {
    settings_ = settings;
    return this;
  }

  /**
   * Connect to the cluster.
   *
   * @param event_loop The event loop to use for connecting to the cluster.
   */
  void connect(EventLoop* event_loop);

  /**
   * Cancel the connection process.
   */
  void cancel();

  /**
   * Release the cluster from the connector. If not released in the callback
   * the cluster will automatically be closed.
   *
   * @return The cluster object for this connector. This returns a null object
   * if the cluster is not connected or an error occurred.
   */
  Cluster::Ptr release_cluster();

public:
  void* data() { return data_; }

  int protocol_version() const { return protocol_version_; }

  bool is_ok() const { return error_code_ == CLUSTER_OK; }
  bool is_cancelled() const { return error_code_ == CLUSTER_CANCELLED; }

  ClusterError error_code() const { return error_code_; }
  const String& error_message() const { return error_message_; }
  CassError ssl_error_code() { return connector_->ssl_error_code(); }

private:
  friend class RunResolveAndConnectCluster;
  friend class RunCancelCluster;

private:
  void internal_resolve_and_connect();
  void internal_connect();
  void internal_cancel();

  void finish();

  void on_error(ClusterError code, const String& message);

  static void on_resolve(MultiResolver* resolver);
  void handle_resolve(MultiResolver* resolver);

  static void on_connect(ControlConnector* connector);
  void handle_connect(ControlConnector* connector);

private:
  Cluster::Ptr cluster_;
  MultiResolver::Ptr resolver_;
  ControlConnector::Ptr connector_;
  ContactPointList contact_points_;
  AddressVec contact_points_resolved_;
  AddressVec::const_iterator contact_points_resolved_it_;
  int protocol_version_;
  ClusterListener* listener_;
  EventLoop* event_loop_;
  Random* random_;
  ClusterSettings settings_;

  void* data_;
  Callback callback_;

  ClusterError error_code_;
  String error_message_;
};

} // namespace cass

#endif
