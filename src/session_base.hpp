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

#ifndef DATASTAX_INTERNAL_SESSION_BASE_HPP
#define DATASTAX_INTERNAL_SESSION_BASE_HPP

#include "cluster_connector.hpp"
#include "prepared.hpp"
#include "schema_agreement_handler.hpp"
#include "token_map.hpp"

namespace datastax { namespace internal {

class Random;

namespace core {

class ClusterConfig;

/**
 * A base class for implementing a session. It manages the state machine for
 * connecting and closing a session.
 */
class SessionBase : public ClusterListener {
public:
  enum State {
    SESSION_STATE_CONNECTING,
    SESSION_STATE_CONNECTED,
    SESSION_STATE_CLOSING,
    SESSION_STATE_CLOSED
  };

  SessionBase();

  virtual ~SessionBase();

  /**
   * Connect to a cluster using the provided cluster configuration and keyspace.
   * The future will be set after a successful connection or if an error
   * occurred.
   *
   * If the session is already connected, connecting or in the process of
   * closing then the future will be set with an error.
   *
   * @param cluster_config A cluster configuration object.
   * @param keyspace The keyspace to connect with. Use the empty string if no
   * keyspace should be used.
   * @return A future object for monitoring the connection progress.
   */
  Future::Ptr connect(const Config& config, const String& keyspace = "");

  /**
   * Close the session. There are not any failure conditions for closing a
   * session. If the session is already closing/closed the future is simply
   * set without error.
   * @return A future object for monitoring the closing progress.
   */
  Future::Ptr close();

public:
  CassUuid client_id() const { return client_id_; }
  CassUuid session_id() const { return session_id_; }
  String connect_keyspace() const { return connect_keyspace_; }
  const Config& config() const { return config_; }
  Cluster::Ptr cluster() const { return cluster_; }
  Random* random() const { return random_.get(); }
  Metrics* metrics() const { return metrics_.get(); }
  State state() const { return state_; }

protected:
  /**
   * Notify the future that session has been successfully connected.
   */
  void notify_connected();

  /**
   * Notify the future that session has encountered an error during the
   * connection process.
   *
   * @param code The error code of the connection failure.
   * @param message The error message of the connection failure.
   */
  void notify_connect_failed(CassError code, const String& message);

  /**
   * Notify the future that session has been closed.
   */
  void notify_closed();

protected:
  /**
   * A callback called after the control connection successfully connects.
   * By default this just notifies the connection future. Override to handle
   * something more complex, but `notify_connected()` must be called in the
   * overridden method.
   *
   * @param connected_host The host the control connection is connected to.
   * @param protocol_version The protocol version of the established control
   * connection.
   * @param hosts The current hosts in the cluster.
   * @param token_map The token map for the cluster.
   * @param local_dc The local datacenter for the cluster determined by the metadata service for
   * initializing the load balancing policies.
   */
  virtual void on_connect(const Host::Ptr& connected_host, ProtocolVersion protocol_version,
                          const HostMap& hosts, const TokenMap::Ptr& token_map,
                          const String& local_dc);

  /**
   * A callback called after the control connection fails to connect. By default
   * this just notifies the connection future of failure. Override to handle
   * something more complex, but `notify_connect_failed()` must be called in the
   * overridden method.
   *
   * @param code The error code for the connection failure.
   * @param message The error message for the connection failure.
   */
  virtual void on_connect_failed(CassError code, const String& message);

  /**
   * A callback called at the start of the close process. By default this closes
   * the cluster object. Override to handle something more complex, but
   * `notify_close()` must be caused in the overridden method to properly close
   * the cluster object and notify the close future.
   */
  virtual void on_close();

protected:
  /**
   * If overridden the override method must call `notify_closed()`.
   */
  virtual void on_close(Cluster* cluster);

private:
  void on_initialize(ClusterConnector* connector);

private:
  mutable uv_mutex_t mutex_;
  State state_;
  ScopedPtr<EventLoop> event_loop_;
  Cluster::Ptr cluster_;
  Config config_;
  ScopedPtr<Random> random_;
  ScopedPtr<Metrics> metrics_;
  String connect_keyspace_;
  CassError connect_error_code_;
  String connect_error_message_;
  Future::Ptr connect_future_;
  Future::Ptr close_future_;
  CassUuid client_id_;
  CassUuid session_id_;
};

} // namespace core
}} // namespace datastax::internal

#endif
