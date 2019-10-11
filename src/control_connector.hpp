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

#ifndef DATASTAX_INTERNAL_CONTROL_CONNECTOR_HPP
#define DATASTAX_INTERNAL_CONTROL_CONNECTOR_HPP

#include "callback.hpp"
#include "control_connection.hpp"
#include "event_response.hpp"
#include "ref_counted.hpp"

namespace datastax { namespace internal { namespace core {

class HostsConnectorRequestCallback;
class Metrics;
class SchemaConnectorRequestCallback;

/**
 * The initial schema metadata retrieved from the cluster when the control
 * connection is established.
 */
struct ControlConnectionSchema {
  ResultResponse::Ptr keyspaces;
  ResultResponse::Ptr tables;
  ResultResponse::Ptr views;
  ResultResponse::Ptr columns;
  ResultResponse::Ptr indexes;
  ResultResponse::Ptr user_types;
  ResultResponse::Ptr functions;
  ResultResponse::Ptr aggregates;
  ResultResponse::Ptr virtual_keyspaces;
  ResultResponse::Ptr virtual_tables;
  ResultResponse::Ptr virtual_columns;
};

/**
 * A connector that establishes a control connection, negotiates the protocol
 * version, and registers for cluster events (topology and schema changes). It
 * also retrieves the initial host and schema metadata for the cluster.
 */
class ControlConnector
    : public RefCounted<ControlConnector>
    , public RecordingConnectionListener {
public:
  typedef SharedRefPtr<ControlConnector> Ptr;
  typedef Vector<Ptr> Vec;
  typedef internal::Callback<void, ControlConnector*> Callback;

  enum ControlConnectionError {
    CONTROL_CONNECTION_OK,
    CONTROL_CONNECTION_CANCELED,
    CONTROL_CONNECTION_ERROR_CLOSE,
    CONTROL_CONNECTION_ERROR_CONNECTION,
    CONTROL_CONNECTION_ERROR_HOSTS,
    CONTROL_CONNECTION_ERROR_SCHEMA
  };

  /**
   * Constructor
   *
   * @param host The host to connect to.
   * @param protocol_version The initial protocol version to use for the
   * connection.
   * @param callback
   */
  ControlConnector(const Host::Ptr& host, ProtocolVersion protocol_version,
                   const Callback& callback);

  /**
   * Sets the listener to use after the control connection has been
   * established.
   *
   * @param listener The control connection listener.
   * @return The connector to chain calls.
   */
  ControlConnector* with_listener(ControlConnectionListener* listener);

  /**
   * Set the metrics object to use to record metrics for the connection.
   *
   * @param metrics A metrics object.
   * @return The connector to chain calls.
   */
  ControlConnector* with_metrics(Metrics* metrics);

  /**
   * Sets the control connection settings as well as the underlying settings
   * for the connection and socket.
   *
   * @param settings The settings to be used for the connection process.
   * @return The connector to chain calls.
   */
  ControlConnector* with_settings(const ControlConnectionSettings& settings);

  /**
   * Start the connection process.
   *
   * @param loop An event loop to use for connecting the connection.
   */
  void connect(uv_loop_t* loop);

  /**
   * Cancel the connection process.
   */
  void cancel();

  /**
   * Release the connection from the connector. If not released in the callback
   * the connection will automatically be closed.
   *
   * @return The connection object for this connector. This returns a null object
   * if the connection is not connected or an error occurred.
   */
  ControlConnection::Ptr release_connection();

public:
  /**
   * Gets the server version of the connection.
   *
   * @return The server version number.
   */
  const VersionNumber server_version() const { return server_version_; }

  /**
   * The initial list of hosts available in the cluster.
   *
   * @return
   */
  const HostMap& hosts() const { return hosts_; }

  /**
   * The initial schema metadata.
   *
   * @return
   */
  const ControlConnectionSchema& schema() const { return schema_; }

public:
  const Address& address() const { return connector_->address(); }

  const ProtocolVersion protocol_version() const { return connector_->protocol_version(); }

  bool is_ok() const { return error_code_ == CONTROL_CONNECTION_OK; }
  bool is_canceled() const { return error_code_ == CONTROL_CONNECTION_CANCELED; }
  bool is_invalid_protocol() const {
    return error_code_ == CONTROL_CONNECTION_ERROR_CONNECTION && connector_->is_invalid_protocol();
  }
  bool is_ssl_error() const {
    return error_code_ == CONTROL_CONNECTION_ERROR_CONNECTION && connector_->is_ssl_error();
  }
  bool is_auth_error() const {
    return error_code_ == CONTROL_CONNECTION_ERROR_CONNECTION && connector_->is_auth_error();
  }

  const StringMultimap& supported_options() const { return connector_->supported_options(); }

  ControlConnectionError error_code() const { return error_code_; }
  const String& error_message() const { return error_message_; }
  CassError ssl_error_code() { return connector_->ssl_error_code(); }

  Connector::ConnectionError connection_error_code() { return connector_->error_code(); }
  const String& connection_error_message() { return connector_->error_message(); }

private:
  friend class HostsConnectorRequestCallback;
  friend class SchemaConnectorRequestCallback;

private:
  void finish();

  void on_success();
  void on_error(ControlConnectionError code, const String& message);

  void on_connect(Connector* connector);
  void handle_connect(Connector* connector);

  void query_hosts();
  void handle_query_hosts(HostsConnectorRequestCallback* callback);

  void query_schema();
  void handle_query_schema(SchemaConnectorRequestCallback* callback);

private:
  // Connection listener methods
  virtual void on_close(Connection* connection);

private:
  Connector::Ptr connector_;
  Connection::Ptr connection_;
  ControlConnection::Ptr control_connection_;
  VersionNumber server_version_;
  VersionNumber dse_server_version_;
  HostMap hosts_;

  /**
   * This is used to keep track of the "listen address" which is used to look
   * up a host in the "system.peers" table by its primary key "peer". No other
   * component cares about this information so it's not need in the `Host` type.
   */
  ListenAddressMap listen_addresses_;
  ControlConnectionSchema schema_;

  Callback callback_;

  ControlConnectionError error_code_;
  String error_message_;

  ControlConnectionListener* listener_;
  Metrics* metrics_;
  Host::Ptr host_;
  ControlConnectionSettings settings_;
};

}}} // namespace datastax::internal::core

#endif
