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

#include "control_connector.hpp"
#include "result_iterator.hpp"

namespace cass {

/**
 * A chained request callback that gets the cluster's hosts from the
 * "system.local" and "system.peers" tables.
 */
class HostsConnectorRequestCallback : public ChainedRequestCallback {
public:
  typedef SharedRefPtr<HostsConnectorRequestCallback> Ptr;

  HostsConnectorRequestCallback(const String& key, const String& query,
                                ControlConnector* connector)
    : ChainedRequestCallback(key, query)
    , connector_(connector) { }

  virtual void on_chain_set() {
    connector_->handle_query_hosts(this);
  }

  virtual void on_chain_error(CassError code, const String& message) {
    connector_->on_error(ControlConnector::CONTROL_CONNECTION_ERROR_HOSTS,
                         "Error running host queries on control connection " + message);
  }

  virtual void on_chain_timeout() {
    connector_->on_error(ControlConnector::CONTROL_CONNECTION_ERROR_HOSTS,
                         "Timed out running host queries on control connection");
  }

private:
  ControlConnector* connector_;
};

/**
 * A chained request callback that gets the cluster's schema metadata.
 */
class SchemaConnectorRequestCallback : public ChainedRequestCallback {
public:
  typedef SharedRefPtr<SchemaConnectorRequestCallback> Ptr;

  SchemaConnectorRequestCallback(const String& key, const String& query,
                                 ControlConnector* connector)
    : ChainedRequestCallback(key, query)
    , connector_(connector) { }

  virtual void on_chain_set() {
    connector_->handle_query_schema(this);
  }

  virtual void on_chain_error(CassError code, const String& message) {
    connector_->on_error(ControlConnector::CONTROL_CONNECTION_ERROR_SCHEMA,
                         "Error running schema queries on control connection " + message);
  }

  virtual void on_chain_timeout() {
    connector_->on_error(ControlConnector::CONTROL_CONNECTION_ERROR_SCHEMA,
                         "Timed out running schema queries on control connection");
  }

private:
  ControlConnector* connector_;
};

ControlConnectionSettings::ControlConnectionSettings()
  : use_schema(CASS_DEFAULT_USE_SCHEMA)
  , token_aware_routing(CASS_DEFAULT_TOKEN_AWARE_ROUTING)
  , refresh_node_info_on_up(CASS_DEFAULT_REFRESH_NODE_INFO_ON_UP) { }

ControlConnectionSettings::ControlConnectionSettings(const Config& config)
  : connection_settings(config)
  , use_schema(config.use_schema())
  , token_aware_routing(config.token_aware_routing())
  , refresh_node_info_on_up(false) { }

ControlConnector::ControlConnector(const Address& address,
                                   int protocol_version,
                                   const Callback& callback)
  : connector_(Memory::allocate<Connector>(address,
                                           protocol_version,
                                           bind_callback(&ControlConnector::on_connect, this)))
  , callback_(callback)
  , error_code_(CONTROL_CONNECTION_OK)
  , listener_(NULL)
  , metrics_(NULL) { }

void ControlConnector::connect(uv_loop_t* loop) {
  inc_ref();
  int event_types = 0;
  if (settings_.use_schema || settings_.token_aware_routing) {
    event_types = CASS_EVENT_TOPOLOGY_CHANGE | CASS_EVENT_STATUS_CHANGE |
                  CASS_EVENT_SCHEMA_CHANGE;
  } else {
    event_types = CASS_EVENT_TOPOLOGY_CHANGE | CASS_EVENT_STATUS_CHANGE;
  }
  connector_
      ->with_metrics(metrics_)
      ->with_settings(settings_.connection_settings)
      ->with_event_types(event_types)
      ->connect(loop);
}

void ControlConnector::cancel() {
  error_code_ = CONTROL_CONNECTION_CANCELED;
  connector_->cancel();
  if (connection_) connection_->close();
  if (control_connection_) control_connection_->close();
}

ControlConnection::Ptr ControlConnector::release_connection() {
  ControlConnection::Ptr temp(control_connection_);
  control_connection_.reset();
  return temp;
}

void ControlConnector::finish() {
  if (connection_) connection_->set_listener(NULL);
  callback_(this);
  // If the connections haven't been released then close them.
  if (connection_) connection_->close();
  if (control_connection_) control_connection_->close();
  dec_ref();
}

void ControlConnector::on_success() {
  if (is_canceled()) {
    finish();
    return;
  }

  // Transfer ownership of the connection to the control connection.
  control_connection_.reset(
        Memory::allocate<ControlConnection>(connection_,
                                            settings_.use_schema,
                                            settings_.token_aware_routing,
                                            settings_.refresh_node_info_on_up,
                                            server_version_,
                                            listen_addresses_));

  control_connection_->set_listener(listener_);
  connection_->set_listener(control_connection_.get());

  // Process any events that happened during control connection setup. It's
  // important to capture any changes that happened during retrieving the
  // host and schema metadata that might not be reflected in that data.
  ControlConnector::process_events(events(), control_connection_.get());

  connection_.reset(); // The control connection now owns it.

  finish();
}

void ControlConnector::on_error(ControlConnector::ControlConnectionError code,
                                const String& message) {
  assert(code != CONTROL_CONNECTION_OK && "Notified error without an error");
  if (error_code_ == CONTROL_CONNECTION_OK) { // Only perform this once
    error_message_ = message;
    error_code_ = code;
    if (connection_) connection_->defunct();
    finish();
  }
}

void ControlConnector::on_connect(Connector* connector) {
  if (!is_canceled() && connector->is_ok()) {
    connection_ = connector->release_connection();
    connection_->set_listener(this);

    // Propagate any events that happened during the connection process.
    Connector::process_events(connector->events(), this);

    query_hosts();
  } else if (is_canceled() || connector->is_canceled()) {
    finish();
  } else if (connector->error_code() == Connector::CONNECTION_ERROR_CLOSE) {
    on_error(CONTROL_CONNECTION_ERROR_CLOSE, connector->error_message());
  } else {
    on_error(CONTROL_CONNECTION_ERROR_CONNECTION,
             "Underlying connection error: " + connector->error_message());
  }
}

void ControlConnector::query_hosts() {
  // This needs to happen before other schema metadata queries so that we have
  // a valid server version because this version determines which follow up
  // schema metadata queries are executed.
  ChainedRequestCallback::Ptr callback(
        Memory::allocate<HostsConnectorRequestCallback>(
          "local", SELECT_LOCAL, this)
        ->chain("peers", SELECT_PEERS));

  if (connection_->write_and_flush(callback) < 0) {
    on_error(CONTROL_CONNECTION_ERROR_HOSTS,
             "Unable able to write hosts query to connection");
  }
}

void ControlConnector::handle_query_hosts(HostsConnectorRequestCallback* callback) {
  ResultResponse::Ptr local_result(callback->result("local"));
  if (local_result && local_result->row_count() > 0) {
    Host::Ptr host(Memory::allocate<Host>(connection_->address()));
    host->set(&local_result->first_row(), settings_.token_aware_routing);
    host->set_up();
    hosts_[host->address()] = host;
    server_version_ = host->server_version();
  } else {
    on_error(CONTROL_CONNECTION_ERROR_HOSTS,
             "No row found in " + connection_->address_string() + "'s local system table");
    return;
  }

  ResultResponse::Ptr peers_result(callback->result("peers"));
  if (peers_result) {
    ResultIterator rows(peers_result.get());
    while (rows.next()) {
      Address address;
      const Row* row = rows.row();
      if (!determine_address_for_peer_host(connection_->address(),
                                           row->get_by_name("peer"),
                                           row->get_by_name("rpc_address"),
                                           &address)) {
        continue;
      }

      Host::Ptr host(Memory::allocate<Host>(address));
      host->set(rows.row(), settings_.token_aware_routing);
      host->set_up();
      listen_addresses_[host->address()] = determine_listen_address(address, row);
      hosts_[host->address()] = host;
    }
  }

  if (settings_.token_aware_routing || settings_.use_schema) {
    query_schema();
  } else {
    // If we're not using token aware routing or schema we can just finish.
    on_success();
  }
}

void ControlConnector::query_schema() {
  ChainedRequestCallback::Ptr callback;

  if (server_version_ >= VersionNumber(3, 0, 0)) {
    callback = ChainedRequestCallback::Ptr(Memory::allocate<SchemaConnectorRequestCallback>(
                                             "keyspaces", SELECT_KEYSPACES_30, this));
    if (settings_.use_schema) {
      callback = callback
                 ->chain("tables", SELECT_TABLES_30)
                 ->chain("views", SELECT_VIEWS_30)
                 ->chain("columns", SELECT_COLUMNS_30)
                 ->chain("indexes", SELECT_INDEXES_30)
                 ->chain("user_types", SELECT_USERTYPES_30)
                 ->chain("functions", SELECT_FUNCTIONS_30)
                 ->chain("aggregates", SELECT_AGGREGATES_30);

      if (server_version_ >= VersionNumber(4, 0, 0)) {
        callback = callback
                   ->chain("virtual_keyspaces", SELECT_VIRTUAL_KEYSPACES_40)
                   ->chain("virtual_tables", SELECT_VIRTUAL_TABLES_40)
                   ->chain("virtual_columns", SELECT_VIRTUAL_COLUMNS_40);
      }
    }
  } else {
    callback = ChainedRequestCallback::Ptr(Memory::allocate<SchemaConnectorRequestCallback>(
                                             "keyspaces", SELECT_KEYSPACES_20, this));
    if (settings_.use_schema) {
      callback = callback
                 ->chain("tables", SELECT_COLUMN_FAMILIES_20)
                 ->chain("columns", SELECT_COLUMNS_20);


      if (server_version_ >= VersionNumber(2, 1, 0)) {
        callback = callback->chain("user_types", SELECT_USERTYPES_21);
      }
      if (server_version_ >= VersionNumber(2, 2, 0)) {
        callback = callback
                   ->chain("functions", SELECT_FUNCTIONS_22)
                   ->chain("aggregates", SELECT_AGGREGATES_22);
      }
    }
  }

  if (connection_->write_and_flush(callback) < 0) {
    on_error(CONTROL_CONNECTION_ERROR_SCHEMA,
             "Unable able to write schema query to connection");
  }
}

void ControlConnector::handle_query_schema(SchemaConnectorRequestCallback* callback) {
  schema_.keyspaces = callback->result("keyspaces");
  schema_.tables = callback->result("tables");
  schema_.views = callback->result("views");
  schema_.columns = callback->result("columns");
  schema_.indexes = callback->result("indexes");
  schema_.user_types = callback->result("types");
  schema_.functions = callback->result("functions");
  schema_.aggregates = callback->result("aggregates");
  schema_.virtual_keyspaces = callback->result("virtual_keyspaces");
  schema_.virtual_tables = callback->result("virtual_tables");
  schema_.virtual_columns = callback->result("virtual_columns");

  on_success();
}

void ControlConnector::on_close(Connection* connection) {
  if (is_canceled()) {
    finish();
  } else {
    on_error(CONTROL_CONNECTION_ERROR_CLOSE,
             "Control connection closed prematurely");
  }
}

} // namespace cass
