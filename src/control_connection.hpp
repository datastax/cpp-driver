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

#ifndef __CASS_CONTROL_CONNECTION_HPP_INCLUDED__
#define __CASS_CONTROL_CONNECTION_HPP_INCLUDED__

#include "address.hpp"
#include "config.hpp"
#include "connection.hpp"
#include "connector.hpp"
#include "dense_hash_map.hpp"
#include "request_callback.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "macros.hpp"
#include "response.hpp"
#include "scoped_ptr.hpp"
#include "token_map.hpp"

#include <stdint.h>

#define SELECT_LOCAL "SELECT * FROM system.local WHERE key='local'"
#define SELECT_PEERS "SELECT * FROM system.peers"

#define SELECT_KEYSPACES_20 "SELECT * FROM system.schema_keyspaces"
#define SELECT_COLUMN_FAMILIES_20 "SELECT * FROM system.schema_columnfamilies"
#define SELECT_COLUMNS_20 "SELECT * FROM system.schema_columns"
#define SELECT_USERTYPES_21 "SELECT * FROM system.schema_usertypes"
#define SELECT_FUNCTIONS_22 "SELECT * FROM system.schema_functions"
#define SELECT_AGGREGATES_22 "SELECT * FROM system.schema_aggregates"

#define SELECT_KEYSPACES_30 "SELECT * FROM system_schema.keyspaces"
#define SELECT_TABLES_30 "SELECT * FROM system_schema.tables"
#define SELECT_VIEWS_30 "SELECT * FROM system_schema.views"
#define SELECT_COLUMNS_30 "SELECT * FROM system_schema.columns"
#define SELECT_INDEXES_30 "SELECT * FROM system_schema.indexes"
#define SELECT_USERTYPES_30 "SELECT * FROM system_schema.types"
#define SELECT_FUNCTIONS_30 "SELECT * FROM system_schema.functions"
#define SELECT_AGGREGATES_30 "SELECT * FROM system_schema.aggregates"

#define SELECT_VIRTUAL_KEYSPACES_40 "SELECT * FROM system_virtual_schema.keyspaces"
#define SELECT_VIRTUAL_TABLES_40 "SELECT * FROM system_virtual_schema.tables"
#define SELECT_VIRTUAL_COLUMNS_40 "SELECT * FROM system_virtual_schema.columns"

namespace cass {

class ChainedControlRequestCallback;
class ControlRequestCallback;
class ControlConnection;
class EventResponse;
class RefreshNodeCallback;
class RefreshKeyspaceCallback;
class RefreshTableCallback;
class RefreshTypeCallback;
class RefreshFunctionCallback;

/**
 * A listener for processing control connection events such as topology, node
 * status, and schema changes.
 */
class ControlConnectionListener {
public:
  enum SchemaType {
    KEYSPACE,
    TABLE,
    VIEW,
    COLUMN,
    INDEX,
    USER_TYPE,
    FUNCTION,
    AGGREGATE
  };

  virtual ~ControlConnectionListener() { }

  /**
   * A callback that's called when a host is marked as being UP.
   *
   * @param address The address of the host.
   * @param refreshed The fully populated host if refreshes on UP are enabled.
   */
  virtual void on_up(const Address& address, const Host::Ptr& refreshed) = 0;

  /**
   * A callback that's called when a host is marked as being DOWN.
   *
   * @param address The address of the host.
   */
  virtual void on_down(const Address& address) = 0;

  /**
   * A callback that's called when a new host is added to the cluster.
   *
   * @param host A fully populated host object.
   */
  virtual void on_add(const Host::Ptr& host) = 0;

  /**
   * A callback that's called when a host is removed from a cluster.
   *
   * @param address The address of the host.
   */
  virtual void on_remove(const Address& address) = 0;

  /**
   * A callback that's called when schema is created or updated. Table and
   * materialized view changes will result in several calls to this method for
   * the associated columns and indexes. Column and indexes are not updated
   * without a preceding table or materialized view update.
   *
   * @param type The type of the schema changed.
   * @param result A result response with the row data associated with the
   * schema change.
   */
  virtual void on_update_schema(SchemaType type,
                                const ResultResponse::Ptr& result,
                                const String& keyspace_name,
                                const String& target_name = "") = 0;

  /**
   * A callback that's called when schema is dropped.
   *
   * @param type The type of the schema dropped.
   * @param keyspace_name The keyspace of the schema.
   * @param target_name The name of the table, user type, function, or
   * aggregate dropped. This is the empty string for dropped keyspaces.
   */
  virtual void on_drop_schema(SchemaType type,
                              const String& keyspace_name,
                              const String& target_name = "") = 0;

  /**
   * A callback that's called when the control connection is closed.
   *
   * @param connection The closing control connection.
   */
  virtual void on_close(ControlConnection* connection) = 0;
};

/**
 * A mapping between a host's address and it's listening address. The listening
 * address is used to look up a peer in the "system.peers" table.
 */
class ListenAddressMap : public DenseHashMap<Address, String, AddressHash> {
public:
  ListenAddressMap() {
    set_empty_key(Address::EMPTY_KEY);
    set_deleted_key(Address::DELETED_KEY);
  }
};

/**
 * A control connection. This is a wrapper around a connection that handles
 * schema, node status, and topology changes. This class handles events
 * by running queries on the control connection to get additional information
 * then passing that data to the listener.
 */
class ControlConnection : public RefCounted<ControlConnection>
                        , public ConnectionListener {
public:
  typedef SharedRefPtr<ControlConnection> Ptr;

  enum RefreshNodeType {
    NEW_NODE,
    MOVED_NODE,
    UP_WITH_REFRESH
  };

  /**
   * Constructor. Don't use directly.
   *
   * @param connection The wrapped connection.
   * @param use_schema If true then connection will get additional data for
   * schema events, otherwise it will ignore those events.
   * @param token_aware_routing If true the connection will get additional data
   * for keyspace schema changes, otherwise it will ignore those events.
   * @param refresh_node_info_on_up If true then connection will refresh node
   * data for UP events.
   * @param server_version The version number of the server implementation.
   * @param listen_addresses The current state of the listen addresses map.
   */
  ControlConnection(const Connection::Ptr& connection,
                    bool use_schema,
                    bool token_aware_routing,
                    bool refresh_node_info_on_up,
                    const VersionNumber& server_version,
                    ListenAddressMap listen_addresses);

  /**
   * Write a request and flush immediately.
   *
   * @param The request callback to write and handle the request.
   * @return The number of bytes written to the connection. If negative then
   * an error occurred.
   */
  int32_t write_and_flush(const RequestCallback::Ptr& callback);

  /**
   * Close the connection.
   */
  void close();

  /**
   * Close the connection with an error.
   */
  void defunct();

  /**
   * Set the listener that will handle control connection events.
   *
   * @param listener The listener.
   */
  void set_listener(ControlConnectionListener* listener);

public:
  const Address& address() const {
    return connection_->address();
  }

  const String& address_string() const {
    return connection_->address_string();
  }

  int protocol_version() const {
    return connection_->protocol_version();
  }

  const VersionNumber& server_version() {
    return server_version_;
  }

  uv_loop_t* loop() { return connection_->loop(); }

private:
  friend class ControlConnector;
  friend class RefreshNodeCallback;
  friend class RefreshKeyspaceCallback;
  friend class RefreshTableCallback;
  friend class RefreshTypeCallback;
  friend class RefreshFunctionCallback;

private:
  void refresh_node(RefreshNodeType type, const Address& address);
  static void on_refresh_node(ControlRequestCallback* callback);
  void handle_refresh_node(RefreshNodeCallback* callback);

  void refresh_keyspace(const StringRef& keyspace_name);
  static void on_refresh_keyspace(ControlRequestCallback* callback);
  void handle_refresh_keyspace(RefreshKeyspaceCallback* callback);

  void refresh_table_or_view(const StringRef& keyspace_name,
                             const StringRef& table_or_view_name);
  static void on_refresh_table_or_view(ChainedControlRequestCallback* callback);
  void handle_refresh_table_or_view(RefreshTableCallback* callback);

  void refresh_type(const StringRef& keyspace_name,
                    const StringRef& type_name);
  static void on_refresh_type(ControlRequestCallback* callback);
  void handle_refresh_type(RefreshTypeCallback* callback);

  void refresh_function(const StringRef& keyspace_name,
                        const StringRef& function_name,
                        const StringRefVec& arg_types,
                        bool is_aggregate);
  static void on_refresh_function(ControlRequestCallback* callback);
  void handle_refresh_function(RefreshFunctionCallback* callback);

  // Connection listener methods
  virtual void on_close(Connection* connection);
  virtual void on_event(const EventResponse::Ptr& response);

private:
  Connection::Ptr connection_;
  bool use_schema_;
  bool token_aware_routing_;
  bool refresh_node_info_on_up_;
  VersionNumber server_version_;
  ListenAddressMap listen_addresses_;
  ControlConnectionListener* listener_;
};

} // namespace cass

#endif
