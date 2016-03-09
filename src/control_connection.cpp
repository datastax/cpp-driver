/*
  Copyright (c) 2014-2016 DataStax

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

#include "control_connection.hpp"

#include "collection_iterator.hpp"
#include "constants.hpp"
#include "event_response.hpp"
#include "load_balancing.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "query_request.hpp"
#include "result_iterator.hpp"
#include "error_response.hpp"
#include "result_response.hpp"
#include "session.hpp"
#include "timer.hpp"

#include <iomanip>
#include <sstream>
#include <vector>

#define SELECT_LOCAL "SELECT data_center, rack, release_version FROM system.local WHERE key='local'"
#define SELECT_LOCAL_TOKENS "SELECT data_center, rack, release_version, partitioner, tokens FROM system.local WHERE key='local'"
#define SELECT_PEERS "SELECT peer, data_center, rack, release_version, rpc_address FROM system.peers"
#define SELECT_PEERS_TOKENS "SELECT peer, data_center, rack, release_version, rpc_address, tokens FROM system.peers"

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

namespace cass {

class ControlStartupQueryPlan : public QueryPlan {
public:
  ControlStartupQueryPlan(const HostMap& hosts)
    : hosts_(hosts)
    , it_(hosts_.begin()) {}

  virtual SharedRefPtr<Host> compute_next() {
    if (it_ == hosts_.end()) return SharedRefPtr<Host>();
    const SharedRefPtr<Host>& host = it_->second;
    ++it_;
    return host;
  }

private:
  const HostMap hosts_;
  HostMap::const_iterator it_;
};

bool ControlConnection::determine_address_for_peer_host(const Address& connected_address,
                                                        const Value* peer_value,
                                                        const Value* rpc_value,
                                                        Address* output) {
  Address peer_address;
  Address::from_inet(peer_value->data(), peer_value->size(),
                     connected_address.port(), &peer_address);
  if (rpc_value->size() > 0) {
    Address::from_inet(rpc_value->data(), rpc_value->size(),
                       connected_address.port(), output);
    if (connected_address.compare(*output) == 0 ||
        connected_address.compare(peer_address) == 0) {
      LOG_DEBUG("system.peers on %s contains a line with rpc_address for itself. "
                "This is not normal, but is a known problem for some versions of DSE. "
                "Ignoring this entry.", connected_address.to_string(false).c_str());
      return false;
    }
    if (bind_any_ipv4_.compare(*output) == 0 ||
        bind_any_ipv6_.compare(*output) == 0) {
      LOG_WARN("Found host with 'bind any' for rpc_address; using listen_address (%s) to contact instead. "
               "If this is incorrect you should configure a specific interface for rpc_address on the server.",
               peer_address.to_string(false).c_str());
      *output = peer_address;
    }
  } else {
    LOG_WARN("No rpc_address for host %s in system.peers on %s. "
             "Ignoring this entry.", peer_address.to_string(false).c_str(),
             connected_address.to_string(false).c_str());
    return false;
  }
  return true;
}

ControlConnection::ControlConnection()
  : state_(CONTROL_STATE_NEW)
  , session_(NULL)
  , connection_(NULL)
  , protocol_version_(0)
  , should_query_tokens_(false) {}

const SharedRefPtr<Host> ControlConnection::connected_host() const {
  return session_->get_host(current_host_address_);
}

void ControlConnection::clear() {
  state_ = CONTROL_STATE_NEW;
  session_ = NULL;
  connection_ = NULL;
  reconnect_timer_.stop();
  query_plan_.reset();
  protocol_version_ = 0;
  last_connection_error_.clear();
  should_query_tokens_ = false;
}

void ControlConnection::connect(Session* session) {
  session_ = session;
  query_plan_.reset(new ControlStartupQueryPlan(session_->hosts_)); // No hosts lock necessary (read-only)
  protocol_version_ = session_->config().protocol_version();
  should_query_tokens_ = session_->config().token_aware_routing();
  if (protocol_version_ < 0) {
    protocol_version_ = CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION;
  }

  if (session_->config().use_schema()) {
    set_event_types(CASS_EVENT_TOPOLOGY_CHANGE | CASS_EVENT_STATUS_CHANGE |
                    CASS_EVENT_SCHEMA_CHANGE);
  } else {
    set_event_types(CASS_EVENT_TOPOLOGY_CHANGE | CASS_EVENT_STATUS_CHANGE);
  }

  reconnect(false);
}

void ControlConnection::close() {
  state_ = CONTROL_STATE_CLOSED;
  if (connection_ != NULL) {
    connection_->close();
  }
  reconnect_timer_.stop();
}

void ControlConnection::schedule_reconnect(uint64_t ms) {
  reconnect_timer_.start(session_->loop(),
                         ms,
                         this,
                         ControlConnection::on_reconnect);
}

void ControlConnection::reconnect(bool retry_current_host) {
  if (state_ == CONTROL_STATE_CLOSED) {
    return;
  }

  if (!retry_current_host) {
    if (!query_plan_->compute_next(&current_host_address_)) {
      if (state_ == CONTROL_STATE_READY) {
        schedule_reconnect(1000); // TODO(mpenick): Configurable?
      } else {
        session_->on_control_connection_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                                              "No hosts available for the control connection");
      }
      return;
    }
  }

  if (connection_ != NULL) {
    connection_->close();
  }

  connection_ = new Connection(session_->loop(),
                               session_->config(),
                               session_->metrics(),
                               current_host_address_,
                               "", // No keyspace
                               protocol_version_,
                               this);
  connection_->connect();
}

void ControlConnection::on_ready(Connection* connection) {
  LOG_DEBUG("Connection ready on host %s",
            connection->address().to_string().c_str());

  // A protocol version is need to encode/decode maps properly
  session_->metadata().set_protocol_version(protocol_version_);

  // The control connection has to refresh meta when there's a reconnect because
  // events could have been missed while not connected.
  query_meta_hosts();
}

void ControlConnection::on_close(Connection* connection) {
  bool retry_current_host = false;

  if (state_ != CONTROL_STATE_CLOSED) {
    LOG_WARN("Lost control connection on host %s", connection->address_string().c_str());
  }

  // This pointer to the connection is no longer valid once it's closed
  connection_ = NULL;

  if (state_ == CONTROL_STATE_NEW) {
    if (connection->is_invalid_protocol()) {
      if (protocol_version_ <= 1) {
        LOG_ERROR("Host %s does not support any valid protocol version",
                  connection->address_string().c_str());
        session_->on_control_connection_error(CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL,
                                             "Not even protocol version 1 is supported");
        return;
      }
      LOG_WARN("Host %s does not support protocol version %d. "
               "Trying protocol version %d...",
               connection->address_string().c_str(),
               protocol_version_,
               protocol_version_ - 1);
      protocol_version_--;
      retry_current_host = true;
    } else if (connection->is_auth_error()) {
      session_->on_control_connection_error(CASS_ERROR_SERVER_BAD_CREDENTIALS,
                                            connection->error_message());
      return;
    } else if (connection->is_ssl_error()) {
      session_->on_control_connection_error(connection->ssl_error_code(),
                                            connection->error_message());
      return;
    }
  }

  reconnect(retry_current_host);
}

void ControlConnection::on_event(EventResponse* response) {
  switch (response->event_type()) {
    case CASS_EVENT_TOPOLOGY_CHANGE: {
      std::string address_str = response->affected_node().to_string();
      switch (response->topology_change()) {
        case EventResponse::NEW_NODE: {
          LOG_INFO("New node %s added", address_str.c_str());
          SharedRefPtr<Host> host = session_->get_host(response->affected_node());
          if (!host) {
            host = session_->add_host(response->affected_node());
            refresh_node_info(host, true, true);
          }
          break;
        }

        case EventResponse::REMOVED_NODE: {
          LOG_INFO("Node %s removed", address_str.c_str());
          SharedRefPtr<Host> host = session_->get_host(response->affected_node());
          if (host) {
            session_->on_remove(host);
            session_->metadata().remove_host(host);
          } else {
            LOG_DEBUG("Tried to remove host %s that doesn't exist", address_str.c_str());
          }
          break;
        }

        case EventResponse::MOVED_NODE:
          LOG_INFO("Node %s moved", address_str.c_str());
          SharedRefPtr<Host> host = session_->get_host(response->affected_node());
          if (host) {
            refresh_node_info(host, false, true);
          } else {
            LOG_DEBUG("Move event for host %s that doesn't exist", address_str.c_str());
            session_->metadata().remove_host(host);
          }
          break;
      }
      break;
    }

    case CASS_EVENT_STATUS_CHANGE: {
      std::string address_str = response->affected_node().to_string();
      switch (response->status_change()) {
        case EventResponse::UP: {
          LOG_INFO("Node %s is up", address_str.c_str());
          on_up(response->affected_node());
          break;
        }

        case EventResponse::DOWN: {
          LOG_INFO("Node %s is down", address_str.c_str());
          on_down(response->affected_node());
          break;
        }
      }
      break;
    }

    case CASS_EVENT_SCHEMA_CHANGE:
      LOG_DEBUG("Schema change (%d): %.*s %.*s\n",
                response->schema_change(),
                (int)response->keyspace().size(), response->keyspace().data(),
                (int)response->target().size(), response->target().data());
      switch (response->schema_change()) {
        case EventResponse::CREATED:
        case EventResponse::UPDATED:
          switch (response->schema_change_target()) {
            case EventResponse::KEYSPACE:
              refresh_keyspace(response->keyspace());
              break;
            case EventResponse::TABLE:
              refresh_table_or_view(response->keyspace(), response->target());
              break;
            case EventResponse::TYPE:
              refresh_type(response->keyspace(), response->target());
              break;
            case EventResponse::FUNCTION:
            case EventResponse::AGGREGATE:
              refresh_function(response->keyspace(),
                               response->target(),
                               response->arg_types(),
                               response->schema_change_target() == EventResponse::AGGREGATE);
              break;
          }
          break;

        case EventResponse::DROPPED:
          switch (response->schema_change_target()) {
            case EventResponse::KEYSPACE:
              session_->metadata().drop_keyspace(response->keyspace().to_string());
              break;
            case EventResponse::TABLE:
              session_->metadata().drop_table_or_view(response->keyspace().to_string(),
                                                      response->target().to_string());
              break;
            case EventResponse::TYPE:
              session_->metadata().drop_user_type(response->keyspace().to_string(),
                                                  response->target().to_string());
              break;
            case EventResponse::FUNCTION:
              session_->metadata().drop_function(response->keyspace().to_string(),
                                                 Metadata::full_function_name(response->target().to_string(),
                                                                              to_strings(response->arg_types())));
              break;
            case EventResponse::AGGREGATE:
              session_->metadata().drop_aggregate(response->keyspace().to_string(),
                                                  Metadata::full_function_name(response->target().to_string(),
                                                                               to_strings(response->arg_types())));
              break;
          }
          break;

      }
      break;

    default:
      assert(false);
      break;
  }
}

void ControlConnection::query_meta_hosts() {
  ScopedRefPtr<ControlMultipleRequestHandler<UnusedData> > handler(
        new ControlMultipleRequestHandler<UnusedData>(this, ControlConnection::on_query_hosts, UnusedData()));
  handler->execute_query("local", SELECT_LOCAL_TOKENS);
  handler->execute_query("peers", SELECT_PEERS_TOKENS);
}

void ControlConnection::on_query_hosts(ControlConnection* control_connection,
                                       const UnusedData& data,
                                       const MultipleRequestHandler::ResponseMap& responses) {
  Connection* connection = control_connection->connection_;
  if (connection == NULL) {
    return;
  }

  Session* session = control_connection->session_;

  bool is_initial_connection = (control_connection->state_ == CONTROL_STATE_NEW);

  // If the 'system.local' table is empty the connection isn't used as a control
  // connection because at least one node's information is required (itself). An
  // empty 'system.local' can happen during the bootstrapping process on some
  // versions of Cassandra. If this happens we defunct the connection and move
  // to the next node in the query plan.
  {
    SharedRefPtr<Host> host = session->get_host(connection->address());
    if (host) {
      host->set_mark(session->current_host_mark_);

      ResultResponse* local_result;
      if (MultipleRequestHandler::get_result_response(responses, "local", &local_result) &&
          local_result->row_count() > 0) {
        local_result->decode_first_row();
        control_connection->update_node_info(host, &local_result->first_row());
        session->metadata().set_cassandra_version(host->cassandra_version());
      } else {
        LOG_WARN("No row found in %s's local system table",
                 connection->address_string().c_str());
        connection->defunct();
        return;
      }
    } else {
      LOG_WARN("Host %s from local system table not found",
               connection->address_string().c_str());
      connection->defunct();
      return;
    }
  }

  {
    ResultResponse* peers_result;
    if (MultipleRequestHandler::get_result_response(responses, "peers", &peers_result)) {
      peers_result->decode_first_row();
      ResultIterator rows(peers_result);
      while (rows.next()) {
        Address address;
        const Row* row = rows.row();
        if (!determine_address_for_peer_host(connection->address(),
                                             row->get_by_name("peer"),
                                             row->get_by_name("rpc_address"),
                                             &address)) {
          continue;
        }

        SharedRefPtr<Host> host = session->get_host(address);
        bool is_new = false;
        if (!host) {
          is_new = true;
          host = session->add_host(address);
        }

        host->set_mark(session->current_host_mark_);

        control_connection->update_node_info(host, rows.row());
        if (is_new && !is_initial_connection) {
          session->on_add(host, false);
        }
      }
    }
  }

  session->purge_hosts(is_initial_connection);

  if (session->config().use_schema()) {
    control_connection->query_meta_schema();
  } else if (is_initial_connection) {
    control_connection->state_ = CONTROL_STATE_READY;
    session->on_control_connection_ready();
    // Create a new query plan that considers all the new hosts from the
    // "system" tables.
    control_connection->query_plan_.reset(session->new_query_plan());
  }
}

//TODO: query and callbacks should be in Metadata
// punting for now because of tight coupling of Session and CC state
void ControlConnection::query_meta_schema() {
  ScopedRefPtr<ControlMultipleRequestHandler<UnusedData> > handler(
        new ControlMultipleRequestHandler<UnusedData>(this, ControlConnection::on_query_meta_schema, UnusedData()));

  if (session_->metadata().cassandra_version() >= VersionNumber(3, 0, 0)) {
    handler->execute_query("keyspaces", SELECT_KEYSPACES_30);
    handler->execute_query("tables", SELECT_TABLES_30);
    handler->execute_query("views", SELECT_VIEWS_30);
    handler->execute_query("columns", SELECT_COLUMNS_30);
    handler->execute_query("indexes", SELECT_INDEXES_30);
    handler->execute_query("user_types", SELECT_USERTYPES_30);
    handler->execute_query("functions", SELECT_FUNCTIONS_30);
    handler->execute_query("aggregates", SELECT_AGGREGATES_30);
  } else {
    handler->execute_query("keyspaces", SELECT_KEYSPACES_20);
    handler->execute_query("tables", SELECT_COLUMN_FAMILIES_20);
    handler->execute_query("columns", SELECT_COLUMNS_20);
    if (session_->metadata().cassandra_version() >= VersionNumber(2, 1, 0)) {
      handler->execute_query("user_types", SELECT_USERTYPES_21);
    }
    if (session_->metadata().cassandra_version() >= VersionNumber(2, 2, 0)) {
      handler->execute_query("functions", SELECT_FUNCTIONS_22);
      handler->execute_query("aggregates", SELECT_AGGREGATES_22);
    }
  }
}

void ControlConnection::on_query_meta_schema(ControlConnection* control_connection,
                                             const UnusedData& unused,
                                             const MultipleRequestHandler::ResponseMap& responses) {
  Connection* connection = control_connection->connection_;
  if (connection == NULL) {
    return;
  }

  Session* session = control_connection->session_;

  session->metadata().clear_and_update_back();

  bool is_initial_connection = (control_connection->state_ == CONTROL_STATE_NEW);

  ResultResponse* keyspaces_result;
  if (MultipleRequestHandler::get_result_response(responses, "keyspaces", &keyspaces_result)) {
    session->metadata().update_keyspaces(keyspaces_result);
  }

  ResultResponse* tables_result;
  if (MultipleRequestHandler::get_result_response(responses, "tables", &tables_result)) {
    session->metadata().update_tables(tables_result);
  }

  ResultResponse* views_result;
  if (MultipleRequestHandler::get_result_response(responses, "views", &views_result)) {
    session->metadata().update_views(views_result);
  }

  ResultResponse* columns_result = NULL;
  if (MultipleRequestHandler::get_result_response(responses, "columns", &columns_result)) {
    session->metadata().update_columns(columns_result);
  }

  ResultResponse* indexes_result;
  if (MultipleRequestHandler::get_result_response(responses, "indexes", &indexes_result)) {
    session->metadata().update_indexes(indexes_result);
  }

  ResultResponse* user_types_result;
  if (MultipleRequestHandler::get_result_response(responses, "user_types", &user_types_result)) {
    session->metadata().update_user_types(user_types_result);
  }

  ResultResponse* functions_result;
  if (MultipleRequestHandler::get_result_response(responses, "functions", &functions_result)) {
    session->metadata().update_functions(functions_result);
  }

  ResultResponse* aggregates_result;
  if (MultipleRequestHandler::get_result_response(responses, "aggregates", &aggregates_result)) {
    session->metadata().update_aggregates(aggregates_result);
  }

  session->metadata().swap_to_back_and_update_front();
  if (control_connection->should_query_tokens_) session->metadata().build();

  if (is_initial_connection) {
    control_connection->state_ = CONTROL_STATE_READY;
    session->on_control_connection_ready();
    // Create a new query plan that considers all the new hosts from the
    // "system" tables.
    control_connection->query_plan_.reset(session->new_query_plan());
  }
}

void ControlConnection::refresh_node_info(SharedRefPtr<Host> host,
                                          bool is_new_node,
                                          bool query_tokens) {
  if (connection_ == NULL) {
    return;
  }

  bool is_connected_host = host->address().compare(connection_->address()) == 0;

  std::string query;
  ControlHandler<RefreshNodeData>::ResponseCallback response_callback;

  bool token_query = should_query_tokens_ && (host->was_just_added() || query_tokens);
  if (is_connected_host || !host->listen_address().empty()) {
    if (is_connected_host) {
      query.assign(token_query ? SELECT_LOCAL_TOKENS : SELECT_LOCAL);
    } else {
      query.assign(token_query ? SELECT_PEERS_TOKENS : SELECT_PEERS);
      query.append(" WHERE peer = '");
      query.append(host->listen_address());
      query.append("'");
    }
    response_callback = ControlConnection::on_refresh_node_info;
  } else {
    query.assign(token_query ? SELECT_PEERS_TOKENS : SELECT_PEERS);
    response_callback = ControlConnection::on_refresh_node_info_all;
  }

  LOG_DEBUG("refresh_node_info: %s", query.c_str());

  RefreshNodeData data(host, is_new_node);
  ScopedRefPtr<ControlHandler<RefreshNodeData> > handler(
        new ControlHandler<RefreshNodeData>(new QueryRequest(query),
                                            this,
                                            response_callback,
                                            data));
  if (!connection_->write(handler.get())) {
    LOG_ERROR("No more stream available while attempting to refresh node info");
  }
}

void ControlConnection::on_refresh_node_info(ControlConnection* control_connection,
                                             const RefreshNodeData& data,
                                             Response* response) {
  Connection* connection = control_connection->connection_;
  if (connection == NULL) {
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response);

  if (result->row_count() == 0) {
    std::string host_address_str = data.host->address().to_string();
    LOG_ERROR("No row found for host %s in %s's local/peers system table. "
              "%s will be ignored.",
              host_address_str.c_str(),
              connection->address_string().c_str(),
              host_address_str.c_str());
    return;
  }
  result->decode_first_row();
  control_connection->update_node_info(data.host, &result->first_row());

  if (data.is_new_node) {
    control_connection->session_->on_add(data.host, false);
  }
}

void ControlConnection::on_refresh_node_info_all(ControlConnection* control_connection,
                                                 const RefreshNodeData& data,
                                                 Response* response) {
  Connection* connection = control_connection->connection_;
  if (connection == NULL) {
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response);

  if (result->row_count() == 0) {
    std::string host_address_str = data.host->address().to_string();
    LOG_ERROR("No row found for host %s in %s's peers system table. "
              "%s will be ignored.",
              host_address_str.c_str(),
              connection->address_string().c_str(),
              host_address_str.c_str());
    return;
  }

  result->decode_first_row();
  ResultIterator rows(result);
  while (rows.next()) {
    const Row* row = rows.row();
    Address address;
    bool is_valid_address
        = determine_address_for_peer_host(connection->address(),
                                          row->get_by_name("peer"),
                                          row->get_by_name("rpc_address"),
                                          &address);
    if (is_valid_address && data.host->address().compare(address) == 0) {
      control_connection->update_node_info(data.host, row);
      if (data.is_new_node) {
        control_connection->session_->on_add(data.host, false);
      }
      break;
    }
  }
}

void ControlConnection::update_node_info(SharedRefPtr<Host> host, const Row* row) {
  const Value* v;

  std::string rack;
  row->get_string_by_name("rack", &rack);

  std::string dc;
  row->get_string_by_name("data_center", &dc);

  std::string release_version;
  row->get_string_by_name("release_version", &release_version);

  // This value is not present in the "system.local" query
  v = row->get_by_name("peer");
  if (v != NULL) {
    Address listen_address;
    Address::from_inet(v->data(), v->size(),
                       connection_->address().port(),
                       &listen_address);
    host->set_listen_address(listen_address.to_string());
  }

  if ((!rack.empty() && rack != host->rack()) ||
      (!dc.empty() && dc != host->dc())) {
    if (!host->was_just_added()) {
      session_->load_balancing_policy_->on_remove(host);
    }
    host->set_rack_and_dc(rack, dc);
    if (!host->was_just_added()) {
      session_->load_balancing_policy_->on_add(host);
    }
  }

  VersionNumber cassandra_version;
  if (cassandra_version.parse(release_version)) {
    host->set_cassaandra_version(cassandra_version);
  } else {
    LOG_WARN("Invalid release version string \"%s\" on host %s",
             release_version.c_str(),
             host->address().to_string().c_str());
  }

  if (should_query_tokens_) {
    bool is_connected_host = connection_ != NULL && host->address().compare(connection_->address()) == 0;
    std::string partitioner;
    if (is_connected_host && row->get_string_by_name("partitioner", &partitioner)) {
      session_->metadata().set_partitioner(partitioner);
    }
    v = row->get_by_name("tokens");
    if (v != NULL) {
      CollectionIterator i(v);
      TokenStringList tokens;
      while (i.next()) {
        tokens.push_back(i.value()->to_string_ref());
      }
      if (!tokens.empty()) {
        session_->metadata().update_host(host, tokens);
      }
    }
  }
}

void ControlConnection::refresh_keyspace(const StringRef& keyspace_name) {
  std::string query;

  if (session_->metadata().cassandra_version() >= VersionNumber(3, 0, 0)) {
    query.assign(SELECT_KEYSPACES_30);
  }  else {
    query.assign(SELECT_KEYSPACES_20);
  }
  query.append(" WHERE keyspace_name='")
       .append(keyspace_name.data(), keyspace_name.size())
       .append("'");

  LOG_DEBUG("Refreshing keyspace %s", query.c_str());

  connection_->write(
        new ControlHandler<std::string>(new QueryRequest(query),
                                        this,
                                        ControlConnection::on_refresh_keyspace,
                                        keyspace_name.to_string()));
}

void ControlConnection::on_refresh_keyspace(ControlConnection* control_connection,
                                            const std::string& keyspace_name,
                                            Response* response) {
  ResultResponse* result = static_cast<ResultResponse*>(response);
  if (result->row_count() == 0) {
    LOG_ERROR("No row found for keyspace %s in system schema table.",
              keyspace_name.c_str());
    return;
  }
  control_connection->session_->metadata().update_keyspaces(result);
}

void ControlConnection::refresh_table_or_view(const StringRef& keyspace_name,
                                              const StringRef& table_or_view_name) {
  std::string table_query;
  std::string view_query;
  std::string column_query;
  std::string index_query;

  if (session_->metadata().cassandra_version() >= VersionNumber(3, 0, 0)) {
    table_query.assign(SELECT_TABLES_30);
    table_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND table_name='").append(table_or_view_name.data(), table_or_view_name.size()).append("'");

    view_query.assign(SELECT_VIEWS_30);
    view_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND view_name='").append(table_or_view_name.data(), table_or_view_name.size()).append("'");

    column_query.assign(SELECT_COLUMNS_30);
    column_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND table_name='").append(table_or_view_name.data(), table_or_view_name.size()).append("'");

    index_query.assign(SELECT_INDEXES_30);
    index_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND table_name='").append(table_or_view_name.data(), table_or_view_name.size()).append("'");

    LOG_DEBUG("Refreshing table/view %s; %s; %s; %s", table_query.c_str(), view_query.c_str(),
                                                      column_query.c_str(), index_query.c_str());
  } else {
    table_query.assign(SELECT_COLUMN_FAMILIES_20);
    table_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND columnfamily_name='").append(table_or_view_name.data(), table_or_view_name.size()).append("'");

    column_query.assign(SELECT_COLUMNS_20);
    column_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
        .append("' AND columnfamily_name='").append(table_or_view_name.data(), table_or_view_name.size()).append("'");

    LOG_DEBUG("Refreshing table %s; %s", table_query.c_str(), column_query.c_str());
  }

  ScopedRefPtr<ControlMultipleRequestHandler<RefreshTableData> > handler(
        new ControlMultipleRequestHandler<RefreshTableData>(this,
                                                            ControlConnection::on_refresh_table_or_view,
                                                            RefreshTableData(keyspace_name.to_string(), table_or_view_name.to_string())));
  handler->execute_query("tables", table_query);
  if (!view_query.empty()) {
    handler->execute_query("views", view_query);
  }
  handler->execute_query("columns", column_query);
  if (!index_query.empty()) {
    handler->execute_query("indexes", index_query);
  }
}

void ControlConnection::on_refresh_table_or_view(ControlConnection* control_connection,
                                                 const RefreshTableData& data,
                                                 const MultipleRequestHandler::ResponseMap& responses) {
  ResultResponse* tables_result;
  Session* session = control_connection->session_;
  if (!MultipleRequestHandler::get_result_response(responses, "tables", &tables_result) ||
      tables_result->row_count() == 0) {
    ResultResponse* views_result;
    if (!MultipleRequestHandler::get_result_response(responses, "views", &views_result) ||
        views_result->row_count() == 0) {
      LOG_ERROR("No row found for table (or view) %s.%s in system schema tables.",
                data.keyspace_name.c_str(), data.table_or_view_name.c_str());
      return;
    }
    session->metadata().update_views(views_result);
  } else {
    session->metadata().update_tables(tables_result);
  }

  ResultResponse* columns_result;
  if (MultipleRequestHandler::get_result_response(responses, "columns", &columns_result)) {
    session->metadata().update_columns(columns_result);
  }

  ResultResponse* indexes_result;
  if (MultipleRequestHandler::get_result_response(responses, "indexes", &indexes_result)) {
    session->metadata().update_indexes(indexes_result);
  }
}


void ControlConnection::refresh_type(const StringRef& keyspace_name,
                                     const StringRef& type_name) {

  std::string query;
  if (session_->metadata().cassandra_version() >= VersionNumber(3, 0, 0)) {
    query.assign(SELECT_USERTYPES_30);
  } else {
    query.assign(SELECT_USERTYPES_21);
  }

  query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
                .append("' AND type_name='").append(type_name.data(), type_name.size()).append("'");

  LOG_DEBUG("Refreshing type %s", query.c_str());

  connection_->write(
        new ControlHandler<std::pair<std::string, std::string> >(new QueryRequest(query),
                                        this,
                                        ControlConnection::on_refresh_type,
                                        std::make_pair(keyspace_name.to_string(), type_name.to_string())));
}

void ControlConnection::on_refresh_type(ControlConnection* control_connection,
                                        const std::pair<std::string, std::string>& keyspace_and_type_names,
                                        Response* response) {
  ResultResponse* result = static_cast<ResultResponse*>(response);
  if (result->row_count() == 0) {
    LOG_ERROR("No row found for keyspace %s and type %s in system schema.",
              keyspace_and_type_names.first.c_str(),
              keyspace_and_type_names.second.c_str());
    return;
  }
  control_connection->session_->metadata().update_user_types(result);
}

void ControlConnection::refresh_function(const StringRef& keyspace_name,
                                         const StringRef& function_name,
                                         const StringRefVec& arg_types,
                                         bool is_aggregate) {

  std::string query;
  if (session_->metadata().cassandra_version() >= VersionNumber(3, 0, 0)) {
    if (is_aggregate) {
      query.assign(SELECT_AGGREGATES_30);
      query.append(" WHERE keyspace_name=? AND aggregate_name=? AND argument_types=?");
    } else {
      query.assign(SELECT_FUNCTIONS_30);
      query.append(" WHERE keyspace_name=? AND function_name=? AND argument_types=?");
    }
  } else {
    if (is_aggregate) {
      query.assign(SELECT_AGGREGATES_22);
      query.append(" WHERE keyspace_name=? AND aggregate_name=? AND signature=?");
    } else {
      query.assign(SELECT_FUNCTIONS_22);
      query.append(" WHERE keyspace_name=? AND function_name=? AND signature=?");
    }
  }

  LOG_DEBUG("Refreshing %s %s in keyspace %s",
            is_aggregate ? "aggregate" : "function",
            Metadata::full_function_name(function_name.to_string(), to_strings(arg_types)).c_str(),
            std::string(keyspace_name.data(), keyspace_name.length()).c_str());

  SharedRefPtr<QueryRequest> request(new QueryRequest(query, 3));
  SharedRefPtr<Collection> signature(new Collection(CASS_COLLECTION_TYPE_LIST, arg_types.size()));

  for (StringRefVec::const_iterator i = arg_types.begin(),
       end = arg_types.end();
       i != end;
       ++i) {
    signature->append(CassString(i->data(), i->size()));
  }

  request->set(0, CassString(keyspace_name.data(), keyspace_name.size()));
  request->set(1, CassString(function_name.data(), function_name.size()));
  request->set(2, signature.get());

  connection_->write(
        new ControlHandler<RefreshFunctionData>(request.get(),
                                                this,
                                                ControlConnection::on_refresh_function,
                                                RefreshFunctionData(keyspace_name, function_name, arg_types, is_aggregate)));
}

void ControlConnection::on_refresh_function(ControlConnection* control_connection,
                                            const RefreshFunctionData& data,
                                            Response* response) {
  ResultResponse* result = static_cast<ResultResponse*>(response);
  if (result->row_count() == 0) {
    LOG_ERROR("No row found for keyspace %s and %s %s",
              data.keyspace.c_str(),
              data.is_aggregate ? "aggregate" : "function",
              Metadata::full_function_name(data.function, data.arg_types).c_str());
    return;
  }
  if (data.is_aggregate) {
    control_connection->session_->metadata().update_aggregates(result);
  } else {
    control_connection->session_->metadata().update_functions(result);
  }
}

bool ControlConnection::handle_query_invalid_response(Response* response) {
  if (check_error_or_invalid_response("ControlConnection", CQL_OPCODE_RESULT,
                                      response)) {
    if (connection_ != NULL) {
      connection_->defunct();
    }
    return true;
  }
  return false;
}

void ControlConnection::handle_query_failure(CassError code, const std::string& message) {
  // TODO(mpenick): This is a placeholder and might not be the right action for
  // all error scenarios
  if (connection_ != NULL) {
    connection_->defunct();
  }
}

void ControlConnection::handle_query_timeout() {
  // TODO(mpenick): Is this the best way to handle a timeout?
  if (connection_ != NULL) {
    connection_->defunct();
  }
}

void ControlConnection::on_up(const Address& address) {
  SharedRefPtr<Host> host = session_->get_host(address);
  if (host) {
    if (host->is_up()) return;

    // Immediately mark the node as up and asynchronously attempt
    // to refresh the node's information. This is done because
    // a control connection may not be available because it's
    // waiting for a node to be marked as up.
    session_->on_up(host);
    refresh_node_info(host, false);
  } else {
    host = session_->add_host(address);
    refresh_node_info(host, true);
  }
}

void ControlConnection::on_down(const Address& address) {
  SharedRefPtr<Host> host = session_->get_host(address);
  if (host) {
    if (host->is_down()) return;

    session_->on_down(host);
  } else {
    LOG_DEBUG("Tried to down host %s that doesn't exist", address.to_string().c_str());
  }
}

void ControlConnection::on_reconnect(Timer* timer) {
  ControlConnection* control_connection = static_cast<ControlConnection*>(timer->data());
  control_connection->query_plan_.reset(control_connection->session_->new_query_plan());
  control_connection->reconnect(false);
}

template<class T>
void ControlConnection::ControlMultipleRequestHandler<T>::on_set(
    const MultipleRequestHandler::ResponseMap& responses) {
  bool has_error = false;
  for (MultipleRequestHandler::ResponseMap::const_iterator it = responses.begin(),
       end = responses.end(); it != end; ++it) {
    if (control_connection_->handle_query_invalid_response(it->second.get())) {
      has_error = true;
    }
  }
  if (has_error) return;
  response_callback_(control_connection_, data_, responses);
}

Address ControlConnection::bind_any_ipv4_("0.0.0.0", 0);
Address ControlConnection::bind_any_ipv6_("::", 0);

} // namespace cass
