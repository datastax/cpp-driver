/*
  Copyright (c) 2014-2015 DataStax

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
#include "query_request.hpp"
#include "result_iterator.hpp"
#include "error_response.hpp"
#include "result_response.hpp"
#include "schema_metadata.hpp"
#include "session.hpp"
#include "timer.hpp"

#include <boost/bind.hpp>

#include <iomanip>
#include <sstream>
#include <vector>

#define HIGHEST_SUPPORTED_PROTOCOL_VERSION 2

#define SELECT_LOCAL "SELECT data_center, rack FROM system.local WHERE key='local'"
#define SELECT_LOCAL_TOKENS "SELECT data_center, rack, partitioner, tokens FROM system.local WHERE key='local'"
#define SELECT_PEERS "SELECT peer, data_center, rack, rpc_address FROM system.peers"
#define SELECT_PEERS_TOKENS "SELECT peer, data_center, rack, rpc_address, tokens FROM system.peers"

#define SELECT_KEYSPACES "SELECT * FROM system.schema_keyspaces"
#define SELECT_COLUMN_FAMILIES "SELECT * FROM system.schema_columnfamilies"
#define SELECT_COLUMNS "SELECT * FROM system.schema_columns"


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
  Address::from_inet(peer_value->buffer().data(), peer_value->buffer().size(),
                     connected_address.port(), &peer_address);
  if (rpc_value->buffer().size() > 0) {
    Address::from_inet(rpc_value->buffer().data(), rpc_value->buffer().size(),
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
  , reconnect_timer_(NULL)
  , protocol_version_(0)
  , query_tokens_(false) {}

const SharedRefPtr<Host> ControlConnection::connected_host() const {
  return session_->get_host(current_host_address_);
}

void ControlConnection::clear() {
  state_ = CONTROL_STATE_NEW;
  session_ = NULL;
  connection_ = NULL;
  reconnect_timer_ = NULL;
  query_plan_.reset();
  protocol_version_ = 0;
  last_connection_error_.clear();
  query_tokens_ = false;
}

void ControlConnection::connect(Session* session) {
  session_ = session;
  query_plan_.reset(new ControlStartupQueryPlan(session_->hosts_));
  protocol_version_ = session->config_.protocol_version();
  query_tokens_ = session_->config().token_aware_routing();
  if (protocol_version_ < 0) {
    protocol_version_ = HIGHEST_SUPPORTED_PROTOCOL_VERSION;
  }
  reconnect(false);
}

void ControlConnection::close() {
  state_ = CONTROL_STATE_CLOSED;
  if (connection_ != NULL) {
    connection_->close();
  }
  if (reconnect_timer_ != NULL) {
    Timer::stop(reconnect_timer_);
    reconnect_timer_ = NULL;
  }
}

void ControlConnection::schedule_reconnect(uint64_t ms) {
  reconnect_timer_= Timer::start(session_->loop(),
                                 ms,
                                 NULL,
                                 boost::bind(&ControlConnection::on_reconnect, this, _1));
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
                               session_->config_,
                               current_host_address_,
                               "", // No keyspace
                               protocol_version_);

  connection_->set_ready_callback(
        boost::bind(&ControlConnection::on_connection_ready, this, _1));
  connection_->set_close_callback(
        boost::bind(&ControlConnection::on_connection_closed, this, _1));
  connection_->set_event_callback(
        CASS_EVENT_TOPOLOGY_CHANGE | CASS_EVENT_STATUS_CHANGE | CASS_EVENT_SCHEMA_CHANGE,
        boost::bind(&ControlConnection::on_connection_event, this, _1));
  connection_->connect();
}

void ControlConnection::on_connection_ready(Connection* connection) {
  LOG_DEBUG("Connection ready on host %s",
            connection->address().to_string().c_str());

  // A protocol version is need to encode/decode maps properly
  session_->cluster_meta().set_protocol_version(protocol_version_);

  // The control connection has to refresh meta when there's a reconnect because
  // events could have been missed while not connected.
  query_meta_all();
}

void ControlConnection::on_connection_closed(Connection* connection) {
  bool retry_current_host = false;

  if (state_ != CONTROL_STATE_CLOSED) {
    LOG_WARN("Lost connection on host %s", connection->address_string().c_str());
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
      protocol_version_--;
      retry_current_host = true;
    } else if (!connection->auth_error().empty()) {
      session_->on_control_connection_error(CASS_ERROR_SERVER_BAD_CREDENTIALS,
                                            connection->auth_error());
      return;
    } else if (!connection->ssl_error().empty()) {
      session_->on_control_connection_error(connection->ssl_error_code(),
                                            connection->ssl_error());
      return;
    }
  }

  reconnect(retry_current_host);
}

//TODO: query and callbacks should be in ClusterMetadata
// punting for now because of tight coupling of Session and CC state
void ControlConnection::query_meta_all() {
  ScopedRefPtr<ControlMultipleRequestHandler> handler(
        new ControlMultipleRequestHandler(this, boost::bind(&ControlConnection::on_query_meta_all, this, _1)));
  handler->execute_query(SELECT_LOCAL_TOKENS);
  handler->execute_query(SELECT_PEERS_TOKENS);
  handler->execute_query(SELECT_KEYSPACES);
  handler->execute_query(SELECT_COLUMN_FAMILIES);
  handler->execute_query(SELECT_COLUMNS);
}

void ControlConnection::on_query_meta_all(const MultipleRequestHandler::ResponseVec& responses) {
  if (connection_ == NULL) {
    return;
  }

  session_->cluster_meta().clear();

  bool is_initial_connection = (state_ == CONTROL_STATE_NEW);

  {
    SharedRefPtr<Host> host = session_->get_host(connection_->address(), true);
    if (host) {
      ResultResponse* local_result =
          static_cast<ResultResponse*>(responses[0]);

      if (local_result->row_count() > 0) {
        local_result->decode_first_row();
        update_node_info(host, &local_result->first_row());
      } else {
        LOG_WARN("No row found in %s's local system table",
                 connection_->address_string().c_str());
      }
    } else {
      LOG_DEBUG("Host %s from local system table not found",
                connection_->address_string().c_str());
    }
  }

  {
    ResultResponse* peers_result =
        static_cast<ResultResponse*>(responses[1]);
    peers_result->decode_first_row();
    ResultIterator rows(peers_result);
    while (rows.next()) {
      Address address;
      const Row* row = rows.row();
      if (!determine_address_for_peer_host(connection_->address(),
                                           row->get_by_name("peer"),
                                           row->get_by_name("rpc_address"),
                                           &address)) {
        continue;
      }

      SharedRefPtr<Host> host = session_->get_host(address, true);
      bool is_new = false;
      if (!host) {
        is_new = true;
        host = session_->add_host(address, true);
      }
      update_node_info(host, rows.row());
      if (is_new && !is_initial_connection) {
        session_->on_add(host, false);
      }
    }
  }

  session_->purge_hosts(is_initial_connection);

  session_->cluster_meta().update_keyspaces(static_cast<ResultResponse*>(responses[2]));
  session_->cluster_meta().update_tables(static_cast<ResultResponse*>(responses[3]),
                                         static_cast<ResultResponse*>(responses[4]));
  session_->cluster_meta().build();

  if (is_initial_connection) {
    state_ = CONTROL_STATE_READY;
    session_->on_control_connection_ready();
    // Create a new query plan that considers all the new hosts from the
    // "system" tables.
    query_plan_.reset(session_->new_query_plan());
  }
}

void ControlConnection::refresh_node_info(SharedRefPtr<Host> host,
                                          RefreshNodeCallback callback,
                                          bool query_tokens) {
  if (connection_ == NULL) {
    return;
  }

  bool is_connected_host = host->address().compare(connection_->address()) == 0;

  std::string query;
  ControlHandler<RefreshNodeData>::ResponseCallback response_callback;

  bool token_query = query_tokens_ && (host->was_just_added() || query_tokens);
  if (is_connected_host || !host->listen_address().empty()) {
    if (is_connected_host) {
      query.assign(token_query ? SELECT_LOCAL_TOKENS : SELECT_LOCAL);
    } else {
      query.assign(token_query ? SELECT_PEERS_TOKENS : SELECT_PEERS);
      query.append(" WHERE peer = '");
      query.append(host->listen_address());
      query.append("'");
    }
    response_callback = boost::bind(&ControlConnection::on_refresh_node_info, this, _1, _2);
  } else {
    query.assign(token_query ? SELECT_PEERS_TOKENS : SELECT_PEERS);
    response_callback = boost::bind(&ControlConnection::on_refresh_node_info_all, this, _1, _2);
  }

  LOG_DEBUG("refresh_node_info: %s", query.c_str());

  RefreshNodeData data(host, callback);
  ScopedRefPtr<ControlHandler<RefreshNodeData> > handler(
        new ControlHandler<RefreshNodeData>(new QueryRequest(query),
                                            this,
                                            response_callback,
                                            data));
  if (!connection_->write(handler.get())) {
    LOG_ERROR("No more stream available while attempting to refresh node info");
  }
}

void ControlConnection::on_refresh_node_info(RefreshNodeData data, Response* response) {
  if (connection_ == NULL) {
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response);

  if (result->row_count() == 0) {
    std::string host_address_str = data.host->address().to_string();
    LOG_ERROR("No row found for host %s in %s's local/peers system table. "
              "%s will be ignored.",
              host_address_str.c_str(),
              connection_->address_string().c_str(),
              host_address_str.c_str());
    return;
  }
  result->decode_first_row();
  update_node_info(data.host, &result->first_row());
  if (data.callback != NULL) {
    data.callback(data.host);
  }
}

void ControlConnection::on_refresh_node_info_all(RefreshNodeData data, Response* response) {
  if (connection_ == NULL) {
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response);

  if (result->row_count() == 0) {
    std::string host_address_str = data.host->address().to_string();
    LOG_ERROR("No row found for host %s in %s's peers system table. "
              "%s will be ignored.",
              host_address_str.c_str(),
              connection_->address_string().c_str(),
              host_address_str.c_str());
    return;
  }

  result->decode_first_row();
  ResultIterator rows(result);
  while (rows.next()) {
    const Row* row = rows.row();
    Address address;
    bool is_valid_address
        = determine_address_for_peer_host(connection_->address(),
                                          row->get_by_name("peer"),
                                          row->get_by_name("rpc_address"),
                                          &address);
    if (is_valid_address && data.host->address().compare(address) == 0) {
      update_node_info(data.host, row);
      if (data.callback != NULL) {
        data.callback(data.host);
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

  // This value is not present in the "system.local" query
  v = row->get_by_name("peer");
  if (v != NULL) {
    Address listen_address;
    Address::from_inet(v->buffer().data(), v->buffer().size(),
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

  if (query_tokens_) {
    std::string partitioner;
    if (row->get_string_by_name("partitioner", &partitioner)) {
      session_->cluster_meta().set_partitioner(partitioner);
    }
    v = row->get_by_name("tokens");
    if (v != NULL) {
      CollectionIterator i(v);
      TokenStringList tokens;
      while (i.next()) {
        const BufferPiece& bp = i.value()->buffer();
        tokens.push_back(boost::string_ref(bp.data(), bp.size()));
      }
      if (!tokens.empty()) {
        session_->cluster_meta().update_host(host, tokens);
      }
    }
  }
}

void ControlConnection::refresh_keyspace(const boost::string_ref& keyspace_name) {
  std::string query(SELECT_KEYSPACES);
  query.append(" WHERE keyspace_name='")
       .append(keyspace_name.data(), keyspace_name.size())
       .append("'");

  LOG_DEBUG("Refreshing keyspace %s", query.c_str());

  connection_->write(
        new ControlHandler<std::string>(new QueryRequest(query),
                                            this,
                                            boost::bind(&ControlConnection::on_refresh_keyspace, this, _1, _2),
                                            keyspace_name.to_string()));
}

void ControlConnection::on_refresh_keyspace(const std::string& keyspace_name, Response* response) {
  ResultResponse* result = static_cast<ResultResponse*>(response);
  if (result->row_count() == 0) {
    LOG_ERROR("No row found for keyspace %s in system schema table.",
              keyspace_name.c_str());
    return;
  }
  session_->cluster_meta().update_keyspaces(result);
}

void ControlConnection::refresh_table(const boost::string_ref& keyspace_name,
                                      const boost::string_ref& table_name) {
  std::string cf_query(SELECT_COLUMN_FAMILIES);
  cf_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
          .append("' AND columnfamily_name='").append(table_name.data(), table_name.size()).append("'");

  std::string col_query(SELECT_COLUMNS);
  col_query.append(" WHERE keyspace_name='").append(keyspace_name.data(), keyspace_name.size())
           .append("' AND columnfamily_name='").append(table_name.data(), table_name.size()).append("'");

  LOG_DEBUG("Refreshing table %s; %s", cf_query.c_str(), col_query.c_str());

  ScopedRefPtr<ControlMultipleRequestHandler> handler(
        new ControlMultipleRequestHandler(this,
                                          boost::bind(&ControlConnection::on_refresh_table,
                                                      this, keyspace_name.to_string(),
                                                      table_name.to_string(), _1)));
  handler->execute_query(cf_query);
  handler->execute_query(col_query);
}

void ControlConnection::on_refresh_table(const std::string& keyspace_name,
                                         const std::string& table_name,
                                         const MultipleRequestHandler::ResponseVec& responses) {
  ResultResponse* column_family_result = static_cast<ResultResponse*>(responses[0]);
  if (column_family_result->row_count() == 0) {
    LOG_ERROR("No row found for column family %s.%s in system schema table.",
              keyspace_name.c_str(), table_name.c_str());
    return;
  }
  session_->cluster_meta().update_tables(column_family_result,
                                         static_cast<ResultResponse*>(responses[1]));
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

void ControlConnection::on_connection_event(EventResponse* response) {
  switch (response->event_type()) {
    case CASS_EVENT_TOPOLOGY_CHANGE: {
      std::string address_str = response->affected_node().to_string();
      switch (response->topology_change()) {
        case EventResponse::NEW_NODE: {
          LOG_INFO("New node %s added", address_str.c_str());
          SharedRefPtr<Host> host = session_->get_host(response->affected_node());
          if (!host) {
            host = session_->add_host(response->affected_node());
            refresh_node_info(host, boost::bind(&Session::on_add, session_, _1, false), true);
          }
          break;
        }

        case EventResponse::REMOVED_NODE: {
          LOG_INFO("Node %s removed", address_str.c_str());
          SharedRefPtr<Host> host = session_->get_host(response->affected_node());
          if (host) {
            session_->on_remove(host);
            session_->cluster_meta().remove_host(host);
          } else {
            LOG_DEBUG("Tried to remove host %s that doesn't exist", address_str.c_str());
          }
          break;
        }

        case EventResponse::MOVED_NODE:
          LOG_INFO("Node %s moved", address_str.c_str());
          SharedRefPtr<Host> host = session_->get_host(response->affected_node());
          if (host) {
            refresh_node_info(host, NULL, true);
          } else {
            LOG_DEBUG("Move event for host %s that doesn't exist", address_str.c_str());
            session_->cluster_meta().remove_host(host);
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
                (int)response->table().size(), response->table().data());
      switch (response->schema_change()) {
        case EventResponse::CREATED:
        case EventResponse::UPDATED:
          if (response->table().size() > 0) {
            refresh_table(response->keyspace(), response->table());
          } else {
            refresh_keyspace(response->keyspace());
          }
          break;

        case EventResponse::DROPPED:
          if (response->table().size() > 0) {
            session_->cluster_meta().drop_table(response->keyspace().to_string(),
                                                response->table().to_string());
          } else {
            session_->cluster_meta().drop_keyspace(response->keyspace().to_string());
          }
          break;
      }
      break;

    default:
      assert(false);
      break;
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
    refresh_node_info(host, RefreshNodeCallback());
  } else {
    host = session_->add_host(address);
    refresh_node_info(host, boost::bind(&Session::on_add, session_, _1, false));
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
  query_plan_.reset(session_->new_query_plan());
  reconnect(false);
  reconnect_timer_ = NULL;
}

void ControlConnection::ControlMultipleRequestHandler::on_set(
    const MultipleRequestHandler::ResponseVec& responses) {
  bool has_error = false;
  for (MultipleRequestHandler::ResponseVec::const_iterator it = responses.begin(),
       end = responses.end(); it != end; ++it) {
    if (control_connection_->handle_query_invalid_response(*it)) {
      has_error = true;
    }
  }
  if (has_error) return;
  response_callback_(responses);
}

Address ControlConnection::bind_any_ipv4_("0.0.0.0", 0);
Address ControlConnection::bind_any_ipv6_("::", 0);

} // namespace cass
