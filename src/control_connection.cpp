/*
  Copyright 2014 DataStax

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

#include "constants.hpp"
#include "event_response.hpp"
#include "load_balancing.hpp"
#include "logger.hpp"
#include "query_request.hpp"
#include "result_iterator.hpp"
#include "error_response.hpp"
#include "result_response.hpp"
#include "session.hpp"
#include "timer.hpp"

#include "third_party/boost/boost/bind.hpp"

#include <iomanip>
#include <sstream>
#include <vector>

#define HIGHEST_SUPPORTED_PROTOCOL_VERSION 2

#define SELECT_LOCAL "SELECT data_center, rack FROM system.local WHERE key='local'"
#define SELECT_PEERS "SELECT peer, data_center, rack, rpc_address FROM system.peers"

namespace cass {

class ControlStartupQueryPlan : public QueryPlan {
public:
  ControlStartupQueryPlan(const HostMap& hosts) {
    for (HostMap::const_iterator it = hosts.begin(),
         end = hosts.end(); it != end; ++it) {
      host_addresses_.push_back(it->second->address());
    }
    it_ = host_addresses_.begin();
  }

  virtual bool compute_next(Address* address) {
    if (it_ == host_addresses_.end()) return false;
    *address = *it_;
    it_++;
    return true;
  }

private:
  AddressVec host_addresses_;
  AddressVec::iterator it_;
};

bool ControlConnection::determine_address_for_peer_host(Logger* logger,
                                                        const Address& connected_address,
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
      logger->debug("system.peers on %s contains a line with rpc_address for itself. "
                    "This is not normal, but is a known problem for some versions of DSE. "
                    "Ignoring this entry.", connected_address.to_string(false).c_str());
      return false;
    }
    if (bind_any_ipv4_.compare(*output) == 0 ||
        bind_any_ipv6_.compare(*output) == 0) {
      logger->warn("Found host with 'bind any' for rpc_address; using listen_address (%s) to contact instead. "
                   "If this is incorrect you should configure a specific interface for rpc_address on the server.",
                   peer_address.to_string(false).c_str());
      *output = peer_address;
    }
  } else {
    logger->warn("No rpc_address for host %s in system.peers on %s. "
                 "Ignoring this entry.", peer_address.to_string(false).c_str(),
                  connected_address.to_string(false).c_str());
    return false;
  }
  return true;
}

void ControlConnection::connect(Session* session) {
  session_ = session;
  logger_ = session_->logger_.get();
  query_plan_.reset(new ControlStartupQueryPlan(session_->hosts_));
  protocol_version_ = session->config_.protocol_version();
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

void ControlConnection::flush() {
  if (connection_ != NULL) connection_->flush();
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
        session_->on_control_connection_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, "No hosts available");
      }
      return;
    }
  }

  if (connection_ != NULL) {
    connection_->close();
  }

  connection_ = new Connection(session_->loop(),
                               session_->logger_.get(),
                               session_->config_,
                               current_host_address_,
                               "",
                               protocol_version_);

  connection_->set_ready_callback(
        boost::bind(&ControlConnection::on_connection_ready, this, _1));
  connection_->set_close_callback(
        boost::bind(&ControlConnection::on_connection_closed, this, _1));
  connection_->set_event_callback(
        CASS_EVENT_TOPOLOGY_CHANGE | CASS_EVENT_STATUS_CHANGE,
        boost::bind(&ControlConnection::on_connection_event, this, _1));
  connection_->connect();
}

void ControlConnection::on_connection_ready(Connection* connection) {
  logger_->debug("ControlConnection: Connection ready on host %s",
                 connection->address().to_string().c_str());

  // The control connection has to refresh the node list anytime
  // there's a reconnect because several events could have been missed while not connected.
  refresh_node_list();
}

void ControlConnection::on_connection_closed(Connection* connection) {
  bool retry_current_host = false;

  // This pointer to the connection is no longer valid once it's closed
  connection_ = NULL;

  if (state_ == CONTROL_STATE_NEW) {
    if (connection->is_invalid_protocol()) {
      if (protocol_version_ <= 1) {
        logger_->error("ControlConnection: Host %s does not support any valid protocol version",
                       connection->address_string().c_str());
        session_-> on_control_connection_error(CASS_ERROR_UNABLE_TO_DETERMINE_PROTOCOL,
                                              "Not even protocol version 1 is supported");
        return;
      }
      protocol_version_--;
      retry_current_host = true;
    } else if(!connection->auth_error().empty()) {
      session_-> on_control_connection_error(CASS_ERROR_SERVER_BAD_CREDENTIALS,
                                            connection->auth_error());
      return;
    }
  }

  reconnect(retry_current_host);
}

void ControlConnection::refresh_node_list() {
  ScopedRefPtr<ControlMultipleRequestHandler> handler(
        new ControlMultipleRequestHandler(this, boost::bind(&ControlConnection::on_node_refresh, this, _1)));
  handler->execute_query(SELECT_LOCAL);
  handler->execute_query(SELECT_PEERS);
}

void ControlConnection::on_node_refresh(const MultipleRequestHandler::ResponseVec& responses) {
  if (connection_ == NULL) {
    return;
  }

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
        logger_->debug("ControlConnection: No row found in %s's local system table",
                       connection_->address_string().c_str());
      }
    } else {
      logger_->debug("ControlConnection: Host %s from local system table not found",
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
      if (!determine_address_for_peer_host(logger_,
                                           connection_->address(),
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

  if (is_initial_connection) {
    state_ = CONTROL_STATE_READY;
    session_->on_control_connection_ready();
  }
}

void ControlConnection::refresh_node_info(SharedRefPtr<Host> host,
                                          RefreshNodeCallback callback) {
  if (connection_ == NULL) {
    return;
  }

  bool is_connected_host = host->address().compare(connection_->address()) == 0;

  std::string query;
  ControlHandler<RefreshNodeData>::ResponseCallback response_callback;

  if (is_connected_host || !host->listen_address().empty()) {
    if (is_connected_host) {
      query.assign(SELECT_LOCAL);
    } else {
      query.assign(SELECT_PEERS);
      query.append(" WHERE peer = '");
      query.append(host->listen_address());
      query.append("'");
    }
    response_callback = boost::bind(&ControlConnection::on_refresh_node_info, this, _1, _2);
  } else {
    query.assign(SELECT_PEERS);
    response_callback = boost::bind(&ControlConnection::on_refresh_node_info_all, this, _1, _2);
  }

  RefreshNodeData data(host, callback);
  connection_->write(
        new ControlHandler<RefreshNodeData>(new QueryRequest(query),
                                            this,
                                            response_callback,
                                            data));
}

void ControlConnection::on_refresh_node_info(RefreshNodeData data, Response* response) {
  if (connection_ == NULL) {
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response);

  if (result->row_count() == 0) {
    std::string host_address_str = data.host->address().to_string();
    logger_->error("ControlConnection: No row found for host %s in %s's local/peers system table. "
                   "%s will be ignored.",
                   host_address_str.c_str(),
                   connection_->address_string().c_str(),
                   host_address_str.c_str());
    return;
  }
  result->decode_first_row();
  update_node_info(data.host, &result->first_row());
  data.callback(data.host);
}

void ControlConnection::on_refresh_node_info_all(RefreshNodeData data, Response* response) {
  if (connection_ == NULL) {
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response);

  if (result->row_count() == 0) {
    std::string host_address_str = data.host->address().to_string();
    logger_->error("ControlConnection: No row found for host %s in %s's peers system table. "
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
        = determine_address_for_peer_host(logger_,
                                          connection_->address(),
                                          row->get_by_name("peer"),
                                          row->get_by_name("rpc_address"),
                                          &address);
    if (is_valid_address && data.host->address().compare(address) == 0) {
      update_node_info(data.host, row);
      data.callback(data.host);
      break;
    }
  }
}


void ControlConnection::update_node_info(SharedRefPtr<Host> host, const Row* row) {
  const Value* v;

  std::string rack;
  v = row->get_by_name("rack");
  if (!v->is_null()) {
    rack.assign(v->buffer().data(), v->buffer().size());
  }

  std::string dc;
  v = row->get_by_name("data_center");
  if (!v->is_null()) {
    dc.assign(v->buffer().data(), v->buffer().size());
  }

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
}

bool ControlConnection::handle_query_invalid_response(Response* response) {
  if (check_error_or_invalid_response("ControlConnection", CQL_OPCODE_RESULT,
                                      response, logger_)) {
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
  std::string address_str = response->affected_node().to_string();

  switch (response->event_type()) {
    case CASS_EVENT_TOPOLOGY_CHANGE:
      switch (response->topology_change()) {
        case EventResponse::NEW_NODE: {
          session_->logger_->info("ControlConnection: New node %s added", address_str.c_str());
          SharedRefPtr<Host> host = session_->get_host(response->affected_node());
          if (!host) {
            host = session_->add_host(response->affected_node());
            refresh_node_info(host, boost::bind(&Session::on_add, session_, _1, false));
          }
          break;
        }

        case EventResponse::REMOVED_NODE: {
          session_->logger_->info("ControlConnection: Node %s removed", address_str.c_str());
          SharedRefPtr<Host> host = session_->get_host(response->affected_node());
          if (host) {
            session_->on_remove(host);
          } else {
            logger_->debug("ControlConnection: Tried to remove host %s that doesn't exist", address_str.c_str());
          }
          break;
        }

        case EventResponse::MOVED_NODE:
          session_->logger_->info("ControlConnection: Node %s moved", address_str.c_str());
          break;
      }
      break;

    case CASS_EVENT_STATUS_CHANGE:
      switch (response->status_change()) {
        case EventResponse::UP: {
          session_->logger_->info("ControlConnection: Node %s is up", address_str.c_str());
          on_up(response->affected_node());
          break;
        }

        case EventResponse::DOWN: {
          session_->logger_->info("ControlConnection: Node %s is down", address_str.c_str());
          on_down(response->affected_node(), false);
          break;
        }
      }
      break;

    case CASS_EVENT_SCHEMA_CHANGE:
      switch (response->schema_change()) {
        case EventResponse::CREATED:
          break;

        case EventResponse::UPDATED:
          break;

        case EventResponse::DROPPED:
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
    refresh_node_info(host, boost::bind(&Session::on_up, session_, _1));
  } else {
    host = session_->add_host(address);
    refresh_node_info(host, boost::bind(&Session::on_add, session_, _1, false));
  }
}

void ControlConnection::on_down(const Address& address, bool is_critical_failure) {
  SharedRefPtr<Host> host = session_->get_host(address);
  if (host) {
    session_->on_down(host, is_critical_failure);
  } else {
    logger_->debug("ControlConnection: Tried to down host %s that doesn't exist", address.to_string().c_str());
  }
}

void ControlConnection::on_reconnect(Timer* timer) {
  query_plan_.reset(session_->load_balancing_policy_->new_query_plan());
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
