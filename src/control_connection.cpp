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

#include "query_request.hpp"
#include "result_iterator.hpp"
#include "error_response.hpp"
#include "result_response.hpp"
#include "session.hpp"
#include "timer.hpp"

#include "third_party/boost/boost/bind.hpp"

#include <sstream>
#include <iomanip>

namespace cass {

void ControlConnection::connect() {
  logger_ = session_->logger();
  schedule_reconnect();
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
  if (ms == 0) {
    query_plan_.reset(session_->load_balancing_policy()->new_query_plan());
    reconnect();
  } else {
    reconnect_timer_= Timer::start(session_->loop(),
                                   ms,
                                   NULL,
                                   boost::bind(&ControlConnection::on_reconnect, this, _1));
  }
}

void ControlConnection::reconnect() {
  if (state_ == CONTROL_STATE_CLOSED) {
    return;
  }

  Host host;

  if (!query_plan_->compute_next(&host)) {
    if (state_ == CONTROL_STATE_READY) {
      schedule_reconnect(1000); // TODO(mpenick): Configurable?
    } else if (error_callback_) {
      error_callback_(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, "No hosts available");
    }
    return;
  }

  if (connection_ != NULL) {
    connection_->close();
  }

  connection_ = new Connection(session_->loop(),
                               host.address,
                               session_->logger(),
                               session_->config(),
                               "",
                               session_->config().protocol_version());

  connection_->set_ready_callback(
        boost::bind(&ControlConnection::on_connection_ready, this, _1));
  connection_->set_close_callback(
        boost::bind(&ControlConnection::on_connection_closed, this, _1));
  connection_->connect();
}

void ControlConnection::on_connection_ready(Connection* connection) {
  logger_->debug("ControlConnection: connection ready on %s", connection->address_string().c_str());
  if (state_== CONTROL_STATE_NEW) {
    logger_->debug("ControlConnection: New connection -- initiating node discovery");
    state_ = CONTROL_STATE_NODE_REFRESH_1;
    QueryRequest* local_request = new QueryRequest();
    // NOTE: queried values affect indexing in on_local_query
    local_request->set_query("SELECT data_center, rack FROM system.local WHERE key='local'");
    QueryRequest* peer_request = new QueryRequest();
    // NOTE: queried valeus affect indexing in on_peer_query
    peer_request->set_query("SELECT peer, data_center, rack, rpc_address FROM system.peers");

    connection_->execute(new ControlHandler(this, local_request,
                                            boost::bind(&ControlConnection::on_local_query, this, _1)));
    connection_->execute(new ControlHandler(this, peer_request,
                                            boost::bind(&ControlConnection::on_peer_query, this, _1)));
  }
}

void ControlConnection::on_local_query(ResponseMessage *response) {
  if (response->opcode() != CQL_OPCODE_RESULT) {
    fail_startup_connect(response);
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());
  result->decode_first_row();
  const Value* v = &result->first_row().values[0];
  std::string dc(v->buffer().data(), v->buffer().size());
  v = &result->first_row().values[1];
  std::string rack(v->buffer().data(), v->buffer().size());
  session_->add_host(Host(connection_->address(), rack, dc));
  maybe_notify_ready();
}

void ControlConnection::on_peer_query(ResponseMessage *response) {
  if (response->opcode() != CQL_OPCODE_RESULT) {
    fail_startup_connect(response);
    return;
  }

  ResultResponse* result =
      static_cast<ResultResponse*>(response->response_body().get());
  result->decode_first_row();
  ResultIterator rows(result);
  while (rows.next()) {
    Address addr;
    if (resolve_peer(rows.row()->values[0], rows.row()->values[3], &addr)) {
      const Value* v = &rows.row()->values[1];
      std::string dc(v->buffer().data(), v->buffer().size());
      v = &rows.row()->values[2];
      std::string rack(v->buffer().data(), v->buffer().size());
      session_->add_host(Host(addr, rack, dc));
    }
  }
  maybe_notify_ready();
}

bool ControlConnection::resolve_peer(const Value& peer_value, const Value& rpc_value, Address* output) {
  Address::from_inet(peer_value.buffer().data(), peer_value.buffer().size(),
                     connection_->address().port(), output);
  const Address& connected_address = connection_->address();
  if (connected_address.compare(*output) == 0) {
    logger_->debug("system.peers on %s contains a line with peer address for itself. "
                   "This is not normal but is a known problem for some versions of DSE. "
                   "Ignoring this entry.", connected_address.to_string(false).c_str());
    return false;
  }
  if (rpc_value.buffer().size() > 0) {
    Address rpc_address;
    Address::from_inet(rpc_value.buffer().data(), rpc_value.buffer().size(),
                       connection_->address().port(), &rpc_address);
    if (connected_address.compare(rpc_address) == 0) {
      logger_->debug("system.peers on %s contains a line with rpc_address for itself. "
                     "This is not normal, but is a known problem for some versions of DSE. "
                     "Ignoring this entry.", connected_address.to_string(false).c_str());
      return false;
    }
    if (rpc_address.compare(bind_any_ipv4_) != 0 &&
        rpc_address.compare(bind_any_ipv6_) != 0) {
      *output = rpc_address;
    } else {
      logger_->warn("Found host with 'bind any' for rpc_address; using listen_address (%s) to contact instead. "
                    "If this is incorrect you should use a specific interface for rpc_address on the server.",
                    output->to_string(false).c_str());
    }
  } else {
    logger_->warn("No rpc_address for host %s in system.peers on %s. "
                   "Using listen_address instead.", output->to_string(false).c_str(),
                   connected_address.to_string(false).c_str());
  }
  return true;
}

void ControlConnection::maybe_notify_ready() {
  if (state_ == CONTROL_STATE_NODE_REFRESH_1) {
    state_ = CONTROL_STATE_NODE_REFRESH_2;
  } else if (state_ == CONTROL_STATE_NODE_REFRESH_2) {
    state_ = CONTROL_STATE_READY;
    if (ready_callback_) {
      ready_callback_(this);
    }
  }
}

void ControlConnection::fail_startup_connect(ResponseMessage* response) {
  std::ostringstream ss;
  if (response->opcode() == CQL_OPCODE_ERROR) {
    ErrorResponse* error
        = static_cast<ErrorResponse*>(response->response_body().get());
    ss << "Error while querying system tables: '" << error->message()
       << "' (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0')
       << CASS_ERROR(CASS_ERROR_SOURCE_SERVER, error->code()) << ")";
    logger_->error(ss.str().c_str());
  } else {
    ss << "Unexpected opcode while querying system tables: " << opcode_to_string(response->opcode());
    logger_->error(ss.str().c_str());
  }
  state_ = CONTROL_STATE_NEW;
  connection_->defunct();
}

void ControlConnection::on_connection_closed(Connection* connection) {
  // This pointer to the connection is no longer valid once it's
  // closed
  connection_ = NULL;

  if (state_ == CONTROL_STATE_NEW) {
    if (!connection->auth_error().empty()) {
      if (error_callback_) {
        error_callback_(CASS_ERROR_SERVER_BAD_CREDENTIALS, connection->auth_error());
        return;
      }
    }
  }

  reconnect();
}

void ControlConnection::on_reconnect(Timer* timer) {
  schedule_reconnect();
  reconnect_timer_ = NULL;
}

void ControlConnection::ControlHandler::on_set(ResponseMessage* response) {
  response_callback_(response);
}

void ControlConnection::ControlHandler::on_error(CassError code, const std::string& message) {
  // TODO(mpenick): This is a placeholder and might not be the right action for
  // all error scenarios
  control_connection_->schedule_reconnect();
}

void ControlConnection::ControlHandler::on_timeout() {
  // TODO(mpenick): Is this the best way to handle a timeout?
  control_connection_->schedule_reconnect();
}

Address ControlConnection::bind_any_ipv4_("0.0.0.0", 0);
Address ControlConnection::bind_any_ipv6_("::", 0);

} // namespace cass
