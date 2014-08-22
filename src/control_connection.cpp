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
#include "event_repsonse.hpp"
#include "session.hpp"
#include "timer.hpp"

#include "third_party/boost/boost/bind.hpp"

namespace cass {

void ControlConnection::connect() {
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
    query_plan_.reset(session_->load_balancing_policy_->new_query_plan());
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
    if (state_ == CONTROL_STATE_CONNECTED) {
      schedule_reconnect(1000); // TODO(mpenick): Configurable?
    } else {
      session_->on_control_conneciton_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, "No hosts available");
    }
    return;
  }

  if (connection_ != NULL) {
    connection_->close();
  }

  connection_ = new Connection(session_->loop(),
                               host,
                               session_->logger_.get(),
                               session_->config_,
                               "",
                               session_->config_.protocol_version());

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
  // TODO:(mpenick): Query system.local and system.peers (CPP-145)

  if (state_== CONTROL_STATE_NEW) {
    state_ = CONTROL_STATE_CONNECTED;
    session_->on_control_connection_ready();
  }
}

void ControlConnection::on_connection_closed(Connection* connection) {
  // This pointer to the connection is no longer valid once it's
  // closed
  connection_ = NULL;

  // TODO(mpenick): Handle protocol version error

  reconnect();
}

void ControlConnection::on_connection_event(EventResponse* response) {

  std::string address_str = response->affected_node().to_string();

  switch (response->event_type()) {
    case CASS_EVENT_TOPOLOGY_CHANGE:
      switch (response->topology_change()) {
        case EventResponse::NEW_NODE:
          session_->logger_->info("ControlConnection: New node '%s' added", address_str.c_str());
          break;

        case EventResponse::REMOVE_NODE:
          session_->logger_->info("ControlConnection: Node '%s' removed", address_str.c_str());
          break;

        case EventResponse::MOVED_NODE:
          session_->logger_->info("ControlConnection: Node'%s' moved", address_str.c_str());
          break;
      }
      break;

    case CASS_EVENT_STATUS_CHANGE:
      switch (response->status_change()) {
        case EventResponse::UP:
          session_->logger_->info("ControlConnection: Node'%s' is up", address_str.c_str());
          break;

        case EventResponse::DOWN:
          session_->logger_->info("ControlConnection: Node'%s' is down", address_str.c_str());
          break;
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

} // namespace cass
