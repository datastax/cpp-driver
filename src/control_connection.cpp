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
    if (state_ == CONTROL_STATE_CONNECTED) {
      schedule_reconnect(1000); // TODO: Configurable?
    } else if (error_callback_) {
      error_callback_(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, "No hosts available");
    }
    return;
  }

  connection_ = new Connection(session_->loop(),
                               host,
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
  // TODO:(mpenick): Query system.local and system.peers
  state_ = CONTROL_STATE_CONNECTED;
  if (ready_callback_) {
    ready_callback_(this);
  }
}

void ControlConnection::on_connection_closed(Connection* connection) {
  reconnect();
}

void ControlConnection::on_reconnect(Timer* timer) {
  schedule_reconnect();
  reconnect_timer_ = NULL;
}

} // namespace cass
