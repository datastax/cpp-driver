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

#include "integration.hpp"
#include "cassandra.h"

class SessionTest : public Integration {
public:
  typedef std::pair<CassHostListenerEvent, std::string> Event;
  typedef std::vector<Event> Events;

  void SetUp() {
    is_session_requested_ = false;
    Integration::SetUp();
  }

  size_t event_count() { return events_.size(); }
  const Events& events() const { return events_; }

protected:
  static void on_host_listener(CassHostListenerEvent event,
                               CassInet inet,
                               void* data) {
    SessionTest* instance = static_cast<SessionTest*>(data);

    char address[CASS_INET_STRING_LENGTH];
    uv_inet_ntop(inet.address_length == CASS_INET_V4_LENGTH ? AF_INET : AF_INET6,
                 inet.address,
                 address, CASS_INET_STRING_LENGTH);
    instance->events_.push_back(Event(event, address));
  }

private:
  Events events_;
};

CASSANDRA_INTEGRATION_TEST_F(SessionTest, MetricsWithoutConnecting) {
  CHECK_FAILURE;

  Session session;

  CassMetrics metrics;
  logger_.add_critera("Attempted to get metrics before connecting session object");
  cass_session_get_metrics(session.get(), &metrics);

  EXPECT_EQ(metrics.requests.min, 0u);
  EXPECT_EQ(metrics.requests.one_minute_rate, 0.0);
  EXPECT_EQ(logger_.count(), 1u);

  CassSpeculativeExecutionMetrics spec_ex_metrics;
  logger_.reset();
  logger_.add_critera("Attempted to get speculative execution metrics before connecting session object");
  cass_session_get_speculative_execution_metrics(session.get(), &spec_ex_metrics);
  EXPECT_EQ(spec_ex_metrics.min, 0u);
  EXPECT_EQ(spec_ex_metrics.percentage, 0.0);
  EXPECT_EQ(logger_.count(), 1u);
}

CASSANDRA_INTEGRATION_TEST_F(SessionTest, ExternalHostListener) {
  CHECK_FAILURE;
  is_test_chaotic_ = true; // Destroy the cluster after the test completes

  Cluster cluster = default_cluster()
    .with_load_balance_round_robin()
    .with_host_listener_callback(on_host_listener, this);
  Session session = cluster.connect();

  ASSERT_EQ(0u, event_count());

  EXPECT_EQ(2, ccm_->bootstrap_node());
  stop_node(1);
  ccm_->start_node(1);
  force_decommission_node(1);

  session.close();

  ASSERT_EQ(6u, event_count());
  EXPECT_EQ(CASS_HOST_LISTENER_EVENT_ADD, events()[0].first);
  EXPECT_EQ(ccm_->get_ip_prefix() + "2", events()[0].second);
  EXPECT_EQ(CASS_HOST_LISTENER_EVENT_UP, events()[1].first);
  EXPECT_EQ(ccm_->get_ip_prefix() + "2", events()[1].second);
  EXPECT_EQ(CASS_HOST_LISTENER_EVENT_DOWN, events()[2].first);
  EXPECT_EQ(ccm_->get_ip_prefix() + "1", events()[2].second);
  EXPECT_EQ(CASS_HOST_LISTENER_EVENT_UP, events()[3].first);
  EXPECT_EQ(ccm_->get_ip_prefix() + "1", events()[3].second);
  EXPECT_EQ(CASS_HOST_LISTENER_EVENT_DOWN, events()[4].first);
  EXPECT_EQ(ccm_->get_ip_prefix() + "1", events()[4].second);
  EXPECT_EQ(CASS_HOST_LISTENER_EVENT_REMOVE, events()[5].first);
  EXPECT_EQ(ccm_->get_ip_prefix() + "1", events()[5].second);
}
