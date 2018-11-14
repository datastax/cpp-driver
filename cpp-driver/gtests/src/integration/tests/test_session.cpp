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
  SessionTest()
    : added_(0)
    , removed_(0)
    , up_(0)
    , down_(0) { }

  void SetUp() {
    is_session_requested_ = false;
    Integration::SetUp();
  }

  const std::string& address() const { return address_; }
  int added() const { return added_; }
  int removed() const { return removed_; }
  int up() const { return up_; }
  int down() const { return down_; }

protected:
  static void on_host_listener(CassHostListenerEvent event,
                               CassInet inet,
                               void* data) {
    SessionTest* instance = static_cast<SessionTest*>(data);

    char address[CASS_INET_STRING_LENGTH];
    uv_inet_ntop(inet.address_length == CASS_INET_V4_LENGTH ? AF_INET : AF_INET6,
                 inet.address,
                 address, CASS_INET_STRING_LENGTH);
    instance->address_ = address;

    if (event == CASS_HOST_LISTENER_EVENT_ADD) {
      TEST_LOG("Added node " << address);
      ++instance->added_;
    } else if (event == CASS_HOST_LISTENER_EVENT_REMOVE) {
      TEST_LOG("Removed node " << address);
      ++instance->removed_;
    } else if (event == CASS_HOST_LISTENER_EVENT_UP) {
      TEST_LOG("Up node " << address);
      ++instance->up_;
    } else if (event == CASS_HOST_LISTENER_EVENT_DOWN) {
      TEST_LOG("Down node " << address);
      ++instance->down_;
    }
  }

private:
  std::string address_;
  int added_;
  int removed_;
  int up_;
  int down_;
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

  // Sanity check
  EXPECT_EQ(0, added());
  EXPECT_EQ(0, removed());
  EXPECT_EQ(0, up());
  EXPECT_EQ(0, down());

  logger_.add_critera("New node " + (ccm_->get_ip_prefix() + "2") + " added");
  EXPECT_EQ(2, ccm_->bootstrap_node());
  EXPECT_TRUE(wait_for_logger(1));
  EXPECT_EQ(1, added());
  EXPECT_STREQ((ccm_->get_ip_prefix() + "2").c_str(), address().c_str());

  stop_node(1);
  EXPECT_EQ(1, down());
  EXPECT_STREQ((ccm_->get_ip_prefix() + "1").c_str(), address().c_str());

  ccm_->start_node(1);
  EXPECT_EQ(1, up());
  EXPECT_STREQ((ccm_->get_ip_prefix() + "1").c_str(), address().c_str());

  force_decommission_node(1);
  EXPECT_EQ(1, removed());
  EXPECT_STREQ((ccm_->get_ip_prefix() + "1").c_str(), address().c_str());

  // Sanity check
  EXPECT_EQ(1, added());
  EXPECT_EQ(1, removed());
  EXPECT_EQ(1, up());
  EXPECT_EQ(1, down());
}
