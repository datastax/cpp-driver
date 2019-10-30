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

class HeartbeatTests : public Integration {
public:
  HeartbeatTests() {
    is_session_requested_ = false;
    number_dc1_nodes_ = 2;
  }
};

/**
 * Heartbeat interval (enabled)
 *
 * This test ensures the heartbeat interval is enabled when connected to a cluster
 *
 * @since 2.1.0
 * @jira_ticket CPP-152
 * @expected_result Heartbeat is enabled.
 */
CASSANDRA_INTEGRATION_TEST_F(HeartbeatTests, HeartbeatEnabled) {
  CHECK_FAILURE;

  logger_.add_critera("Heartbeat completed on host " + ccm_->get_ip_prefix());
  Cluster cluster = default_cluster().with_connection_heartbeat_interval(1); // Quick heartbeat
  connect(cluster);

  start_timer();
  while (elapsed_time() < 2000) {
    session_.execute(SELECT_ALL_SYSTEM_LOCAL_CQL);
  }
  EXPECT_GE(logger_.count(), 1u);
}

/**
 * Heartbeat interval (disabled)
 *
 * This test ensures the heartbeat interval is disabled when connected to a cluster
 *
 * @since 2.1.0
 * @jira_ticket CPP-152
 * @expected_result Heartbeat is disabled.
 */
CASSANDRA_INTEGRATION_TEST_F(HeartbeatTests, HeartbeatDisabled) {
  CHECK_FAILURE;

  logger_.add_critera("Heartbeat completed on host " + ccm_->get_ip_prefix());
  Cluster cluster = default_cluster().with_connection_heartbeat_interval(0);
  connect(cluster);

  start_timer();
  while (elapsed_time() < 2000) {
    session_.execute(SELECT_ALL_SYSTEM_LOCAL_CQL);
  }
  EXPECT_EQ(0u, logger_.count());
}

/**
 * Heartbeat interval (failed)
 *
 * This test ensures the heartbeat interval is enabled when connected to a cluster and fails to get
 * a response from a node resulting in connection termination.
 *
 * @since 2.1.0
 * @jira_ticket CPP-152
 * @expected_result Heartbeat failure on node 2; node 2 connection is lost.
 */
CASSANDRA_INTEGRATION_TEST_F(HeartbeatTests, HeartbeatFailed) {
  CHECK_FAILURE;

  logger_.add_critera("Failed to send a heartbeat within connection idle interval.");
  Cluster cluster =
      default_cluster().with_connection_heartbeat_interval(1).with_connection_idle_timeout(5);
  connect(cluster);

  cass_uint64_t initial_connections = session_.metrics().stats.total_connections;
  pause_node(2);
  start_timer();
  while (session_.metrics().stats.total_connections >= initial_connections &&
         elapsed_time() < 60000) {
    session_.execute_async(SELECT_ALL_SYSTEM_LOCAL_CQL); // Simply execute statements ignore any
                                                         // error that can occur from paused node
  }
  EXPECT_LT(session_.metrics().stats.total_connections, initial_connections);
  EXPECT_GE(logger_.count(), 1u);
}
