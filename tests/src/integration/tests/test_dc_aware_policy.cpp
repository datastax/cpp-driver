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

#include "cassandra.h"
#include "integration.hpp"

#include <algorithm>
#include <iterator>

class DcAwarePolicyTest : public Integration {
public:
  void SetUp() {
    // Create a cluster with 2 DCs with 2 nodes in each
    number_dc1_nodes_ = 2;
    number_dc2_nodes_ = 2;
    is_session_requested_ = false;
    Integration::SetUp();
  }

  void initialize() {
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "text"));
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "1", "'one'"));
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "2", "'two'"));
  }

  std::vector<std::string> validate() {
    std::vector<std::string> attempted_hosts, temp;
    Result result;

    result = session_.execute(select_statement("1"));
    temp = result.attempted_hosts();
    std::copy(temp.begin(), temp.end(), std::back_inserter(attempted_hosts));
    EXPECT_EQ(result.first_row().next().as<Varchar>(), Varchar("one"));

    result = session_.execute(select_statement("2"));
    temp = result.attempted_hosts();
    std::copy(temp.begin(), temp.end(), std::back_inserter(attempted_hosts));
    EXPECT_EQ(result.first_row().next().as<Varchar>(), Varchar("two"));

    return attempted_hosts;
  }

  Statement select_statement(const std::string& key) {
    Statement statement(
        format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), key.c_str()));
    statement.set_consistency(CASS_CONSISTENCY_ONE);
    statement.set_record_attempted_hosts(true);
    return statement;
  }

  bool contains(const std::string& host, const std::vector<std::string>& attempted_hosts) {
    return std::count(attempted_hosts.begin(), attempted_hosts.end(), host) > 0;
  }
};

/**
 * Verify that the "used hosts per remote DC" setting allows queries to use the
 * remote DC nodes when the local DC nodes are unavailable.
 *
 * This ensures that the DC aware policy correctly uses remote hosts when
 * "used hosts per remote DC" has a value greater than 0.
 *
 * @since 2.8.1
 * @jira_ticket CPP-572
 * @test_category load_balancing_policy:dc_aware
 */
CASSANDRA_INTEGRATION_TEST_F(DcAwarePolicyTest, UsedHostsRemoteDc) {
  CHECK_FAILURE

  // Use up to one of the remote DC nodes if no local nodes are available.
  cluster_ = default_cluster();
  cluster_.with_load_balance_dc_aware("dc1", 1, false);
  connect(cluster_);

  // Create a test table and add test data to it
  initialize();

  { // Run queries using the local DC
    std::vector<std::string> attempted_hosts = validate();

    // Verify that local DC hosts were used
    EXPECT_TRUE(contains(ccm_->get_ip_prefix() + "1", attempted_hosts) ||
                contains(ccm_->get_ip_prefix() + "2", attempted_hosts));

    // Verify that no remote DC hosts were used
    EXPECT_TRUE(!contains(ccm_->get_ip_prefix() + "3", attempted_hosts) &&
                !contains(ccm_->get_ip_prefix() + "4", attempted_hosts));
  }

  // Stop the whole local DC
  stop_node(1, true);
  stop_node(2, true);

  { // Run queries using the remote DC
    std::vector<std::string> attempted_hosts = validate();

    // Verify that remote DC hosts were used
    EXPECT_TRUE(contains(ccm_->get_ip_prefix() + "3", attempted_hosts) ||
                contains(ccm_->get_ip_prefix() + "4", attempted_hosts));

    // Verify that no local DC hosts where used
    EXPECT_TRUE(!contains(ccm_->get_ip_prefix() + "1", attempted_hosts) &&
                !contains(ccm_->get_ip_prefix() + "2", attempted_hosts));
  }
}
