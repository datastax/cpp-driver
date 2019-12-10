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

#define SPECULATIVE_EXECUTION_SELECT_FORMAT "SELECT timeout(value) FROM %s WHERE key=%d"
#define SPECULATIVE_EXECUTION_CREATE_TIMEOUT_UDF_FORMAT   \
  "CREATE OR REPLACE FUNCTION timeout(arg int) "          \
  "RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java " \
  "AS $$ long start = System.currentTimeMillis(); "       \
  "while(System.currentTimeMillis() - start < arg) {"     \
  ";;"                                                    \
  "}"                                                     \
  "return arg;"                                           \
  "$$;"

class SpeculativeExecutionTests : public Integration {
public:
  SpeculativeExecutionTests() { number_dc1_nodes_ = 3; }

  void SetUp() {
    CHECK_VERSION(2.2.0);
    Integration::SetUp();

    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "0", "1000"));
    session_.execute(SPECULATIVE_EXECUTION_CREATE_TIMEOUT_UDF_FORMAT);
  }

  Statement create_select_statement(uint64_t request_timeout = 30000) {
    Statement statement =
        Statement(format_string(SPECULATIVE_EXECUTION_SELECT_FORMAT, table_name_.c_str(), 0));
    statement.set_idempotent(true);
    statement.set_request_timeout(request_timeout);
    statement.set_record_attempted_hosts(true);
    return statement;
  }

  void wait_for_count(const Session& session, cass_uint64_t expected_count) {
    CassSpeculativeExecutionMetrics metrics = session.speculative_execution_metrics();
    for (int i = 0; i < 600 && metrics.count != expected_count; ++i) {
      metrics = session.speculative_execution_metrics();
      msleep(100);
    }
    EXPECT_EQ(expected_count, metrics.count);
  }
};

/**
 * Execute a idempotent query to ensure that all nodes are attempted and the extra speculative
 * execution attempts were aborted (via metrics).
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
CASSANDRA_INTEGRATION_TEST_F(SpeculativeExecutionTests, AttemptOnAllNodes) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2.0);
  Session session =
      default_cluster().with_constant_speculative_execution_policy(100, 2).connect(keyspace_name_);

  // Ensure all hosts were attempted
  Result result = session.execute(create_select_statement());
  std::vector<std::string> hosts = result.attempted_hosts();
  EXPECT_EQ(3u, hosts.size());
  EXPECT_TRUE(std::find(hosts.begin(), hosts.end(), result.host()) != hosts.end());

  // Ensure the other speculative executions were aborted
  wait_for_count(session, 2u);
}

/**
 * Execute a idempotent query to ensure that only two nodes are attempted and the extra speculative
 * execution attempt were aborted (via metrics).
 *
 * Note: This is done by limiting the number of max allowed speculative executions to one.
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
CASSANDRA_INTEGRATION_TEST_F(SpeculativeExecutionTests, LimitToTwoNodes) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2.0);
  Session session =
      default_cluster().with_constant_speculative_execution_policy(100, 1).connect(keyspace_name_);

  // Ensure only two hosts were attempted
  Result result = session.execute(create_select_statement());
  std::vector<std::string> hosts = result.attempted_hosts();
  EXPECT_EQ(2u, hosts.size());
  EXPECT_TRUE(std::find(hosts.begin(), hosts.end(), result.host()) != hosts.end());

  // Ensure the other speculative executions were aborted
  wait_for_count(session, 1u);
}

/**
 * Execute a idempotent query to ensure that only one node is attempted and that no other
 * speculative execution attempts were performed (via metrics).
 *
 * Note: This is done by having a delay greater than the UDF timeout.
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
CASSANDRA_INTEGRATION_TEST_F(SpeculativeExecutionTests, DelayIsNotReached) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2.0);
  Session session =
      default_cluster().with_constant_speculative_execution_policy(5000, 2).connect(keyspace_name_);

  Result result = session.execute(create_select_statement());
  std::vector<std::string> hosts = result.attempted_hosts();
  EXPECT_EQ(1u, hosts.size());
  EXPECT_EQ(result.host(), hosts[0]);
  EXPECT_EQ(0u, session.speculative_execution_metrics().count);
}

/**
 * Execute a standard (non-idempotent) query to ensure that only one node is attempted and that no
 * speculative execution attempts were performed (via metrics).
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
CASSANDRA_INTEGRATION_TEST_F(SpeculativeExecutionTests, DisabledByDefault) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2.0);
  Session session =
      default_cluster().with_constant_speculative_execution_policy(100, 2).connect(keyspace_name_);

  Statement select_statement(
      format_string(SPECULATIVE_EXECUTION_SELECT_FORMAT, table_name_.c_str(), 0));
  select_statement.set_record_attempted_hosts(true);
  Result result = session.execute(select_statement);
  std::vector<std::string> hosts = result.attempted_hosts();
  EXPECT_EQ(1u, hosts.size());
  EXPECT_EQ(result.host(), hosts[0]);
  EXPECT_EQ(0u, session.speculative_execution_metrics().count);
}

/**
 * Execute a idempotent query to ensure that all nodes are attempted, all speculative executions
 * attempts were aborted (via metrics), and the driver returns a timeout error.
 *
 * Note: This is done by setting s request timeout lower than the UDF timeout.
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
CASSANDRA_INTEGRATION_TEST_F(SpeculativeExecutionTests, Timeout) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2.0);
  Session session =
      default_cluster().with_constant_speculative_execution_policy(100, 2).connect(keyspace_name_);

  // Ensure all hosts were attempted and a timeout occurred
  Result result = session.execute(create_select_statement(300), false);
  EXPECT_EQ(CASS_ERROR_LIB_REQUEST_TIMED_OUT, result.error_code());
  std::vector<std::string> hosts = result.attempted_hosts();
  EXPECT_EQ(3u, hosts.size());

  // Ensure all speculative executions were aborted
  wait_for_count(session, 3u);
}
