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

#define SPECULATIVE_EXECUTION_SELECT_FORMAT "SELECT timeout(value) FROM %s.%s WHERE key=%d"
#define SPECULATIVE_EXECUTION_CREATE_TIMEOUT_UDF_FORMAT   \
  "CREATE OR REPLACE FUNCTION %s.timeout(arg int) "       \
  "RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java " \
  "AS $$ long start = System.currentTimeMillis(); "       \
  "while(System.currentTimeMillis() - start < arg) {"     \
  ";;"                                                    \
  "}"                                                     \
  "return arg;"                                           \
  "$$;"

class MetricsTests : public Integration {
public:
  MetricsTests() { number_dc1_nodes_ = 2; }
};

/**
 * This test ensures that the driver is reporting the proper connection statistics
 *
 * @since 2.0.0
 * @jira_ticket CPP-188
 */
CASSANDRA_INTEGRATION_TEST_F(MetricsTests, StatsConnections) {
  CHECK_FAILURE;

  Session session = default_cluster()
                        .with_num_threads_io(1)
                        .with_core_connections_per_host(1)
                        .with_constant_reconnect(10)
                        .connect(); // low reconnect for faster node restart

  CassMetrics metrics = session.metrics();
  EXPECT_EQ(2u, metrics.stats.total_connections);

  stop_node(1);
  metrics = session.metrics();
  EXPECT_EQ(1u, metrics.stats.total_connections);

  start_node(1);
  metrics = session.metrics();
  EXPECT_EQ(2u, metrics.stats.total_connections);
}

/**
 * This test ensures that the driver is reporting the proper timeouts for connections
 *
 * @since 2.0.0
 * @jira_ticket CPP-188
 */
CASSANDRA_INTEGRATION_TEST_F(MetricsTests, ErrorsConnectionTimeouts) {
  CHECK_FAILURE;

  Session session =
      default_cluster().with_core_connections_per_host(2).with_connect_timeout(1).connect(
          "", false); // Quick connection timeout and no assertion

  CassMetrics metrics = session.metrics();
  EXPECT_GE(2u, metrics.errors.connection_timeouts);
}

/**
 * This test ensures that the driver is reporting the proper timeouts for requests
 *
 * @since 2.0.0
 * @jira_ticket CPP-188
 */
CASSANDRA_INTEGRATION_TEST_F(MetricsTests, ErrorsRequestTimeouts) {
  CHECK_FAILURE;

  Session session = default_cluster()
                        .with_connect_timeout(30000) // 30s timeout
                        .with_request_timeout(1)
                        .connect(); // very low request timeout

  for (int n = 0; n < 100; ++n) {
    session.execute_async(SELECT_ALL_SYSTEM_LOCAL_CQL);
  }

  CassMetrics metrics = session.metrics();
  for (int i = 0; i < 100 && metrics.errors.request_timeouts == 0; ++i) {
    metrics = session.metrics();
    msleep(100);
  }
  EXPECT_GT(metrics.errors.request_timeouts, 0u);
}

/**
 * This test ensures that the histogram data calculated by the driver is being updated.
 *
 * NOTE: The data returned by the driver is not validated as this is performed
 *       in the unit tests.
 *
 * @since 2.0.0
 * @jira_ticket CPP-188
 */
CASSANDRA_INTEGRATION_TEST_F(MetricsTests, Requests) {
  CHECK_FAILURE;

  CassMetrics metrics = session_.metrics();
  for (int i = 0; i < 600 && metrics.requests.fifteen_minute_rate == 0; ++i) {
    session_.execute_async(SELECT_ALL_SYSTEM_LOCAL_CQL);
    metrics = session_.metrics();
    msleep(100);
  }

  EXPECT_LT(metrics.requests.min, CASS_UINT64_MAX);
  EXPECT_GT(metrics.requests.max, 0u);
  EXPECT_GT(metrics.requests.mean, 0u);
  EXPECT_GT(metrics.requests.stddev, 0u);
  EXPECT_GT(metrics.requests.median, 0u);
  EXPECT_GT(metrics.requests.percentile_75th, 0u);
  EXPECT_GT(metrics.requests.percentile_95th, 0u);
  EXPECT_GT(metrics.requests.percentile_98th, 0u);
  EXPECT_GT(metrics.requests.percentile_99th, 0u);
  EXPECT_GT(metrics.requests.percentile_999th, 0u);
  EXPECT_GT(metrics.requests.mean_rate, 0.0);
  EXPECT_GT(metrics.requests.one_minute_rate, 0.0);
  EXPECT_GT(metrics.requests.five_minute_rate, 0.0);
  EXPECT_GT(metrics.requests.fifteen_minute_rate, 0.0);
}

/**
 * This test ensures that the histogram data for the speculative execution metrics calculated by the
 * driver is being updated.
 *
 * NOTE: The data returned by the driver is not validated as this is performed
 *       in the unit tests.
 *
 * @since 2.0.0
 * @jira_ticket CPP-188
 */
CASSANDRA_INTEGRATION_TEST_F(MetricsTests, SpeculativeExecutionRequests) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2.0);

  Session session = default_cluster().with_constant_speculative_execution_policy(100, 10).connect();

  session.execute(format_string(CASSANDRA_KEY_VALUE_QUALIFIED_TABLE_FORMAT, keyspace_name_.c_str(),
                                table_name_.c_str(), "int", "int"));
  session.execute(format_string(CASSANDRA_KEY_VALUE_QUALIFIED_INSERT_FORMAT, keyspace_name_.c_str(),
                                table_name_.c_str(), "0", "200"));
  session.execute(
      format_string(SPECULATIVE_EXECUTION_CREATE_TIMEOUT_UDF_FORMAT, keyspace_name_.c_str()));
  Statement statement(format_string(SPECULATIVE_EXECUTION_SELECT_FORMAT, keyspace_name_.c_str(),
                                    table_name_.c_str(), 0));
  statement.set_idempotent(true);
  statement.set_request_timeout(30000);

  CassSpeculativeExecutionMetrics metrics = session.speculative_execution_metrics();
  for (int i = 0; i < 600 && metrics.count < 10; ++i) {
    session.execute_async(statement);
    metrics = session.speculative_execution_metrics();
    msleep(100);
  }

  EXPECT_LT(metrics.min, CASS_UINT64_MAX);
  EXPECT_GT(metrics.max, 0u);
  EXPECT_GT(metrics.mean, 0u);
  EXPECT_GT(metrics.stddev, 0u);
  EXPECT_GT(metrics.median, 0u);
  EXPECT_GT(metrics.percentile_75th, 0u);
  EXPECT_GT(metrics.percentile_95th, 0u);
  EXPECT_GT(metrics.percentile_98th, 0u);
  EXPECT_GT(metrics.percentile_99th, 0u);
  EXPECT_GT(metrics.percentile_999th, 0u);
  EXPECT_GT(metrics.percentage, 0.0);
  EXPECT_GT(metrics.count, 0u);
}
