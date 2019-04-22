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

#include "simulacron_integration.hpp"

#include "cassandra.h"

/**
 * Idempotent integration tests
 */
class IdempotentTest : public SimulacronIntegration {
using SimulacronIntegration::execute_mock_query;
public:
  void SetUp() {
    number_dc1_nodes_ = 3;
    SimulacronIntegration::SetUp();
  }

  /**
   * Create and execute a mock query with the desired idempotent setting on the
   * statement and apply the IdempotentRetryPolicy (with logging retry policy)
   * to advance to the next host on failures
   *
   * NOTE: The statement execution is performed without assertions on the
   *       error code returned from the future; use Result::error_code() to
   *       check value
   *
   * @param is_idempotent True if statement execution is idempotent; false
   *                      otherwise
   * @param apply_custom_retry_policy True if custom idempotent retry policy
   *                                  should be enabled; false otherwise
   * @return Result from the executed mock query; see Result::error_code()
   */
  test::driver::Result execute_mock_query(bool is_idempotent,
    bool apply_custom_retry_policy) {
    // Create the statement with the desired idempotence and custom retry policy
    test::driver::Statement statement("mock query");
    statement.set_consistency(CASS_CONSISTENCY_ONE);
    statement.set_idempotent(is_idempotent);
    statement.set_record_attempted_hosts(true);

    // Determine if the custom or default retry policy should be applied
    test::driver::RetryPolicy policy((apply_custom_retry_policy
      ? NextHostRetryPolicy::policy()
      : test::driver::DefaultRetryPolicy()));
    test::driver::LoggingRetryPolicy logging_policy(policy);
    statement.set_retry_policy(logging_policy);

    // Execute the statement and return the result
    return session_.execute(statement, false);
  }
};

/**
 * Perform query using a non-idempotent statement and validate error
 *
 * This test will perform a query using a multi-node Simulacron server cluster
 * simulating a write timeout on node one; each result will be validated to
 * ensure error exists or query succeeded
 *
 * @test_category queries:basic
 * @since 1.0.0
 * @expected_result Successful result on all nodes except on node one
 */
SIMULACRON_INTEGRATION_TEST_F(IdempotentTest, WriteTimeoutNonIdempotentNoRetry) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Simulate a write timeout on node 1
  prime_mock_query_with_result(new prime::WriteTimeout(), 1);

  // Loop through all the nodes in the cluster execute the mock query
  for (unsigned int n = 0; n < number_dc1_nodes_; ++n) {
    test::driver::Result result = execute_mock_query(false, true);
    if (result.host() == sc_->get_ip_address(1)) {
      ASSERT_EQ(CASS_ERROR_SERVER_WRITE_TIMEOUT, result.error_code());
    } else {
      ASSERT_EQ(CASS_OK, result.error_code());
    }
  }
}


/**
 * Perform query using a idempotent statement; no errors should occur
 *
 * This test will perform a query using a multi-node Simulacron server cluster
 * simulating a write timeout on node one; each result will be validated to
 * ensure query succeeded and write timeout node was attempted
 *
 * @test_category queries:idempotent
 * @since 1.0.0
 * @expected_result Successful result on all nodes
 */
SIMULACRON_INTEGRATION_TEST_F(IdempotentTest, WriteTimeoutIdempotentRetry) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Simulate a write timeout on node 1
  prime_mock_query_with_result(new prime::WriteTimeout(), 1);

  // Loop through all the nodes in the cluster execute the mock query
  bool was_node_one_attempted = false;
  for (unsigned int n = 0; n < number_dc1_nodes_; ++n) {
    test::driver::Result result = execute_mock_query(true, true);
    std::vector<std::string> attempted_hosts = result.attempted_hosts();
    ASSERT_EQ(CASS_OK, result.error_code());
    if (result.attempted_hosts().size() > 1) {
      ASSERT_EQ(sc_->get_ip_address(1), attempted_hosts.at(0));
      was_node_one_attempted = true;
    }
  }

  // Ensure that node one was attempted (NextHostRetryPolicy used))
  ASSERT_TRUE(was_node_one_attempted);
}

/**
 * Perform query using a non-idempotent statement while a connection is closed
 *
 * This test will perform a query using a multi-node Simulacron server cluster
 * simulating a closed connection on node one during a query; each result will
 * be validated to ensure error exists or query succeeded
 *
 * @test_category queries:basic
 * @since 1.0.0
 * @expected_result Successful result on all nodes except on node one
 */
SIMULACRON_INTEGRATION_TEST_F(IdempotentTest, ClosedConnectionNonIdempotentNoRetry) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Simulate a closed connection on node 1
  prime_mock_query_with_result(new prime::CloseConnection(), 1);

  // Loop through all the nodes in the cluster execute the mock query
  for (unsigned int n = 0; n < number_dc1_nodes_; ++n) {
    test::driver::Result result = execute_mock_query(false, true);
    if (result.host() == sc_->get_ip_address(1)) {
      ASSERT_EQ(CASS_ERROR_LIB_REQUEST_TIMED_OUT, result.error_code());
    } else {
      ASSERT_EQ(CASS_OK, result.error_code());
    }
  }
}


/**
 * Perform query using a idempotent statement while a connection is closed
 *
 * This test will perform a query using a multi-node Simulacron server cluster
 * simulating a closed connection on node one during a query; each result will
 * be validated to ensure query succeeded and closed connection node was
 * attempted
 *
 * @test_category queries:idempotent
 * @since 1.0.0
 * @expected_result Successful result with request execution on all nodes
 */
SIMULACRON_INTEGRATION_TEST_F(IdempotentTest, ClosedConnectionIdempotentRetry) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Simulate a closed connection on node 1
  prime_mock_query_with_result(new prime::CloseConnection(), 1);

  // Loop through all the nodes in the cluster execute the mock query
  bool was_node_one_attempted = false;
  for (unsigned int n = 0; n < number_dc1_nodes_; ++n) {
    test::driver::Result result = execute_mock_query(true, false);
    std::vector<std::string> attempted_hosts = result.attempted_hosts();
    ASSERT_EQ(CASS_OK, result.error_code());
    if (result.attempted_hosts().size() > 1) {
      ASSERT_EQ(sc_->get_ip_address(1), attempted_hosts.at(0));
      was_node_one_attempted = true;
    }
  }

  // Ensure that node one was attempted (NextHostRetryPolicy used))
  ASSERT_TRUE(was_node_one_attempted);
}
