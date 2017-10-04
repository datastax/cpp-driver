/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "scassandra_integration.hpp"
#include "next_host_retry_policy.hpp"

#include "cassandra.h"

/**
 * Idempotent integration tests
 */
class IdempotentTest : public SCassandraIntegration {
public:
  using SCassandraIntegration::execute_mock_query;

  void SetUp() {
    number_dc1_nodes_ = 3;
    SCassandraIntegration::SetUp();
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
 * This test will perform a query using a multi-node SCassandra server cluster
 * simulating a write timeout on node one; each result will be validated to
 * ensure error exists or query succeeded
 *
 * @test_category queries:basic
 * @since 1.0.0
 * @expected_result Successful result on all nodes except on node one
 */
SCASSANDRA_INTEGRATION_TEST_F(IdempotentTest, WriteTimeoutNonIdempotentNoRetry) {
  // Simulate a write timeout on node 1
  prime_mock_query_with_error(PrimingResult::write_request_timeout(), 1);

  // Loop through all the nodes in the cluster execute the mock query
  for (unsigned int n = 0; n < number_dc1_nodes_; ++n) {
    test::driver::Result result = execute_mock_query(false, true);
    if (result.host() == scc_->get_ip_address(1)) {
      ASSERT_EQ(CASS_ERROR_SERVER_WRITE_TIMEOUT, result.error_code());
    } else {
      ASSERT_EQ(CASS_OK, result.error_code());
    }
  }
}


/**
 * Perform query using a idempotent statement; no errors should occur
 *
 * This test will perform a query using a multi-node SCassandra server cluster
 * simulating a write timeout on node one; each result will be validated to
 * ensure query succeeded and write timeout node was attempted
 *
 * @test_category queries:idempotent
 * @since 1.0.0
 * @expected_result Successful result on all nodes
 */
SCASSANDRA_INTEGRATION_TEST_F(IdempotentTest, WriteTimeoutIdempotentRetry) {
  // Simulate a write timeout on node 1 and server error on node 3
  prime_mock_query_with_error(PrimingResult::write_request_timeout(), 1);

  // Loop through all the nodes in the cluster execute the mock query
  bool was_node_one_attempted = false;
  for (unsigned int n = 0; n < number_dc1_nodes_; ++n) {
    test::driver::Result result = execute_mock_query(true, true);
    std::vector<std::string> attempted_hosts = result.attempted_hosts();
    ASSERT_EQ(CASS_OK, result.error_code());
    if (result.attempted_hosts().size() > 1) {
      ASSERT_EQ(scc_->get_ip_address(1), attempted_hosts.at(0));
      was_node_one_attempted = true;
    }
  }

  // Ensure that node one was attempted (NextHostRetryPolicy used))
  ASSERT_TRUE(was_node_one_attempted);
}

/**
 * Perform query using a non-idempotent statement while a connection is closed
 *
 * This test will perform a query using a multi-node SCassandra server cluster
 * simulating a closed connection on node one during a query; each result will
 * be validated to ensure error exists or query succeeded
 *
 * @test_category queries:basic
 * @since 1.0.0
 * @expected_result Successful result on all nodes except on node one
 */
SCASSANDRA_INTEGRATION_TEST_F(IdempotentTest, ClosedConnectionNonIdempotentNoRetry) {
  // Simulate a closed connection on node 1
  prime_mock_query_with_error(PrimingResult::closed_connection(), 1);
  std::stringstream node_closed;
  node_closed << "to host " << scc_->get_ip_prefix() << "1 closed";
  logger_.add_critera(node_closed.str());

  // Loop through all the nodes in the cluster execute the mock query
  for (unsigned int n = 0; n < number_dc1_nodes_; ++n) {
    test::driver::Result result = execute_mock_query(false, true);
    if (result.host() == scc_->get_ip_address(1)) {
      ASSERT_EQ(CASS_ERROR_LIB_REQUEST_TIMED_OUT, result.error_code());
    } else {
      ASSERT_EQ(CASS_OK, result.error_code());
    }
  }

  // Ensure that node one connection was closed
  ASSERT_EQ(1u, logger_.get_count());
}


/**
 * Perform query using a idempotent statement while a connection is closed
 *
 * This test will perform a query using a multi-node SCassandra server cluster
 * simulating a closed connection on node one during a query; each result will
 * be validated to ensure query succeeded and closed connection node was
 * attempted
 *
 * @test_category queries:idempotent
 * @since 1.0.0
 * @expected_result Successful result on all nodes
 */
SCASSANDRA_INTEGRATION_TEST_F(IdempotentTest, ClosedConnectionIdempotentRetry) {
  // Simulate a closed connection on node 1
  prime_mock_query_with_error(PrimingResult::closed_connection(), 1);

  // Loop through all the nodes in the cluster execute the mock query
  bool was_node_one_attempted = false;
  for (unsigned int n = 0; n < number_dc1_nodes_; ++n) {
    test::driver::Result result = execute_mock_query(true, false);
    std::vector<std::string> attempted_hosts = result.attempted_hosts();
    ASSERT_EQ(CASS_OK, result.error_code());
    if (result.attempted_hosts().size() > 1) {
      ASSERT_EQ(scc_->get_ip_address(1), attempted_hosts.at(0));
      was_node_one_attempted = true;
    }
  }

  // Ensure that node one was attempted (NextHostRetryPolicy used))
  ASSERT_TRUE(was_node_one_attempted);
}
