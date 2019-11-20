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

/**
 * Consistency integration tests; two node cluster
 */
class ConsistencyTwoNodeClusterTests : public Integration {
public:
  ConsistencyTwoNodeClusterTests() { number_dc1_nodes_ = 2; }

  virtual void SetUp() {
    // Call the parent setup function
    Integration::SetUp();

    // Create the table, insert and select statements for the test (with values)
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
    insert_ = Statement(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "?", "?"), 2);
    insert_.bind<Integer>(0, Integer(0));
    insert_.bind<Integer>(1, Integer(1));
    select_ = Statement(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "?"), 1);
    select_.bind<Integer>(0, Integer(0));
  }

protected:
  /**
   * Insert statement ready to be executed
   */
  Statement insert_;
  /**
   * Select statement ready to be executed
   */
  Statement select_;
};

/**
 * Consistency integration tests; three node cluster
 */
class ConsistencyThreeNodeClusterTests : public ConsistencyTwoNodeClusterTests {
public:
  ConsistencyThreeNodeClusterTests() {
    number_dc1_nodes_ = 3;
    replication_factor_ = 3;
  }
};

/**
 * Serial consistency integration tests; one node cluster
 */
class SerialConsistencyTests : public Integration {
public:
  SerialConsistencyTests() { number_dc1_nodes_ = 1; }

  virtual void SetUp() {
    // Call the parent setup function
    Integration::SetUp();

    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
  }

  /**
   * Insert a row using "IF NOT EXISTS" with a provided serial consistency.
   *
   * @param serial_conistency The serial consistency to use for the LWT.
   * @return The result of the insert
   */
  Result insert_if_not_exists(CassConsistency serial_conistency) {
    Statement statement(format_string("INSERT INTO %s (key, value) VALUES (1, 99) IF NOT EXISTS",
                                      table_name_.c_str()));
    statement.set_serial_consistency(serial_conistency);
    return session_.execute(statement, false);
  }
};

/**
 * Perform consistency inserts and selects using consistency `ONE`
 *
 * This test will perform insert and select operations using a simple statement
 * while validating the operation was successful against a two node cluster.
 *
 * @test_category consistency
 * @since core:1.0.0
 * @expected_result Successful insert and select using consistency `ONE`
 */
CASSANDRA_INTEGRATION_TEST_F(ConsistencyTwoNodeClusterTests, SimpleOne) {
  CHECK_FAILURE;

  // Assign the consistency level for the test
  insert_.set_consistency(CASS_CONSISTENCY_ONE);
  select_.set_consistency(CASS_CONSISTENCY_ONE);

  // Perform insert and select operations
  session_.execute(insert_);
  session_.execute(select_);
}

/**
 * Perform consistency inserts and selects using consistency `TWO`
 *
 * This test will perform insert and select operations using a simple statement
 * while validating the operation failed against a two node cluster.
 *
 * @test_category consistency
 * @since core:1.0.0
 * @expected_result Failed insert and select using consistency `TWO`
 */
CASSANDRA_INTEGRATION_TEST_F(ConsistencyTwoNodeClusterTests, SimpleLocalTwo) {
  CHECK_FAILURE;

  // Assign the consistency level for the test
  insert_.set_consistency(CASS_CONSISTENCY_TWO);
  select_.set_consistency(CASS_CONSISTENCY_TWO);

  // Perform insert and select operations (should fail: N=2, RF=1)
  ASSERT_EQ(CASS_ERROR_SERVER_UNAVAILABLE, session_.execute(insert_, false).error_code());
  ASSERT_EQ(CASS_ERROR_SERVER_UNAVAILABLE, session_.execute(select_, false).error_code());
}

/**
 * Perform consistency inserts and selects using consistency `THREE`
 *
 * This test will perform insert and select operations using a simple statement
 * while validating the operation failed against a two node cluster.
 *
 * @test_category consistency
 * @since core:1.0.0
 * @expected_result Failed insert and select using consistency `THREE`
 */
CASSANDRA_INTEGRATION_TEST_F(ConsistencyTwoNodeClusterTests, SimpleLocalThree) {
  CHECK_FAILURE;

  // Assign the consistency level for the test
  insert_.set_consistency(CASS_CONSISTENCY_THREE);
  select_.set_consistency(CASS_CONSISTENCY_THREE);

  // Perform insert and select operations (should fail: N=2, RF=1)
  ASSERT_EQ(CASS_ERROR_SERVER_UNAVAILABLE, session_.execute(insert_, false).error_code());
  ASSERT_EQ(CASS_ERROR_SERVER_UNAVAILABLE, session_.execute(select_, false).error_code());
}

/**
 * Perform consistency inserts and selects using consistency `ANY`
 *
 * This test will perform insert and select operations using a simple statement
 * while validating the insert was successful and the select caused an invalid
 * query operation against a two node cluster.
 *
 * @test_category consistency
 * @since core:1.0.0
 * @expected_result Successful insert and failed select using consistency `ANY`
 */
CASSANDRA_INTEGRATION_TEST_F(ConsistencyTwoNodeClusterTests, SimpleAny) {
  CHECK_FAILURE;

  // Assign the consistency level for the test
  insert_.set_consistency(CASS_CONSISTENCY_ANY);
  select_.set_consistency(CASS_CONSISTENCY_ANY);

  // Perform insert and select operations (NOTE: `ANY` is for writes only)
  session_.execute(insert_);
  ASSERT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(select_, false).error_code());
}

/**
 * Perform consistency inserts and selects using consistency `LOCAL_QUORUM`
 *
 * This test will perform insert and select operations using a simple statement
 * while validating the operation was successful against a two node cluster.
 *
 * @test_category consistency
 * @since core:1.0.0
 * @expected_result Successful insert and select using consistency
 *                 `LOCAL_QUORUM`
 */
CASSANDRA_INTEGRATION_TEST_F(ConsistencyTwoNodeClusterTests, SimpleLocalQuorum) {
  CHECK_FAILURE;

  // Assign the consistency level for the test
  insert_.set_consistency(CASS_CONSISTENCY_LOCAL_QUORUM);
  select_.set_consistency(CASS_CONSISTENCY_LOCAL_QUORUM);

  // Perform insert and select operations
  session_.execute(insert_);
  session_.execute(select_);
}

/**
 * Perform consistency inserts and selects using consistency `EACH_QUORUM`
 *
 * This test will perform insert and select operations using a simple statement
 * while validating the insert was successful and the select operation caused
 * an invalid query (for Cassandra < v3.0.0) or was successful (for Cassandra
 * >= 3.0.0) against a two node cluster.
 *
 * @test_category consistency
 * @since core:1.0.0
 * @expected_result Successful insert and failed select using consistency
 *                  `EACH_QUORUM` (Successful select using Cassandra v3.0.0+;
 *                 see CASSANDRA-9602)
 */
CASSANDRA_INTEGRATION_TEST_F(ConsistencyTwoNodeClusterTests, SimpleEachQuorum) {
  CHECK_FAILURE;
  CHECK_VERSION(3.0.0);

  // Assign the consistency level for the test
  insert_.set_consistency(CASS_CONSISTENCY_EACH_QUORUM);
  select_.set_consistency(CASS_CONSISTENCY_EACH_QUORUM);

  // Perform insert and select operations
  session_.execute(insert_);
  // Handle `EACH_QUORUM` read support; added to C* v3.0.0
  // https://issues.apache.org/jira/browse/CASSANDRA-9602
  if (server_version_ >= "3.0.0") {
    session_.execute(select_);
  } else {
    ASSERT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, session_.execute(select_, false).error_code());
  }
}

/**
 * Perform multiple inserts and selects using different consistencies against a
 * cluster with a single decommissioned node
 *
 * This test will perform insert and select operations using a simple statement
 * while validating the operation were successful or failed against a three
 * three node cluster with a decommissioned node.
 *
 * @test_category consistency
 * @since core:1.0.0
 * @expected_result Successful insert and select using multiple consistencies:
 *                  `ALL`, `ONE`, `TWO`, and `QUORUM`
 *                  Failed insert and select using multiple consistencies:
 *                  `ALL` (after decommission) and `THREE`
 */
CASSANDRA_INTEGRATION_TEST_F(ConsistencyThreeNodeClusterTests, OneNodeDecommissioned) {
  CHECK_FAILURE;

  // Perform a sanity check against a full healthy cluster (N=3, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_ALL);
  select_.set_consistency(CASS_CONSISTENCY_ALL);
  session_.execute(insert_);
  session_.execute(select_);

  // Decommission node two
  force_decommission_node(2);

  // Perform a check using consistency `QUORUM` (N=2, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_QUORUM);
  select_.set_consistency(CASS_CONSISTENCY_QUORUM);
  session_.execute(insert_);
  session_.execute(select_);

  // Perform a check using consistency `ONE` (N=2, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_ONE);
  select_.set_consistency(CASS_CONSISTENCY_ONE);
  session_.execute(insert_);
  session_.execute(select_);

  // Perform a check using consistency `TWO` (N=2, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_TWO);
  select_.set_consistency(CASS_CONSISTENCY_TWO);
  session_.execute(insert_);
  session_.execute(select_);

  // Perform a check using consistency `ALL` (should fail N=2, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_ALL);
  select_.set_consistency(CASS_CONSISTENCY_ALL);
  ASSERT_NE(CASS_OK, session_.execute(insert_, false).error_code());
  ASSERT_NE(CASS_OK, session_.execute(select_, false).error_code());

  // Perform a check using consistency `THREE` (should fail N=2, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_THREE);
  select_.set_consistency(CASS_CONSISTENCY_THREE);
  ASSERT_NE(CASS_OK, session_.execute(insert_, false).error_code());
  ASSERT_NE(CASS_OK, session_.execute(select_, false).error_code());
}

/**
 * Perform multiple inserts and selects using different consistencies against a
 * cluster with a two decommissioned nodes
 *
 * This test will perform insert and select operations using a simple statement
 * while validating the operation were successful or failed against a three
 * node cluster with two decommissioned nodes.
 *
 * @test_category consistency
 * @since core:1.0.0
 * @expected_result Successful insert and select using multiple consistencies:
 *                  `ALL`, and `ONE`
 *                  Failed insert and select using multiple consistencies:
 *                  `ALL` (after decommission), `QUORUM`, `TWO`, and `THREE`
 */
CASSANDRA_INTEGRATION_TEST_F(ConsistencyThreeNodeClusterTests, TwoNodesDecommissioned) {
  CHECK_FAILURE;

  // Perform a sanity check against a full healthy cluster (N=3, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_ALL);
  select_.set_consistency(CASS_CONSISTENCY_ALL);
  session_.execute(insert_);
  session_.execute(select_);

  // Decommission node two and three
  force_decommission_node(2);
  force_decommission_node(3);

  // Perform a check using consistency `ONE` (N=1, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_ONE);
  select_.set_consistency(CASS_CONSISTENCY_ONE);
  session_.execute(insert_);
  session_.execute(select_);

  // Perform a check using consistency `ALL` (should fail N=1, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_ALL);
  select_.set_consistency(CASS_CONSISTENCY_ALL);
  ASSERT_NE(CASS_OK, session_.execute(insert_, false).error_code());
  ASSERT_NE(CASS_OK, session_.execute(select_, false).error_code());

  // Perform a check using consistency `QUORUM` (should fail N=1, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_QUORUM);
  select_.set_consistency(CASS_CONSISTENCY_QUORUM);
  ASSERT_NE(CASS_OK, session_.execute(insert_, false).error_code());
  ASSERT_NE(CASS_OK, session_.execute(select_, false).error_code());

  // Perform a check using consistency `TWO` (should fail N=1, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_TWO);
  select_.set_consistency(CASS_CONSISTENCY_TWO);
  ASSERT_NE(CASS_OK, session_.execute(insert_, false).error_code());
  ASSERT_NE(CASS_OK, session_.execute(select_, false).error_code());

  // Perform a check using consistency `THREE` (should fail N=1, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_THREE);
  select_.set_consistency(CASS_CONSISTENCY_THREE);
  ASSERT_NE(CASS_OK, session_.execute(insert_, false).error_code());
  ASSERT_NE(CASS_OK, session_.execute(select_, false).error_code());
}

/**
 * Perform multiple inserts and selects using different consistencies against a
 * cluster with varying stopped nodes
 *
 * This test will perform insert and select operations using a simple statement
 * while validating the operation were successful against a three node cluster
 * with one stopped node followed by two stopped nodes using the downgrading
 * retry policy.
 *
 * @test_category consistency
 * @since core:1.0.0
 * @expected_result Successful insert and select using multiple consistencies
 *                  with the downgrading retry policy applied
 */
CASSANDRA_INTEGRATION_TEST_F(ConsistencyThreeNodeClusterTests, DowngradingRetryPolicy) {
  CHECK_FAILURE;

  // Create a new session to utilize the downgrading retry policy
  cluster_.with_retry_policy(DowngradingConsistencyRetryPolicy());
  Session session = cluster_.connect(keyspace_name_);

  // Perform a sanity check against a full healthy cluster (N=3, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_ALL);
  select_.set_consistency(CASS_CONSISTENCY_ALL);
  session.execute(insert_);
  session.execute(select_);

  // Stop node two
  stop_node(2);

  // Perform a check using consistency `QUORUM` (N=2,  RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_QUORUM);
  select_.set_consistency(CASS_CONSISTENCY_QUORUM);
  session.execute(insert_);
  session.execute(select_);

  // Stop node three
  stop_node(3);

  // Perform a check using consistency `QUORUM` (N=1,  RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_QUORUM);
  select_.set_consistency(CASS_CONSISTENCY_QUORUM);
  session.execute(insert_);
  session.execute(select_);

  // Perform a check using consistency `TWO` (should fail N=1, RF=3)
  insert_.set_consistency(CASS_CONSISTENCY_TWO);
  select_.set_consistency(CASS_CONSISTENCY_TWO);
  session.execute(insert_);
  session.execute(select_);
}

/**
 * Verify that the serial consistency flag is passed properly when using a LWT.
 */
CASSANDRA_INTEGRATION_TEST_F(SerialConsistencyTests, Simple) {
  CHECK_FAILURE;

  Result result = insert_if_not_exists(CASS_CONSISTENCY_SERIAL);
  ASSERT_GT(result.row_count(), 0u);
  ASSERT_GT(result.column_count(), 0u);
  EXPECT_TRUE(result.first_row().column_by_name<Boolean>("[applied]").value());
}

/**
 * Verify that the serial consistency flag is passed properly when using a LWT and causes an error
 * when invalid.
 */
CASSANDRA_INTEGRATION_TEST_F(SerialConsistencyTests, Invalid) {
  CHECK_FAILURE;

  Result result = insert_if_not_exists(CASS_CONSISTENCY_ONE); // Invalid serial consistency
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, result.error_code());
}
