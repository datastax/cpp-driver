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
 * Batch (statement) integration tests using standard key/value pair
 *
 * Cluster with a single node
 */
class BatchSingleNodeClusterTests : public Integration {
public:
  BatchSingleNodeClusterTests()
      : value_cql_data_type_("text") {}

  virtual void SetUp() {
    // Call the parent setup function
    Integration::SetUp();

    // Create the table, insert and select statements for the test
    session_.execute(format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int",
                                   value_cql_data_type_.c_str()));
    create_queries_select_statements();
  }

protected:
  /**
   * CQL data type to use for the value
   */
  std::string value_cql_data_type_;
  /**
   * The insert query used for an insert statement
   */
  std::string insert_query_;
  /**
   * The update query used for an update statement
   */
  std::string update_query_;
  /**
   * Prepared select statement
   */
  Prepared select_prepared_;

  /**
   * Create the queries and select statement for the test
   */
  virtual void create_queries_select_statements() {
    insert_query_ = format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "?", "?");
    update_query_ =
        format_string(CASSANDRA_UPDATE_VALUE_FORMAT, table_name_.c_str(), "value + ?", "?");
    select_prepared_ =
        session_.prepare(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "?"));
  }

  /**
   * Validate the inserts performed in the test case
   *
   * @param expected_rows Number of expected rows in the table
   */
  void validate_inserts(int expected_rows) {
    // Determine the number of rows in the table
    int number_of_rows = default_select_count();
    ASSERT_EQ(expected_rows, number_of_rows);

    // Iterate over each key in the row and validate
    for (int i = 0; i < expected_rows; ++i) {
      // Bind the key and get the result of the select
      Statement statement = select_prepared_.bind();
      statement.bind<Integer>(0, Integer(i));
      Result result = session_.execute(statement);

      // Validate the result
      ASSERT_EQ(Text(format_string("test data %d", i)), result.first_row().next().as<Text>());
    }
  }

  /**
   * Validate the result for the text data type
   *
   * @param result The result to validate
   * @param index The index/row being validated
   */
  virtual void validate_result(Result result, int index) {
    ASSERT_EQ(Text(format_string("test data %d", index)), result.first_row().next().as<Text>());
  }
};

/**
 * Batch (statement) integration tests using counter key/value pair
 *
 * Cluster with a single node
 */
class BatchCounterSingleNodeClusterTests : public BatchSingleNodeClusterTests {
public:
  BatchCounterSingleNodeClusterTests() { value_cql_data_type_ = "counter"; }

  /**
   * Validate the result for the text data type
   *
   * @param result The result to validate
   * @param index The index/row being validated
   */
  void validate_result(Result result, int index) {
    ASSERT_EQ(Counter(index), result.first_row().next().as<Counter>());
  }
};

/**
 * Batch (statement) integration tests using counter key/value pair
 *
 * Cluster with three nodes
 */
class BatchCounterThreeNodeClusterTests : public BatchCounterSingleNodeClusterTests {
  void SetUp() {
    // Call the parent setup function and increase the cluster size
    number_dc1_nodes_ = 3;
    BatchSingleNodeClusterTests::SetUp();
  }
};

/**
 * Perform batch execution using a prepared statement and validate the inserts
 *
 * This test will perform a batch insert using a prepared statement and ensure
 * the inserts were completed against a single node cluster.
 *
 * @test_category queries:batch
 * @test_category queries:prepared
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted via batch and validated
 */
CASSANDRA_INTEGRATION_TEST_F(BatchSingleNodeClusterTests, Prepared) {
  CHECK_FAILURE;

  // Create a prepared and batch statement for the inserts
  Prepared prepared_insert = session_.prepare(insert_query_);
  Batch batch(CASS_BATCH_TYPE_LOGGED);

  // Add multiple inserts into the batch statement
  int number_of_rows = 4;
  for (int i = 0; i < number_of_rows; ++i) {
    Statement statement = prepared_insert.bind();
    statement.bind<Integer>(0, Integer(i));
    statement.bind<Text>(1, Text(format_string("test data %d", i)));
    batch.add(statement);
  }

  // Execute the batch statement and validate the inserts
  session_.execute(batch);
  validate_inserts(number_of_rows);
}

/**
 * Perform batch execution using a simple statement and validate the inserts
 *
 * This test will perform a batch insert using a simple statement and ensure
 * the inserts were completed against a single node cluster.
 *
 * @test_category queries:batch
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted via batch and validated
 */
CASSANDRA_INTEGRATION_TEST_F(BatchSingleNodeClusterTests, Simple) {
  CHECK_FAILURE;

  // Create a batch statement for the inserts
  Batch batch(CASS_BATCH_TYPE_LOGGED);

  // Add multiple inserts into the batch statement
  int number_of_rows = 4;
  for (int i = 0; i < number_of_rows; ++i) {
    Statement statement(insert_query_, 2);
    statement.bind<Integer>(0, Integer(i));
    statement.bind<Text>(1, Text(format_string("test data %d", i)));
    batch.add(statement);
  }

  // Execute the batch statement and validate the inserts
  session_.execute(batch);
  validate_inserts(number_of_rows);
}

/**
 * Perform batch execution using a mixed statements and validate the inserts
 *
 * This test will perform a batch insert using a prepared and simple statements
 * while ensuring the inserts were completed against a single node cluster.
 *
 * @test_category queries:batch
 * @test_category queries:prepared
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted via batch and validated
 */
CASSANDRA_INTEGRATION_TEST_F(BatchSingleNodeClusterTests, MixedPreparedAndSimple) {
  CHECK_FAILURE;

  // Create a prepared and batch statement for the inserts
  Prepared prepared_insert = session_.prepare(insert_query_);
  Batch batch(CASS_BATCH_TYPE_LOGGED);

  // Add multiple inserts into the batch statement
  /* Batch statement default sizes were reduced in Cassandra v2.2.0+)
   * see: https://issues.apache.org/jira/browse/CASSANDRA-8011
   */
  int number_of_rows = (server_version_ >= "2.2.0" ? 100 : 1000);
  for (int i = 0; i < number_of_rows; ++i) {
    // Create either a prepared or simple bound statement
    Statement statement;
    if (i % 2 == 0) {
      statement = prepared_insert.bind();
    } else {
      statement = Statement(insert_query_, 2);
    }

    // Bind the values and add to the batch
    statement.bind<Integer>(0, Integer(i));
    statement.bind<Text>(1, Text(format_string("test data %d", i)));
    statement.set_consistency(CASS_CONSISTENCY_QUORUM);
    batch.add(statement);
  }

  // Execute the batch statement and validate the inserts
  session_.execute(batch);
  validate_inserts(number_of_rows);
}

/**
 * Perform batch execution using an invalid insert query
 *
 * This test will perform a batch insert using a statement with an invalid
 * statement contained in a batch against a single node cluster.
 *
 * NOTE: This test uses an insert statement against a counter which is invalid
 *       for logged batches
 *
 * @test_category queries:batch
 * @test_category queries:prepared
 * @since core:1.0.0
 * @expected_result Server will return and invalid query response
 */
CASSANDRA_INTEGRATION_TEST_F(BatchCounterSingleNodeClusterTests, InvalidBatchType) {
  CHECK_FAILURE;

  // Create a batch statement for the insert statement
  Batch batch(CASS_BATCH_TYPE_LOGGED);

  // Create and add the insert statement
  Statement statement(insert_query_, 2);
  statement.bind<Integer>(
      0, Integer(37)); // Attempt to insert a counter value inside a batch statement
  statement.bind<Counter>(1, Counter(37));
  batch.add(statement);

  // Execute the batch statement and verify the server response
  Result result = session_.execute(batch, false);
  ASSERT_EQ(CASS_ERROR_SERVER_INVALID_QUERY,
            result.error_code()); // Cannot include a counter statement in a logged batch
}

/**
 * Perform batch execution using a mixed statements and validate the inserts
 *
 * This test will perform a batch insert using a prepared and simple statements
 * while ensuring the inserts were completed against a three node cluster.
 *
 * @test_category queries:batch
 * @test_category queries:prepared
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted via batch and validated
 */
CASSANDRA_INTEGRATION_TEST_F(BatchCounterThreeNodeClusterTests, MixedPreparedAndSimple) {
  CHECK_FAILURE;

  // Create a prepared and batch statement for the inserts
  Prepared prepared_insert = session_.prepare(update_query_);
  Batch batch(CASS_BATCH_TYPE_COUNTER);

  // Add multiple counter updates into the batch statement
  /* Batch statement default sizes were reduced in Cassandra v2.2.0+)
   * see: https://issues.apache.org/jira/browse/CASSANDRA-8011
   */
  size_t number_of_rows = (server_version_ >= "2.2.0" ? 100 : 1000);
  for (size_t i = 0; i < number_of_rows; ++i) {
    // Create either a prepared or simple bound statement
    Statement statement;
    if (i % 2 == 0) {
      statement = prepared_insert.bind();
    } else {
      statement = Statement(update_query_, 2);
    }

    // Bind the values and add to the batch
    statement.bind<Counter>(0, Counter(i));
    statement.bind<Integer>(1, Integer(i));
    batch.add(statement);
  }

  // Execute the batch statement
  session_.execute(batch);

  // Validate the updates
  Result result = session_.execute(default_select_all(), CASS_CONSISTENCY_QUORUM);
  ASSERT_EQ(number_of_rows, result.row_count());
  ASSERT_EQ(2u, result.column_count());
  Rows rows = result.rows();
  for (size_t i = 0; i < rows.row_count(); ++i) {
    // Get the current row and validate the values
    Row row = rows.next();
    Integer key = row.next().as<Integer>();
    Counter value = row.next().as<Counter>();
    ASSERT_EQ(key.value(), value.value());
  }
}
