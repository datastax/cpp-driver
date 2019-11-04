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
 * Server-side warnings and errors integration tests
 */
class ServerSideFailureTests : public Integration {};

/**
 * Server-side errors integration tests that require three nodes.
 */
class ServerSideFailureThreeNodeTests : public Integration {
public:
  ServerSideFailureThreeNodeTests() {
    number_dc1_nodes_ = 3;
    replication_factor_ = 3;
  }

  void SetUp() {
    Integration::SetUp();
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "double"));
  }

  void validate_write_response(Session session, CassError expected_error_code) {
    Statement insert_statement(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "2", "2.71"));
    insert_statement.set_consistency(CASS_CONSISTENCY_LOCAL_QUORUM);
    insert_statement.set_host("127.0.0.1", 9042);
    Result result = session.execute(insert_statement, false);
    validate_response(result, expected_error_code);
  }

  void validate_read_response(Session session, CassError expected_error_code) {
    Statement select_statement(default_select_all());
    select_statement.set_consistency(CASS_CONSISTENCY_LOCAL_QUORUM);
    select_statement.set_host("127.0.0.1", 9042);
    Result result = session.execute(select_statement, false);
    validate_response(result, expected_error_code);
  }

private:
  void validate_response(Result result, CassError expected_error_code) {
    EXPECT_EQ(expected_error_code, result.error_code());

    ErrorResult error_result = result.error_result();
    ASSERT_TRUE(error_result);
    EXPECT_EQ(expected_error_code, error_result.error_code());
    EXPECT_EQ(CASS_CONSISTENCY_LOCAL_QUORUM, error_result.consistency());
    EXPECT_EQ(1, error_result.responses_received());
    EXPECT_EQ(2, error_result.responses_required());
    EXPECT_TRUE(expected_error_code != CASS_ERROR_SERVER_READ_TIMEOUT ||
                error_result.data_present());
  }
};

/**
 * Validate that server-side warnings are logged.
 *
 * @since 2.2.0-beta
 * @jira_ticket CPP-292
 * @cassandra_version 2.2.x
 */
CASSANDRA_INTEGRATION_TEST_F(ServerSideFailureTests, Warning) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2);

  logger_.add_critera("Server-side warning: Aggregation query used without partition key");
  session_.execute("SELECT sum(gossip_generation) FROM system.local");
  EXPECT_EQ(logger_.count(), 1u);
}

/**
 * Validate UDF Function_failures are returned from Cassandra
 *
 * Create a function that will throw an exception when invoked; ensure the
 * Function_failure is returned from Cassandra.
 *
 * @since 2.2.0-beta
 * @jira_ticket CPP-294
 * @cassandra_version 2.2.x
 */
CASSANDRA_INTEGRATION_TEST_F(ServerSideFailureTests, ErrorFunctionFailure) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2);

  // Create the table and associated failing function
  session_.execute("CREATE TABLE server_function_failures (id int PRIMARY KEY, value double)");
  session_.execute("CREATE FUNCTION function_failure(value double) RETURNS NULL ON NULL INPUT "
                   "RETURNS double LANGUAGE java "
                   "AS 'throw new RuntimeException(\"failure\");'");

  // Bind and insert values into Cassandra
  session_.execute("INSERT INTO server_function_failures(id, value) VALUES (1, 3.14)");

  // Execute the failing function
  Result result = session_.execute(
      Statement("SELECT function_failure(value) FROM server_function_failures WHERE id = 1"),
      false);
  EXPECT_EQ(CASS_ERROR_SERVER_FUNCTION_FAILURE, result.error_code());

  ErrorResult error_result = result.error_result();
  ASSERT_TRUE(error_result);
  EXPECT_EQ(CASS_ERROR_SERVER_FUNCTION_FAILURE, error_result.error_code());
  EXPECT_EQ(keyspace_name_, error_result.keyspace());
  EXPECT_EQ("function_failure", error_result.function());
  EXPECT_EQ(1u, error_result.num_arg_types());
  EXPECT_EQ("double", error_result.arg_type(0));
}

/**
 * Validate already exists failures are returned when creating the same table twice.
 */
CASSANDRA_INTEGRATION_TEST_F(ServerSideFailureTests, ErrorTableAlreadyExists) {
  CHECK_FAILURE;

  std::string create_table_query =
      "CREATE TABLE already_exists_table (id int PRIMARY KEY, value double)";
  session_.execute(create_table_query);
  Result result = session_.execute(Statement(create_table_query), false);
  EXPECT_EQ(CASS_ERROR_SERVER_ALREADY_EXISTS, result.error_code());

  ErrorResult error_result = result.error_result();
  ASSERT_TRUE(error_result);
  EXPECT_EQ(CASS_ERROR_SERVER_ALREADY_EXISTS, error_result.error_code());
  EXPECT_EQ(keyspace_name_, error_result.keyspace());
  EXPECT_EQ("already_exists_table", error_result.table());
}

/**
 * Validate a failure is returned when creating the same function twice.
 *
 * @cassandra_version 2.2.x
 */
CASSANDRA_INTEGRATION_TEST_F(ServerSideFailureTests, ErrorFunctionAlreadyExists) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2);

  std::string create_function_query =
      "CREATE FUNCTION already_exists_function(value double) RETURNS NULL ON NULL INPUT "
      "RETURNS double LANGUAGE java "
      "AS 'return 3.14;'";
  session_.execute(create_function_query);
  Result result = session_.execute(Statement(create_function_query), false);
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, result.error_code());
  EXPECT_TRUE(result.error_message().find("(double) -> double already exists") !=
              std::string::npos);

  ErrorResult error_result = result.error_result();
  ASSERT_TRUE(error_result);
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, error_result.error_code());
}

/**
 * Validate read/write timeout server-side failures and error result data.
 */
CASSANDRA_INTEGRATION_TEST_F(ServerSideFailureThreeNodeTests, ErrorReadWriteTimeout) {
  Session session =
      default_cluster().with_retry_policy(FallthroughRetryPolicy()).connect(keyspace_name_);

  pause_node(2);
  pause_node(3);

  validate_write_response(session, CASS_ERROR_SERVER_WRITE_TIMEOUT);
  validate_read_response(session, CASS_ERROR_SERVER_READ_TIMEOUT);
}

/**
 * Validate read/write unavailable server-side failures and error result data.
 */
CASSANDRA_INTEGRATION_TEST_F(ServerSideFailureThreeNodeTests, ErrorUnavailable) {
  Session session =
      default_cluster().with_retry_policy(FallthroughRetryPolicy()).connect(keyspace_name_);

  stop_node(2);
  stop_node(3);

  validate_write_response(session, CASS_ERROR_SERVER_UNAVAILABLE);
  validate_read_response(session, CASS_ERROR_SERVER_UNAVAILABLE);
}
