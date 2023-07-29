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

#define CREATE_KEYSPACE                                                                          \
  "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class' : 'NetworkTopologyStrategy',  " \
  "'dc1' : 1 }"

#define DROP_KEYSPACE "DROP KEYSPACE %s"

/**
 * Set keyspace integration tests
 */
class SetKeyspaceTests : public Integration {
public:
  void SetUp() {
    Integration::SetUp();
    keyspace_name_other_ = keyspace_name_ + "__";
    maybe_shrink_name(keyspace_name_other_);
    session_.execute(format_string(CREATE_KEYSPACE, keyspace_name_other_.c_str()));

    create_table();
  }

  void TearDown() {
    session_.execute(format_string(DROP_KEYSPACE, keyspace_name_other_.c_str()));
    Integration::TearDown();
  }

  const std::string& keyspace_name_other() const { return keyspace_name_other_; }
  const std::string& keyspace_name() const { return keyspace_name_; }

  void create_table() {
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "1", "11"));
  }

  /**
   * Run a SELECT query using a simple statement and a session connected with
   * the provided keyspace. The result of the query is validated.
   *
   * @param session_keyspace The session's connected keyspace
   */
  void query_with_keyspace(const std::string& session_keyspace) {
    Session session = default_cluster().connect(session_keyspace);

    Statement statement(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "1"));
    statement.set_keyspace(keyspace_name());

    Result result = session.execute(statement);
    ASSERT_TRUE(result);
    ASSERT_GT(result.row_count(), 0u);
    EXPECT_EQ(11, result.first_row().column_by_name<Integer>("value").value());
  }

  /**
   * Run a SELECT query using a prepared statement and a session connected with
   * the provided keyspace. The result of the query if validated.
   *
   * @param session_keyspace The session's connected keyspace
   */
  void prepared_query_with_keyspace(const std::string& session_keyspace) {
    Session session = default_cluster().connect(session_keyspace);

    Statement statement(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "1"));
    statement.set_keyspace(keyspace_name());

    Prepared prepared = session.prepare_from_existing(statement);
    ASSERT_TRUE(prepared);

    Result result = session.execute(prepared.bind());
    ASSERT_TRUE(result);
    ASSERT_GT(result.row_count(), 0u);
    EXPECT_EQ(11, result.first_row().column_by_name<Integer>("value").value());
  }

  /**
   * Run INSERT statements using a batch and as session connected with the
   * provided keyspace. A SELECT query is used to validate the results of
   * the batch.
   *
   * @param session_keyspace The session's connected keyspace
   */
  void batch_query_with_keyspace(const std::string& session_keyspace) {
    Session session = default_cluster().connect(session_keyspace);
    session.execute(create_batch_with_keyspace_name());
    validate_batch_results();
  }

  /**
   * Run a SELECT query and validate the results of a batch test.
   */
  void validate_batch_results() {
    Result result = session_.execute(default_select_all());
    ASSERT_TRUE(result);
    EXPECT_EQ(3u, result.row_count());
    EXPECT_EQ(2u, result.column_count());

    std::map<int, int> actual, expected;

    expected[1] = 11;
    expected[2] = 22;
    expected[3] = 33;

    Rows rows = result.rows();

    for (size_t i = 0; i < rows.row_count(); ++i) {
      Row row = rows.next();
      actual[row.column<Integer>(0).value()] = row.column<Integer>(1).value();
    }

    EXPECT_EQ(expected, actual);
  }

  /**
   * Create a batch with a batch-level keyspace set.
   *
   * @return The batch object
   */
  Batch create_batch_with_keyspace_name() {
    Batch batch;
    batch.set_keyspace(keyspace_name());
    batch.add(Statement(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "2", "22")));
    batch.add(Statement(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "3", "33")));
    return batch;
  }

private:
  std::string keyspace_name_other_;
};

/**
 * Verify that older protocols don't attempt to send a statement's keyspace.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, QueryNotSupported) {
  CHECK_FAILURE;

  Session session = default_cluster()
                        .with_protocol_version(CASS_PROTOCOL_VERSION_V4)
                        .connect();

  Statement statement(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "1"));
  statement.set_keyspace(keyspace_name());

  Result result = session.execute(statement, false);
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, result.error_code());
}

/**
 * Verify that a simple statement's keyspace is used when no session keyspace
 * is set.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, QueryWithNoSessionKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  query_with_keyspace("");
}

/**
 * Verify that a simple statement's keyspace is used when a different session
 * keyspace is set.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, QueryWithDifferentSessionKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  query_with_keyspace(keyspace_name_other());
}

/**
 * Verify that a simple statement's keyspace is used when the same session
 * keyspace is set.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, QueryWithSameSessionKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  query_with_keyspace(keyspace_name());
}

/**
 * Verify that older protocols don't attempt to send a prepared statement's
 * keyspace.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, PreparedNotSupported) {
  CHECK_FAILURE;

  Session session = default_cluster()
                        .with_protocol_version(CASS_PROTOCOL_VERSION_V4)
                        .connect();

  Statement statement(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "1"));
  statement.set_keyspace(keyspace_name());

  Prepared prepared = session.prepare_from_existing(statement, false);
  EXPECT_FALSE(prepared);
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, prepared.error_code());
}

/**
 * Verify that a re-prepare (as a result of a UNPREPARED response) correctly
 * prepares the statement with the original keyspace.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, ReprepareWithSameKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  Session session = default_cluster().connect();

  Statement statement(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "1"));
  statement.set_keyspace(keyspace_name());

  Prepared prepared = session.prepare_from_existing(statement, false);
  ASSERT_TRUE(prepared);

  session.execute("TRUNCATE system.prepared_statements"); // Required for 3.10+ (CASSANDRA-8831)

  drop_table(table_name_);
  create_table();

  Result result = session.execute(prepared.bind());
  ASSERT_TRUE(result);
  ASSERT_GT(result.row_count(), 0u);
  EXPECT_EQ(11, result.first_row().column_by_name<Integer>("value").value());
}

/**
 * Verify that a prepared statement's keyspace is used when no session keyspace
 * is set.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, PreparedWithNoSessionKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  prepared_query_with_keyspace("");
}

/**
 * Verify that a prepared statement's keyspace is used when a different keyspace
 * is set.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, PreparedWithDifferentSessionKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  prepared_query_with_keyspace(keyspace_name_other());
}

/**
 * Verify that a prepared statement's keyspace is used when the same keyspace
 * is set.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, PreparedWithSameSessionKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  prepared_query_with_keyspace(keyspace_name());
}

/**
 * Verify that a batch determines its keyspace from one the first simple
 * statement with a non-empty keyspace.
 *
 * @since 2.8
 * @test_category basic
 *
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, BatchWithKeyspaceFromSimple) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  Session session = default_cluster().connect();

  Batch batch;
  batch.add(
      Statement(format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "2", "22")));

  { // Add a simple statement with the keyspace set
    Statement statement(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "3", "33"));
    statement.set_keyspace(keyspace_name());
    batch.add(statement);
  }

  session.execute(batch);
  validate_batch_results();
}

/**
 * Verify that a batch determines its keyspace from one the first prepared
 * statement with a non-empty keyspace.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, BatchWithKeyspaceFromPrepared) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  Session session = default_cluster().connect();

  Batch batch;

  batch.add(
      Statement(format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "2", "22")));

  { // Create prepared statement with the keyspace set
    Statement statement(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "3", "33"));
    statement.set_keyspace(keyspace_name());
    batch.add(session.prepare_from_existing(statement).bind());
  }

  session.execute(batch);
  validate_batch_results();
}

/**
 * Verify that the batch-level keyspace is not sent when using older protocols.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, BatchNotSupported) {
  CHECK_FAILURE;

  Session session = default_cluster()
                        .with_protocol_version(CASS_PROTOCOL_VERSION_V4)
                        .connect();

  Result result = session.execute(create_batch_with_keyspace_name(), false);
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, result.error_code());
}

/**
 * Verify that a batch's keyspace is used when no session keyspace is set.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, BatchWithNoSessionKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  batch_query_with_keyspace("");
}

/**
 * Verify that a batch's keyspace is used when a different session keyspace
 * is set.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, BatchWithDifferentSessionKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  batch_query_with_keyspace(keyspace_name_other());
}

/**
 * Verify that a batch's keyspace is used when then same session keyspace
 * is set.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(SetKeyspaceTests, BatchWithSameSessionKeyspace) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  batch_query_with_keyspace(keyspace_name());
}
