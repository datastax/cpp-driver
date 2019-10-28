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
 * Basics integration tests; common operations
 */
class BasicsTests : public Integration {};

/**
 * Perform inserts and validate the timestamps from the server
 *
 * This test will perform multiple inserts using a simple statement and ensure
 * the timestamps between the inserts are valid and different based on a timed
 * tolerance against a single node cluster.
 *
 * @test_category queries:timestamp
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and timestamps are validated
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, Timestamps) {
  CHECK_FAILURE;

  // Create the table, insert and select queries for the test
  session_.execute(
      format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
  std::string insert_query =
      format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "?", "?");
  Statement select_query = Statement("SELECT WRITETIME (value) FROM " + table_name_);

  // Insert a value and get the timestamp from the server
  Statement statement(insert_query, 2);
  statement.bind<Integer>(0, Integer(0));
  statement.bind<Integer>(1, Integer(1));
  session_.execute(statement);
  Result result = session_.execute(select_query);
  ASSERT_EQ(1u, result.row_count());
  ASSERT_EQ(1u, result.column_count());
  BigInteger timestamp_1 = result.first_row().next().as<BigInteger>();

  // Wait five seconds before performing next insert and timestamp retrieval
  unsigned int pause_duration = 5000;
  msleep(pause_duration);

  // Overwrite the value and get the timestamp from the server
  statement = Statement(insert_query, 2);
  statement.bind<Integer>(0, Integer(0));
  statement.bind<Integer>(1, Integer(2)); // Overwritten value
  session_.execute(statement);
  result = session_.execute(select_query);
  ASSERT_EQ(1u, result.row_count());
  ASSERT_EQ(1u, result.column_count());
  BigInteger timestamp_2 = result.first_row().next().as<BigInteger>();

  // Validate the timestamps
  ASSERT_NE(timestamp_1, timestamp_2);
  ASSERT_LT(timestamp_2 - timestamp_1 - BigInteger(pause_duration * 1000), BigInteger(100000));
}

/**
 * Perform inserts and validate counter value
 *
 * This test will perform multiple update/upserts using a simple statement and
 * ensure the counters are properly updated against a single node cluster.
 *
 * @test_category queries:counter
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and counters are validated
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, Counters) {
  CHECK_FAILURE;

  // Create the table and update/upsert queries for the test
  session_.execute(
      format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "counter"));
  std::string update_query = "UPDATE " + table_name_ + " SET value=value %s ? WHERE key=0";

  // Perform multiple upserts against the counter value
  for (int step = 0; step < 100; ++step) {
    Statement statement(format_string(update_query.c_str(), (step % 2 == 0 ? "-" : "+")), 1);
    statement.bind<Counter>(0, Counter(step));
    session_.execute(statement);
  }

  // Select the columns from the and ensure the counter and rows
  Statement statement(default_select_all());
  Result result = session_.execute(statement);
  ASSERT_EQ(1u, result.row_count());
  ASSERT_GT(result.column_count(), 0u);
  Row row = result.first_row();
  ASSERT_EQ(Integer(0), row.next().as<Integer>());
  ASSERT_EQ(Counter(50), row.next().as<Counter>());
}

/**
 * Perform inserts and validate rows inserted is equal to rows selected
 *
 * This test will perform multiple inserts using a simple statement and ensure
 * that the number of rows inserted is equal to the number of rows selected
 * (along with data validation) against a single node cluster.
 *
 * @test_category queries:basic
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and counters are validated
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, RowsInRowsOut) {
  CHECK_FAILURE;

  // Create the table, insert, and select statements for the test
  session_.execute("CREATE TABLE " + table_name_ +
                   " (key bigint PRIMARY KEY, "
                   "value_1 bigint, value_2 bigint, value_3 bigint)");
  Statement insert_statement("INSERT INTO " + table_name_ +
                                 " (key, value_1, value_2, value_3) "
                                 "VALUES (?, ?, ?, ?)",
                             4);
  Statement select_statement("SELECT key, value_1, value_2, value_3 FROM " + table_name_ +
                             " LIMIT 1000");

  // Create multiple rows with varying data
  for (int i = 0; i < 1000; ++i) {
    insert_statement.bind<BigInteger>(0, BigInteger(i));
    insert_statement.bind<BigInteger>(1, BigInteger(i + 1));
    insert_statement.bind<BigInteger>(2, BigInteger(i + 2));
    insert_statement.bind<BigInteger>(3, BigInteger(i + 3));
    session_.execute(insert_statement);
  }

  // Validate the rows inserted are the rows selected
  Result result = session_.execute(select_statement);
  ASSERT_EQ(1000u, result.row_count());
  ASSERT_EQ(4u, result.column_count());
  Rows rows = result.rows();
  int number_of_rows = 0;
  for (size_t i = 0; i < rows.row_count(); ++i) {
    Row row = rows.next();
    BigInteger key = row.next().as<BigInteger>();
    ASSERT_EQ(key + BigInteger(1), row.next().as<BigInteger>()); // value_1
    ASSERT_EQ(key + BigInteger(2), row.next().as<BigInteger>()); // value_2
    ASSERT_EQ(key + BigInteger(3), row.next().as<BigInteger>()); // value_3
    ++number_of_rows;
  }

  // Ensure that all the rows were read
  ASSERT_EQ(1000, number_of_rows);
}

/**
 * Perform insert and validate columns by name
 *
 * This test will perform an insert using a simple statement and ensure
 * that the number of rows inserted is equal to the number of rows selected
 * (along with data validation) against a single node cluster.
 *
 * @test_category queries:basic
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and counters are validated
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, ColumnNames) {
  CHECK_FAILURE;

  // Create the table for the test
  session_.execute("CREATE TABLE " + table_name_ +
                   " (key bigint PRIMARY KEY, "
                   "value_1 text, value_2 int, value_3 bigint, value_4 float)");

  // Validate the column names
  Result result = session_.execute(default_select_all());
  ASSERT_EQ(0u, result.row_count());
  ASSERT_EQ(5u, result.column_count());
  std::vector<std::string> column_names = result.column_names();
  ASSERT_EQ("key", column_names[0]);
  ASSERT_EQ("value_1", column_names[1]);
  ASSERT_EQ("value_2", column_names[2]);
  ASSERT_EQ("value_3", column_names[3]);
  ASSERT_EQ("value_4", column_names[4]);
}

/**
 * Perform statement executions and ensure empty results
 *
 * This test will perform varying statement type executions using a simple
 * statement and ensure the result set is empty (row count == 0) when executing
 * these statement types that do not return values from the server.
 *
 * NOTE: This is run against a single node cluster.
 *
 * @test_category queries:basic
 * @since core:1.0.0-rc1
 * @expected_result Statement execution queries will not contain value in result
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, EmptyResults) {
  CHECK_FAILURE;

  // Create the table
  Result result = session_.execute(
      format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
  ASSERT_TRUE(result.is_empty());

  // Insert data into the table
  result = session_.execute(
      format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "0", "0"));
  ASSERT_TRUE(result.is_empty());

  // Delete data from the table
  result = session_.execute(format_string(CASSANDRA_DELETE_ROW_FORMAT, table_name_.c_str(), "0"));
  ASSERT_TRUE(result.is_empty());

  // Select data from the table (all rows have been deleted)
  result = session_.execute(default_select_all());
  ASSERT_TRUE(result.is_empty());
}

/**
 * Perform insert and ensure UNSET parameter is set (Cassandra v2.2+)
 *
 * This test will perform an insert using a simple statement with set and unset
 * bound values while ensuring the proper error is returned for Cassandra <=
 * v2.1 against a single node cluster. The known values will be validated to
 * ensure they were properly inserted.
 *
 * @test_category error_codes
 * @test_category queries:basic
 * @since core:2.2.0-beta1
 * @expected_result Known value inserts will complete and validated
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, UnsetParameters) {
  CHECK_FAILURE;

  // Create the table, insert a known value, and create insert statement for the test
  session_.execute(
      format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
  session_.execute(format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "0", "1"));
  Prepared insert_prepared = session_.prepare(
      format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "?", "?"));

  // Bind a single value and leave one unset
  Statement insert_statement = insert_prepared.bind();
  insert_statement.bind<Integer>(0, Integer(0));

  // Execute the insert statement and validate the error code
  Result result = session_.execute(insert_statement, false);
  if (server_version_ >= "2.2.0") {
    // Cassandra v2.2+ uses the value UNSET; making this a no-op
    ASSERT_EQ(CASS_OK, result.error_code());
  } else {
    ASSERT_EQ(CASS_ERROR_LIB_PARAMETER_UNSET, result.error_code());
  }

  // Validate known values from previous insert
  result = session_.execute(default_select_all());
  ASSERT_EQ(1u, result.row_count());
  ASSERT_EQ(2u, result.column_count());
  Row row = result.first_row();
  ASSERT_EQ(Integer(0), row.next().as<Integer>());
  ASSERT_EQ(Integer(1), row.next().as<Integer>());
}

/**
 * Perform insert against a blob data type using a string (char*)
 *
 * This test will perform an insert using a simple statement by binding a string
 * to a blob data type and validate the result against a single node cluster.
 *
 * NOTE: Previous versions of the driver allowed for this
 *
 * @test_category data_types:primitive
 * @test_category queries:basic
 * @since core:2.3.0
 * @expected_result String will be bound/inserted into blob  and values will be
 *                 validated
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, BindBlobAsString) {
  CHECK_FAILURE;

  // Create the table, prepared and  insert statement for the test
  session_.execute(
      format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "blob"));
  // Prepared needed to validate bind type information
  Prepared insert_prepared = session_.prepare(
      format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "?", "?"));
  Statement insert_statement = insert_prepared.bind();

  // Bind and insert the data into the table
  Blob data("blob_string");
  insert_statement.bind<Integer>(0, Integer(0));
  insert_statement.bind<Blob>(1, data);
  session_.execute(insert_statement);

  // Validate the inserted data
  Result result = session_.execute(default_select_all());
  ASSERT_EQ(1u, result.row_count());
  ASSERT_EQ(2u, result.column_count());
  Row row = result.first_row();
  ASSERT_EQ(Integer(0), row.next().as<Integer>());
  ASSERT_EQ(data, row.next().as<Blob>());
}

/**
 * Perform select against a table using COMPACT STORAGE in compatibility mode.
 *
 * This this will perform querying a table with COMPACT STORAGE applied and
 * queried using a separate session where the NO_COMPACT STARTUP_OPTIONS was
 * supplied.
 *
 * NOTE: This test can only be run using Apache Cassandra versions where
 *       COMPACT STORAGE is still applicable and supports the NO_COMPACT
 *       STARTUP OPTIONS (e.g. v3.0.16+, v3.11.2+; but must be less than v4.x)
 *
 * @jira_ticket CPP-578
 * @test_category connection
 * @since core:2.9.0
 * @cassandra_version 3.0.16
 * @cassandra_version 3.11.2
 * @expected_result Values inserted into the COMPACT STORAGE table will be
 *                  selectable and contain additional metadata (columns).
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, NoCompactEnabledConnection) {
  CHECK_FAILURE;
  CHECK_VERSION(3.0.16);
  CHECK_VERSION(3.11.2);
  CCM::CassVersion cass_version = server_version_;
  if (!Options::is_cassandra()) {
    if (server_version_ >= "6.0.0") {
      SKIP_TEST("Unsupported for DataStax Enterprise Version "
                << server_version_.to_string()
                << ": Apache Cassandra server version must be used and less than"
                << " v4.0.0 and either 3.0.16+ or 3.11.2+ in order to execute");
    }
    cass_version = static_cast<CCM::DseVersion>(cass_version).get_cass_version();
  }
  if (cass_version >= "4.0.0") {
    SKIP_TEST("Unsupported for Apache Cassandra Version "
              << cass_version.to_string()
              << ": Server version must be less than v4.0.0 and either 3.0.16+"
              << " or 3.11.2+ in order to execute");
  }

  // Create a session where the NO_COMPACT option is set
  Session no_compact_session = default_cluster().with_no_compact().connect(default_keyspace());

  // Create the table and insert data using the NO_COMPACT session
  no_compact_session.execute(format_string(
      "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH COMPACT STORAGE", table_name_.c_str()));
  no_compact_session.execute(
      format_string("INSERT INTO %s (k, v) VALUES(%s, %s)", table_name_.c_str(), "1", "1"));
  no_compact_session.execute(
      format_string("INSERT INTO %s (k, v) VALUES(%s, %s)", table_name_.c_str(), "2", "2"));
  no_compact_session.execute(
      format_string("INSERT INTO %s (k, v) VALUES(%s, %s)", table_name_.c_str(), "3", "3"));

  // Validate the default session with compact storage enabled
  Result result = session_.execute(default_select_all());
  ASSERT_EQ(3u, result.row_count());
  ASSERT_EQ(2u, result.column_count());
  Rows rows = result.rows();
  for (size_t i = 0; i < rows.row_count(); ++i) {
    Row row = rows.next();
    Integer k = row.next().as<Integer>();
    Integer v = row.next().as<Integer>();
    ASSERT_EQ(k, Integer(i + 1));
    ASSERT_EQ(v, Integer(i + 1));
  }

  // Validate the default session with compact storage disabled (NO_COMPACT)
  result = no_compact_session.execute(default_select_all());
  ASSERT_EQ(3u, result.row_count());
  ASSERT_EQ(4u, result.column_count()); // Should contain extra columns (column and value)
  std::vector<std::string> column_names = result.column_names();
  ASSERT_EQ("k", column_names[0]);
  ASSERT_EQ("column1", column_names[1]);
  ASSERT_EQ("v", column_names[2]);
  ASSERT_EQ("value", column_names[3]);
  rows = result.rows();
  for (size_t i = 0; i < rows.row_count(); ++i) {
    Row row = rows.next();
    Integer k = row.next().as<Integer>();
    ASSERT_EQ(k, Integer(i + 1));
    ASSERT_TRUE(row.next().as<Varchar>().is_null());
    Integer v = row.next().as<Integer>();
    ASSERT_EQ(v, Integer(i + 1));
    ASSERT_TRUE(row.next().as<Blob>().is_null());
  }
}

static void on_future_callback_connect_close(CassFuture* future, void* data) {
  bool* is_success = static_cast<bool*>(data);
  *is_success = cass_future_error_code(future) == CASS_OK;
}

/**
 * Verify a future callback is called when connecting a session.
 *
 * @expected_result The flag is set correctly inside the connect future callback.
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, FutureCallbackConnect) {
  CHECK_FAILURE;

  Session session;
  Future future = default_cluster().connect_async(session);

  bool is_success = false;
  cass_future_set_callback(future.get(), on_future_callback_connect_close, &is_success);

  future.wait();

  EXPECT_TRUE(is_success);
}

/**
 * Verify a future callback is called when closing a session.
 *
 * @expected_result The flag is set correctly inside the close future callback.
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, FutureCallbackClose) {
  CHECK_FAILURE;

  Session session = default_cluster().connect();

  Future future = session.close_async();

  bool is_success = false;
  cass_future_set_callback(future.get(), on_future_callback_connect_close, &is_success);

  future.wait();

  EXPECT_TRUE(is_success);
}

static void on_future_callback_result(CassFuture* future, void* data) {
  const CassResult** result = static_cast<const CassResult**>(data);
  *result = cass_future_get_result(future);
}

/**
 * Verify a future callback is called with query results.
 *
 * @expected_result Result correctly returned in the future callback.
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, FutureCallbackResult) {
  CHECK_FAILURE;

  Future future = session_.execute_async(SELECT_ALL_SYSTEM_LOCAL_CQL);

  const CassResult* result = NULL;
  cass_future_set_callback(future.get(), on_future_callback_result, &result);

  future.wait();

  ASSERT_TRUE(result != NULL);
  EXPECT_EQ(1u, Result(result).row_count());
}

/**
 * Verify a future callback is called correctly after the query results have been set.
 *
 * @expected_result Result correctly returned in the future callback.
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, FutureCallbackAfterSet) {
  CHECK_FAILURE;

  Future future = session_.execute_async(SELECT_ALL_SYSTEM_LOCAL_CQL);

  future.wait(); // Wait for result before setting the callback

  const CassResult* result = NULL;
  // Callback should be called immediately with the already retrieved result.
  cass_future_set_callback(future.get(), on_future_callback_result, &result);

  ASSERT_TRUE(result != NULL);
  EXPECT_EQ(1u, Result(result).row_count());
}

/**
 * Verify that paging and paging using the token properly returns rows.
 *
 * @expected_result Expect 10 pages of 10 rows using both paging methods.
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, Paging) {
  CHECK_FAILURE;

  session_.execute(
      format_string(CASSANDRA_COMPOSITE_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));

  { // Insert rows
    Statement insert_statement(format_string(CASSANDRA_COMPOSITE_KEY_VALUE_INSERT_FORMAT,
                                             table_name_.c_str(), "0", "?", "?"),
                               2);

    for (int i = 0; i < 100; ++i) {
      insert_statement.bind<TimeUuid>(0, uuid_generator_.generate_timeuuid());
      insert_statement.bind<Integer>(1, Integer(i));
      session_.execute(insert_statement);
    }
  }

  { // Page through inserted rows
    Statement select_statement(
        format_string(CASSANDRA_COMPOSITE_SELECT_VALUE_FORMAT, table_name_.c_str(), "0"));
    select_statement.set_paging_size(10);

    size_t num_pages = 0;

    while (true) {
      Result result = session_.execute(select_statement);
      if (!result.has_more_pages()) break;
      EXPECT_EQ(10u, result.row_count());
      num_pages++;
      select_statement.set_paging_state(result);
    }

    EXPECT_EQ(10u, num_pages);
  }

  { // Page through inserted rows using page state token
    Statement select_statement(
        format_string(CASSANDRA_COMPOSITE_SELECT_VALUE_FORMAT, table_name_.c_str(), "0"));
    select_statement.set_paging_size(10);

    size_t num_pages = 0;

    while (true) {
      Result result = session_.execute(select_statement);
      if (!result.has_more_pages()) break;
      EXPECT_EQ(10u, result.row_count());
      num_pages++;
      std::string token = result.paging_state_token();
      EXPECT_FALSE(token.empty());
      select_statement.set_paging_state_token(token);
    }

    EXPECT_EQ(10u, num_pages);
  }
}

/**
 * Verify that a query of an empty table returns the correct paging state.
 *
 * @expected_result The result should signal no more pages and have an empty paging token.
 */
CASSANDRA_INTEGRATION_TEST_F(BasicsTests, PagingEmpty) {
  CHECK_FAILURE;

  session_.execute(
      format_string(CASSANDRA_COMPOSITE_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));

  // No rows inserted

  Statement select_statement(
      format_string(CASSANDRA_COMPOSITE_SELECT_VALUE_FORMAT, table_name_.c_str(), "0"));
  select_statement.set_paging_size(10);

  Result result = session_.execute(select_statement);

  EXPECT_FALSE(result.has_more_pages());

  std::string token = result.paging_state_token();
  EXPECT_TRUE(token.empty());
}
