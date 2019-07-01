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

#define TABLE_FORMAT           \
  "CREATE TABLE %s ("          \
  "key timeuuid PRIMARY KEY, " \
  "a int, "                    \
  "b boolean, "                \
  "c text, "                   \
  "abc float, "                \
  "\"ABC\" float, "            \
  "\"aBc\" float"              \
  ")"
#define TABLE_BYTES_FORMAT     \
  "CREATE TABLE %s ("          \
  "key timeuuid PRIMARY KEY, " \
  "blobs blob, "               \
  "varints varint"             \
  ")"
#define INSERT_FORMAT "INSERT INTO %s (key, a, b, c) VALUES (?, ?, ?, ?)"
#define INSERT_CASE_SENSITIVE_FORMAT \
  "INSERT INTO %s "                  \
  "(key, abc, \"ABC\", \"aBc\") "    \
  "VALUES (?, ?, ?, ?)"
#define INSERT_ALL_FORMAT                  \
  "INSERT INTO %s "                        \
  "(key, a, b, c, abc, \"ABC\", \"aBc\") " \
  "VALUES (?, ?, ?, ?, ?, ?, ?)"
#define INSERT_BYTES_FORMAT "INSERT INTO %s (key, blobs, varints) VALUES (?, ?, ?)"

/**
 * By name integration tests
 */
class ByNameTests : public Integration {
public:
  virtual void SetUp() {
    // Call the parent setup function
    Integration::SetUp();

    // Create the table for the test
    session_.execute(format_string(TABLE_FORMAT, table_name_.c_str()));
  }

protected:
  /**
   * Insert and validate
   *
   * @param statement Insert statement to use (not case sensitive format)
   */
  void insert_and_validate(Statement statement) {
    // Insert values into the table by name
    TimeUuid key = uuid_generator_.generate_timeuuid();
    statement.bind<TimeUuid>("key", key);
    statement.bind<Integer>("a", Integer(9042));
    statement.bind<Boolean>("b", Boolean(true));
    statement.bind<Text>("c", Text("yyz"));
    session_.execute(statement);

    // Validate the inserts into the table
    Result result = session_.execute(default_select_all());
    ASSERT_EQ(1u, result.row_count());
    ASSERT_EQ(7u, result.column_count());
    Row row = result.first_row();
    ASSERT_EQ(key, row.column_by_name<TimeUuid>("key"));
    ASSERT_EQ(Integer(9042), row.column_by_name<Integer>("a"));
    ASSERT_EQ(Boolean(true), row.column_by_name<Boolean>("b"));
    ASSERT_EQ(Text("yyz"), row.column_by_name<Text>("c"));
  }

  /**
   * Insert and validate
   *
   * @param statement Insert statement to use (case sensitive format)
   */
  void insert_and_validate_case_sensitive(Statement statement) {
    // Insert values into the table by name
    TimeUuid key = uuid_generator_.generate_timeuuid();
    statement.bind<TimeUuid>("key", key);
    statement.bind<Float>("\"abc\"", Float(1.1f));
    statement.bind<Float>("\"ABC\"", Float(2.2f));
    statement.bind<Float>("\"aBc\"", Float(3.3f));
    session_.execute(statement);

    // Validate the inserts into the table
    Result result = session_.execute(default_select_all());
    ASSERT_EQ(1u, result.row_count());
    ASSERT_EQ(7u, result.column_count());
    Row row = result.first_row();
    ASSERT_EQ(key, row.column_by_name<TimeUuid>("key"));
    ASSERT_EQ(Float(1.1f), row.column_by_name<Float>("\"abc\""));
    ASSERT_EQ(Float(2.2f), row.column_by_name<Float>("\"ABC\""));
    ASSERT_EQ(Float(3.3f), row.column_by_name<Float>("\"aBc\""));
  }

  /**
   * Insert all values into the table
   *
   * @param statement Insert statement to use (all format)
   */
  void insert_and_validate_all(Statement statement) {
    // Insert values into the table by name
    TimeUuid key = uuid_generator_.generate_timeuuid();
    statement.bind<TimeUuid>("key", key);
    statement.bind<Integer>("a", Integer(9042));
    statement.bind<Boolean>("b", Boolean(true));
    statement.bind<Text>("c", Text("yyz"));
    statement.bind<Float>("\"abc\"", Float(1.1f));
    statement.bind<Float>("\"ABC\"", Float(2.2f));
    statement.bind<Float>("\"aBc\"", Float(3.3f));
    session_.execute(statement);

    // Validate the inserts into the table
    Result result = session_.execute(default_select_all());
    ASSERT_EQ(1u, result.row_count());
    ASSERT_EQ(7u, result.column_count());
    Row row = result.first_row();
    ASSERT_EQ(key, row.column_by_name<TimeUuid>("key"));
    ASSERT_EQ(Integer(9042), row.column_by_name<Integer>("a"));
    ASSERT_EQ(Boolean(true), row.column_by_name<Boolean>("b"));
    ASSERT_EQ(Text("yyz"), row.column_by_name<Text>("c"));
    ASSERT_EQ(Float(1.1f), row.column_by_name<Float>("\"abc\""));
    ASSERT_EQ(Float(2.2f), row.column_by_name<Float>("\"ABC\""));
    ASSERT_EQ(Float(3.3f), row.column_by_name<Float>("\"aBc\""));
  }

  /**
   * Insert all values into the table
   *
   * @param statement Insert statement to use (all format)
   */
  void insert_and_validate_all_null(Statement statement) {
    // Create NULL wrapped objects for easier inserting and validation
    TimeUuid key = uuid_generator_.generate_timeuuid();
    Integer a = Integer();
    Boolean b = Boolean();
    Text c = Text();
    Float abc = Float();

    // Insert values into the table by name
    statement.bind<TimeUuid>("key", key);
    statement.bind<Integer>("a", a);
    statement.bind<Boolean>("b", b);
    statement.bind<Text>("c", c);
    statement.bind<Float>("\"abc\"", abc);
    statement.bind<Float>("\"ABC\"", abc);
    statement.bind<Float>("\"aBc\"", abc);
    session_.execute(statement);

    // Validate the inserts into the table
    Result result = session_.execute(default_select_all());
    ASSERT_EQ(1u, result.row_count());
    ASSERT_EQ(7u, result.column_count());
    Row row = result.first_row();
    ASSERT_EQ(key, row.column_by_name<TimeUuid>("key"));
    ASSERT_TRUE(row.column_by_name<Integer>("a").is_null());
    ASSERT_TRUE(row.column_by_name<Boolean>("b").is_null());
    ASSERT_TRUE(row.column_by_name<Text>("c").is_null());
    ASSERT_TRUE(row.column_by_name<Float>("\"abc\"").is_null());
    ASSERT_TRUE(row.column_by_name<Float>("\"ABC\"").is_null());
    ASSERT_TRUE(row.column_by_name<Float>("\"aBc\"").is_null());
  }
};

/**
 * By name (bytes) integration tests
 */
class ByNameBytesTests : public Integration {
  virtual void SetUp() {
    // Call the parent setup function
    Integration::SetUp();

    // Create the table for the test
    session_.execute(format_string(TABLE_BYTES_FORMAT, table_name_.c_str()));
  }
};

/**
 * Perform `by name` references using a prepared statement and validate
 *
 * This test will perform a `by name` insert using a prepared statement and
 * validate the results a single node cluster.
 *
 * @test_category queries:prepared
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and validated by name
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameTests, Prepared) {
  CHECK_FAILURE;

  // Prepare, create, insert and validate
  Prepared prepared = session_.prepare(format_string(INSERT_FORMAT, table_name_.c_str()));
  insert_and_validate(prepared.bind());
}

/**
 * Perform `by name` references using a simple (bound) statement and validate
 *
 * This test will perform a `by name` insert using a simple statement and
 * validate the results a single node cluster.
 *
 * @test_category queries:basic
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and validated by name
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameTests, Simple) {
  CHECK_FAILURE;

  // Prepare, create, insert and validate
  Statement statement(format_string(INSERT_FORMAT, table_name_.c_str()), 4);
  insert_and_validate(statement);
}

/**
 * Perform case sensitive `by name` references using a prepared statement and
 * validate
 *
 * This test will perform a case sensitive`by name` insert using a prepared
 * statement and validate the results a single node cluster.
 *
 * @test_category queries:prepared
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and validated by name
 *                  (case sensitive)
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameTests, PreparedCaseSensitive) {
  CHECK_FAILURE;

  // Prepare, create, insert and validate
  Prepared prepared =
      session_.prepare(format_string(INSERT_CASE_SENSITIVE_FORMAT, table_name_.c_str()));
  insert_and_validate_case_sensitive(prepared.bind());
}

/**
 * Perform case sensitive `by name` references using a simple (bound) statement
 * and validate
 *
 * This test will perform a case sensitive `by name` insert using a simple
 * statement and validate the results a single node cluster.
 *
 * @test_category queries:basic
 * @since core:1.0.0
 * @cassandra_version 2.1.0
 * @expected_result Cassandra values are inserted and validated by name
 *                  (case sensitive)
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameTests, SimpleCaseSensitive) {
  CHECK_FAILURE;
  CHECK_VERSION(2.1.0);

  // Prepare, create, insert and validate
  Statement statement(format_string(INSERT_CASE_SENSITIVE_FORMAT, table_name_.c_str()), 4);
  insert_and_validate_case_sensitive(statement);
}

/**
 * Perform `by name` references using a prepared statement to insert multiple
 * value and validate
 *
 * This test will perform bindings of a value to multiple columns `by name`
 * insert using a prepared statement and validate the results a single node
 * cluster.
 *
 * @test_category queries:prepared
 * @since core:1.0.0
 * @expected_result Cassandra values are inserted and validated by name
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameTests, MultipleBinds) {
  CHECK_FAILURE;

  // Prepare, bind, and insert the values into the table
  Prepared prepared =
      session_.prepare(format_string(INSERT_CASE_SENSITIVE_FORMAT, table_name_.c_str()));
  Statement statement = prepared.bind();
  TimeUuid key = uuid_generator_.generate_timeuuid();
  statement.bind<TimeUuid>("key", key);
  statement.bind<Float>("abc", Float(1.23f)); // This should bind to columns `abc`, `ABC`, and `aBc`
  session_.execute(statement);

  // Validate the inserts to multiple binded columns
  Result result = session_.execute(default_select_all());
  ASSERT_EQ(1u, result.row_count());
  ASSERT_EQ(7u, result.column_count());
  Row row = result.first_row();
  ASSERT_EQ(key, row.column_by_name<TimeUuid>("key"));
  ASSERT_EQ(Float(1.23f), row.column_by_name<Float>("\"abc\""));
  ASSERT_EQ(Float(1.23f), row.column_by_name<Float>("\"ABC\""));
  ASSERT_EQ(Float(1.23f), row.column_by_name<Float>("\"aBc\""));
}

/**
 * Perform `by name` references using a prepared statement against an invalid
 * column name
 *
 * This test will attempt to bind an invalid column name to a prepared statement
 * using a single node cluster.
 *
 * @test_category queries:prepared
 * @since core:1.0.0
 * @expected_result Driver error will occur when binding value to invalid column
 *                 name
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameTests, BindUsingInvalidName) {
  CHECK_FAILURE;

  // Prepare and create the insert statement
  Prepared prepared = session_.prepare(format_string(INSERT_ALL_FORMAT, table_name_.c_str()));
  Statement statement = prepared.bind();

  // Bind values to invalid columns name and validate error
  CassError error_code = cass_statement_bind_int32_by_name(statement.get(), "d", 0);
  ASSERT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST, error_code);
  error_code = cass_statement_bind_float_by_name(statement.get(), "\"aBC", 0.0f);
  ASSERT_EQ(CASS_ERROR_LIB_NAME_DOES_NOT_EXIST, error_code);
  error_code = cass_statement_bind_float_by_name(statement.get(), "\"abC", 0.0f);
}

/**
 * Perform `by name` references against an invalid column name lookup
 *
 * This test will perform a `by name` insert using a prepared statement and
 * attempt to retrieve a column using an invalid name against a single node
 * cluster.
 *
 * @test_category queries:basic
 * @since core:1.0.0
 * @expected_result Driver error will occur when retrieving value from invalid
 *                 column name
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameTests, RetrieveInvalidName) {
  CHECK_FAILURE;

  // Prepare, create, insert and validate (all)
  Prepared prepared = session_.prepare(format_string(INSERT_ALL_FORMAT, table_name_.c_str()));
  Statement statement = prepared.bind();
  insert_and_validate_all(statement);

  // Retrieve the row and validate error using invalid column name
  Result result = session_.execute(default_select_all());
  Row row = result.first_row();
  ASSERT_TRUE(NULL == cass_row_get_column_by_name(row.get(), "d"));
  ASSERT_TRUE(NULL == cass_row_get_column_by_name(row.get(), "\"aBC\""));
  ASSERT_TRUE(NULL == cass_row_get_column_by_name(row.get(), "\"abC\""));
}

/**
 * Perform `by name` references using a prepared statement and validate values
 * are NULL
 *
 * This test will perform a `by name` NULL insert using a prepared statement and
 * validate the results a single node cluster.
 *
 * @test_category queries:prepared
 * @since core:1.0.0
 * @expected_result Cassandra NULL values are inserted and validated by name
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameTests, NullPrepared) {
  CHECK_FAILURE;

  // Prepare, create, insert and validate
  Prepared prepared = session_.prepare(format_string(INSERT_ALL_FORMAT, table_name_.c_str()));
  insert_and_validate_all_null(prepared.bind());
}

/**
 * Perform `by name` references using a simple (bound) statement and validate
 * values are NULL
 *
 * This test will perform a `by name` NULL insert using a simple statement and
 * validate the results a single node cluster.
 *
 * @test_category queries:basic
 * @since core:1.0.0
 * @cassandra_version 2.1.0
 * @expected_result Cassandra NULL values are inserted and validated by name
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameTests, NullSimple) {
  CHECK_FAILURE;
  CHECK_VERSION(2.1.0);

  // Prepare, create, insert and validate
  Statement statement(format_string(INSERT_ALL_FORMAT, table_name_.c_str()), 7);
  insert_and_validate_all_null(statement);
}

/**
 * Perform `by name` references using a prepared statement and validate using
 * bytes
 *
 * This test will perform a `by name` bytes insert using a prepared statement
 * for `blob` and `varint` data types validating  inserts.
 *
 * NOTE: This will be using a single node cluster.
 *
 * @jira_ticket CPP-272
 * @test_category queries:basic
 * @since core:2.1.0-beta
 * @expected_result Bytes will be bound to `blob` and `varint` valued and will
 *                  be validated by name
 */
CASSANDRA_INTEGRATION_TEST_F(ByNameBytesTests, Prepared) {
  CHECK_FAILURE;

  // Create values to be inserted in the test
  TimeUuid key = uuid_generator_.generate_timeuuid();
  Blob blobs("68971169783116971203269110116101114112114105115101329911211245100114105118101114");
  Varint varints("1234567890123456789012345678901234567890");

  // Prepare, bind, and insert the values into the table
  Prepared prepared = session_.prepare(format_string(INSERT_BYTES_FORMAT, table_name_.c_str()));
  Statement statement = prepared.bind();
  statement.bind<TimeUuid>("key", key);
  statement.bind<Blob>("blobs", blobs);
  statement.bind<Varint>("varints", varints);
  session_.execute(statement);

  // Validate the inserts
  Result result = session_.execute(default_select_all());
  ASSERT_EQ(1u, result.row_count());
  ASSERT_EQ(3u, result.column_count());
  Row row = result.first_row();
  ASSERT_EQ(key, row.column_by_name<TimeUuid>("key"));
  ASSERT_EQ(blobs, row.column_by_name<Blob>("blobs"));
  ASSERT_EQ(varints, row.column_by_name<Varint>("varints"));
}
