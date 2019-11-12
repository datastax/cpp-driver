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

#define TABLE_FORMAT                                                                           \
  "CREATE TABLE IF NOT EXISTS %s(key int, value_text text, value_uuid uuid, value_blob blob, " \
  "value_list_floats list<float>, PRIMARY KEY (key, value_text))"
#define INSERT_QUERY_FORMAT                                                            \
  "INSERT INTO %s(key, value_text, value_uuid, value_blob, value_list_floats) VALUES " \
  "(:named_key, :named_text, :named_uuid, :named_blob, :named_list_floats)"
#define SELECT_QUERY_FORMAT                                                            \
  "SELECT value_uuid, value_blob, value_list_floats FROM %s WHERE key=:named_key AND " \
  "value_text=:named_text"

const Float FLOAT_VALUES[] = { Float::max(), Float::min(), Float(3.14159f), Float(2.71828f),
                               Float(1.61803f) };
const std::vector<Float> NUMBERS(FLOAT_VALUES, FLOAT_VALUES + ARRAY_LEN(FLOAT_VALUES));

class NamedParametersTests : public Integration {
public:
  NamedParametersTests()
      : key_(1)
      , value_text_("DataStax C/C++ Driver")
      , value_blob_("Cassandra")
      , value_list_floats_(NUMBERS) {
    value_uuid_ = uuid_generator_.generate_random_uuid();
  }

  virtual void SetUp() {
    Integration::SetUp();

    session_.execute(format_string(TABLE_FORMAT, table_name_.c_str()));
    prepared_insert_statement_ =
        session_.prepare(format_string(INSERT_QUERY_FORMAT, table_name_.c_str()));
    prepared_select_statement_ =
        session_.prepare(format_string(SELECT_QUERY_FORMAT, table_name_.c_str()));
  }

  Statement create_insert_statement(bool is_from_prepared = false) {
    if (is_from_prepared) {
      return prepared_insert_statement_.bind();
    }
    return Statement(format_string(INSERT_QUERY_FORMAT, table_name_.c_str()), 5);
  }

  Statement create_select_statement(bool is_from_prepared = false) {
    if (is_from_prepared) {
      return prepared_select_statement_.bind();
    }
    return Statement(format_string(SELECT_QUERY_FORMAT, table_name_.c_str()), 2);
  }

  void execute_select_statement(const Statement& select_statement) {
    Result result = session_.execute(select_statement);
    ASSERT_EQ(1u, result.row_count());
    ASSERT_EQ(3u, result.column_count());
    Row row = result.first_row();
    EXPECT_EQ(value_uuid_, row.column_by_name<Uuid>("value_uuid"));
    EXPECT_EQ(value_blob_, row.column_by_name<Blob>("value_blob"));
    EXPECT_EQ(value_list_floats_, row.column_by_name<List<Float> >("value_list_floats"));
  }

protected:
  Integer key_;
  Text value_text_;
  Uuid value_uuid_;
  Blob value_blob_;
  List<Float> value_list_floats_;

private:
  Prepared prepared_insert_statement_;
  Prepared prepared_select_statement_;
};

/**
 * Ensures named parameters can be used with simple statements when bound in order.
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @cassandra_version 2.1.x
 */
CASSANDRA_INTEGRATION_TEST_F(NamedParametersTests, SimpleStatementInOrder) {
  CHECK_FAILURE;
  CHECK_VERSION(2.1.0);

  Statement insert_statement = create_insert_statement();
  insert_statement.bind<Integer>("named_key", key_);
  insert_statement.bind<Text>("named_text", value_text_);
  insert_statement.bind<Uuid>("named_uuid", value_uuid_);
  insert_statement.bind<Blob>("named_blob", value_blob_);
  insert_statement.bind<List<Float> >("named_list_floats", value_list_floats_);
  session_.execute(insert_statement);

  Statement select_statement = create_select_statement();
  select_statement.bind<Integer>("named_key", key_);
  select_statement.bind<Text>("named_text", value_text_);
  execute_select_statement(select_statement);
}

/**
 * Ensures named parameters can be used with simple statements when bound in any order.
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @cassandra_version 2.1.x
 */
CASSANDRA_INTEGRATION_TEST_F(NamedParametersTests, SimpleStatementAnyOrder) {
  CHECK_FAILURE;
  CHECK_VERSION(2.1.0);

  Statement insert_statement = create_insert_statement();
  insert_statement.bind<Blob>("named_blob", value_blob_);
  insert_statement.bind<Text>("named_text", value_text_);
  insert_statement.bind<List<Float> >("named_list_floats", value_list_floats_);
  insert_statement.bind<Integer>("named_key", key_);
  insert_statement.bind<Uuid>("named_uuid", value_uuid_);
  session_.execute(insert_statement);

  Statement select_statement = create_select_statement();
  select_statement.bind<Text>("named_text", value_text_);
  select_statement.bind<Integer>("named_key", key_);
  execute_select_statement(select_statement);
}

/**
 * Ensures named parameters can be used with prepared statements when bound in order.
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @cassandra_version 2.1.x
 */
CASSANDRA_INTEGRATION_TEST_F(NamedParametersTests, PreparedStatementInOrder) {
  CHECK_FAILURE;
  CHECK_VERSION(2.1.0);

  Statement insert_statement = create_insert_statement(true);
  insert_statement.bind<Integer>("named_key", key_);
  insert_statement.bind<Text>("named_text", value_text_);
  insert_statement.bind<Uuid>("named_uuid", value_uuid_);
  insert_statement.bind<Blob>("named_blob", value_blob_);
  insert_statement.bind<List<Float> >("named_list_floats", value_list_floats_);
  session_.execute(insert_statement);

  Statement select_statement = create_select_statement(true);
  select_statement.bind<Integer>("named_key", key_);
  select_statement.bind<Text>("named_text", value_text_);
  execute_select_statement(select_statement);
}

/**
 * Ensures named parameters can be used with prepared statements when bound in any order.
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @cassandra_version 2.1.x
 */
CASSANDRA_INTEGRATION_TEST_F(NamedParametersTests, PreparedStatementAnyOrder) {
  CHECK_FAILURE;
  CHECK_VERSION(2.1.0);

  Statement insert_statement = create_insert_statement(true);
  insert_statement.bind<Blob>("named_blob", value_blob_);
  insert_statement.bind<Text>("named_text", value_text_);
  insert_statement.bind<List<Float> >("named_list_floats", value_list_floats_);
  insert_statement.bind<Integer>("named_key", key_);
  insert_statement.bind<Uuid>("named_uuid", value_uuid_);
  session_.execute(insert_statement);

  Statement select_statement = create_select_statement(true);
  select_statement.bind<Text>("named_text", value_text_);
  select_statement.bind<Integer>("named_key", key_);
  execute_select_statement(select_statement);
}

/**
 * Ensures invalid named parameters return an error when bound and executed using a simple
 * statement.
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @cassandra_version 2.1.x
 */
CASSANDRA_INTEGRATION_TEST_F(NamedParametersTests, SimpleStatementInvalidName) {
  CHECK_FAILURE;
  CHECK_VERSION(2.1.0);

  Statement insert_statement = create_insert_statement();
  insert_statement.bind<Integer>("invalid_named_key", key_);
  insert_statement.bind<Text>("named_text", value_text_);
  insert_statement.bind<Uuid>("named_uuid", value_uuid_);
  insert_statement.bind<Blob>("named_blob", value_blob_);
  insert_statement.bind<List<Float> >("named_list_floats", value_list_floats_);
  EXPECT_EQ(CASS_ERROR_SERVER_INVALID_QUERY,
            session_.execute(insert_statement, false).error_code());
}

/**
 * Ensures invalid named parameters return an error when bound and executed using a prepared
 * statement.
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-263
 * @cassandra_version 2.1.x
 */
CASSANDRA_INTEGRATION_TEST_F(NamedParametersTests, PreparedStatementInvalidName) {
  CHECK_FAILURE;
  CHECK_VERSION(2.1.0);

  Statement insert_statement = create_insert_statement(true);
  EXPECT_EQ(
      CASS_ERROR_LIB_NAME_DOES_NOT_EXIST,
      cass_statement_bind_int32_by_name(insert_statement.get(), "invalid_named_key", key_.value()));
}
