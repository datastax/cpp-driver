/*
  Copyright (c) 2014-2016 DataStax

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

#include "test_utils.hpp"
#include "cassandra.h"

#include <boost/format.hpp>
#include <boost/test/unit_test.hpp>

struct DataTypesTests : public test_utils::SingleSessionTest {
  DataTypesTests() : test_utils::SingleSessionTest(1, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }

  ~DataTypesTests() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(session,
      str(boost::format(test_utils::DROP_KEYSPACE_FORMAT)
      % test_utils::SIMPLE_KEYSPACE));
  }

  /**
   * Insert and validate a datatype
   *
   * @param value_type CassValueType to use for value
   * @param value Value to use
   */
  template <class T>
  void insert_value(CassValueType value_type, T value) {
    // Create the table for the test
    std::string table_name = "table_" + test_utils::generate_unique_str(uuid_gen);
    std::string create_table = "CREATE TABLE " + table_name + "(key text PRIMARY KEY, value " + test_utils::get_value_type(value_type) +")";
    test_utils::execute_query(session, create_table.c_str());

    // Bind, validate, and insert the value into Cassandra
    std::string insert_query = "INSERT INTO " + table_name + "(key, value) VALUES(? , ?)";
    test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
    // Determine if bound parameters can be used based on C* version
    if (version.major_version == 1) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query);
      statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }
    BOOST_REQUIRE_EQUAL(cass_statement_bind_string(statement.get(), 0, "simple"), CASS_OK);
    BOOST_REQUIRE_EQUAL(test_utils::Value<T>::bind(statement.get(), 1, value), CASS_OK);
    test_utils::wait_and_check_error(test_utils::CassFuturePtr(cass_session_execute(session, statement.get())).get());
    test_utils::CassPreparedPtr prepared(test_utils::prepare(session, insert_query.c_str()));
    statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    BOOST_REQUIRE_EQUAL(cass_data_type_type(cass_prepared_parameter_data_type(prepared.get(), 0)), CASS_VALUE_TYPE_VARCHAR);
    BOOST_REQUIRE_EQUAL(cass_data_type_type(cass_prepared_parameter_data_type_by_name(prepared.get(), "key")), CASS_VALUE_TYPE_VARCHAR);
    BOOST_REQUIRE_EQUAL(cass_data_type_type(cass_prepared_parameter_data_type(prepared.get(), 1)), value_type);
    BOOST_REQUIRE_EQUAL(cass_data_type_type(cass_prepared_parameter_data_type_by_name(prepared.get(), "value")), value_type);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_string(statement.get(), 0, "prepared"), CASS_OK);
    BOOST_REQUIRE_EQUAL(test_utils::Value<T>::bind(statement.get(), 1, value), CASS_OK);
    test_utils::wait_and_check_error(test_utils::CassFuturePtr(cass_session_execute(session, statement.get())).get());

    // Ensure the value can be read
    std::string select_query = "SELECT key, value FROM " + table_name;
    test_utils::CassResultPtr result;
    test_utils::execute_query(session, select_query.c_str(), &result);
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 2);
    BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 2);
    test_utils::CassIteratorPtr rows(cass_iterator_from_result(result.get()));
    unsigned int count = 0;
    while (cass_iterator_next(rows.get()) && count < 2) {
      const CassRow* row = cass_iterator_get_row(rows.get());

      // Verify the key
      const CassValue* key_value = cass_row_get_column(row, 0);
      const CassDataType* key_data_type = cass_value_data_type(key_value);
      BOOST_REQUIRE_EQUAL(cass_value_type(key_value), CASS_VALUE_TYPE_VARCHAR);
      BOOST_REQUIRE_EQUAL(cass_data_type_type(key_data_type), CASS_VALUE_TYPE_VARCHAR);
      CassString key_result;
      cass_value_get_string(key_value, &key_result.data, &key_result.length);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(CassString("simple"), key_result) || test_utils::Value<CassString>::equal(CassString("prepared"), key_result));

      // Verify the value
      const CassValue* value_value = cass_row_get_column(row, 1);
      const CassDataType* value_data_type = cass_value_data_type(value_value);
      BOOST_REQUIRE_EQUAL(cass_value_type(value_value), value_type);
      BOOST_REQUIRE_EQUAL(cass_data_type_type(value_data_type), value_type);
      T value_result;
      BOOST_REQUIRE_EQUAL(test_utils::Value<T>::get(value_value, &value_result), CASS_OK);
      BOOST_REQUIRE(test_utils::Value<T>::equal(value, value_result));

      ++count;
    }
    BOOST_REQUIRE_EQUAL(count, 2);
  }
};

BOOST_FIXTURE_TEST_SUITE(datatypes, DataTypesTests)

/**
 * Read/Write Datatypes
 *
 * This test ensures driver datatypes can be read/written to Cassandra
 *
 * @since 2.1.0-beta
 * @jira_ticket CPP-96
 * @test_category data_types
 */
BOOST_AUTO_TEST_CASE(read_write_primitives) {
  {
    CassString value("Test Value");
    insert_value<CassString>(CASS_VALUE_TYPE_ASCII, value);
    insert_value<CassString>(CASS_VALUE_TYPE_VARCHAR, value); // NOTE: text is alias for varchar
  }

  {
    cass_int64_t value = 1234567890;
    insert_value<cass_int64_t>(CASS_VALUE_TYPE_BIGINT, value);
    insert_value<cass_int64_t>(CASS_VALUE_TYPE_TIMESTAMP, value);
  }

  {
    CassBytes value = test_utils::bytes_from_string("012345678900123456789001234567890012345678900123456789001234567890");
    insert_value<CassBytes>(CASS_VALUE_TYPE_BLOB, value);
    insert_value<CassBytes>(CASS_VALUE_TYPE_VARINT, value);
  }

  insert_value<cass_bool_t>(CASS_VALUE_TYPE_BOOLEAN, cass_true);

  {
    const cass_uint8_t pi[] = { 57, 115, 235, 135, 229, 215, 8, 125, 13, 43, 1, 25, 32, 135, 129, 180,
      112, 176, 158, 120, 246, 235, 29, 145, 238, 50, 108, 239, 219, 100, 250,
      84, 6, 186, 148, 76, 230, 46, 181, 89, 239, 247 };
    const cass_int32_t pi_scale = 100;
    CassDecimal value = CassDecimal(pi, sizeof(pi), pi_scale);
    insert_value<CassDecimal>(CASS_VALUE_TYPE_DECIMAL, value);
  }

  if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
    CassDuration value = CassDuration(0, 0, 0);
    insert_value<CassDuration>(CASS_VALUE_TYPE_DURATION, value);

    value = CassDuration(1, 2, 3);
    insert_value<CassDuration>(CASS_VALUE_TYPE_DURATION, value);

    value = CassDuration((1ULL << 31) - 1, (1ULL << 31) - 1, (1ULL << 63) - 1);
    insert_value<CassDuration>(CASS_VALUE_TYPE_DURATION, value);

    value = CassDuration(1LL << 31, 1LL << 31, 1LL << 63);
    insert_value<CassDuration>(CASS_VALUE_TYPE_DURATION, value);
  }

  insert_value<cass_double_t>(CASS_VALUE_TYPE_DOUBLE, 3.141592653589793);
  insert_value<cass_float_t>(CASS_VALUE_TYPE_FLOAT, 3.1415926f);
  insert_value<cass_int32_t>(CASS_VALUE_TYPE_INT, 123);
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    insert_value<cass_int16_t>(CASS_VALUE_TYPE_SMALL_INT, 123);
    insert_value<cass_int8_t>(CASS_VALUE_TYPE_TINY_INT, 123);
    insert_value<CassDate>(CASS_VALUE_TYPE_DATE, test_utils::Value<CassDate>::min_value() + 1u);
    insert_value<CassTime>(CASS_VALUE_TYPE_TIME, 123);
  }

  {
    CassUuid value = test_utils::generate_random_uuid(uuid_gen);
    insert_value<CassUuid>(CASS_VALUE_TYPE_UUID, value);
  }

  {
    CassInet value = test_utils::inet_v4_from_int(16777343); // 127.0.0.1
    insert_value<CassInet>(CASS_VALUE_TYPE_INET, value);
  }

  {
    CassUuid value = test_utils::generate_time_uuid(uuid_gen);
    insert_value<CassUuid>(CASS_VALUE_TYPE_TIMEUUID, value);
  }
}

/**
 * Ensure that a server error occurs with invalid duration values (mixed)
 *
 * This test will ensure that when using mixed positive and negative values on
 * a duration data type, the server will return an error during statement
 * execution.
 *
 * @jira_ticket CPP-429
 * @since 2.6.0
 * @test_category data_types::duration
 * @expected_result Driver will handle the server error on statement execution
 */
BOOST_AUTO_TEST_CASE(duration_mixed_values_server_error)
{
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 3 && version.minor_version >= 10)
    || version.major_version >= 4) {
    // Create the table for the test
    std::string table_name = "duration_server_error";
    std::string create_table = "CREATE TABLE " + table_name
      + "(key text PRIMARY KEY, value duration)";
    test_utils::execute_query(session, create_table.c_str());

    // Bind, validate, and insert the value into the server
    std::string insert_query = "INSERT INTO " + table_name + "(key, value) VALUES(? , ?)";
    test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
    CassDuration value = CassDuration(0, -1, 1);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_string(statement.get(), 0, "simple"), CASS_OK);
    BOOST_REQUIRE_EQUAL(test_utils::Value<CassDuration>::bind(statement.get(), 1, value), CASS_OK);
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    CassError error_code = test_utils::wait_and_return_error(future.get());

    // Validate the server error and message
    BOOST_REQUIRE_EQUAL(CASS_ERROR_SERVER_INVALID_QUERY, error_code);
    CassString message;
    cass_future_error_message(future.get(), &message.data, &message.length);
    std::string expected = "The duration months, days and nanoseconds must be all of the same sign";
    BOOST_REQUIRE(std::string(message.data, message.length).find(expected) != std::string::npos);

  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string()
      << ": Skipping datatypes/duration_mixed_values_server_error" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
