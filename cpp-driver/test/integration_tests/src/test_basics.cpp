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

#include <algorithm>
#include <cmath>

#include <boost/chrono.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/test/debug.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "constants.hpp"
#include "test_utils.hpp"

struct BasicTests : public test_utils::SingleSessionTest {
  BasicTests()
      : SingleSessionTest(1, 0) {
    test_utils::execute_query(session,
                              str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) %
                                  test_utils::SIMPLE_KEYSPACE % "1"));

    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }

  ~BasicTests() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(
        session,
        str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % test_utils::SIMPLE_KEYSPACE));
  }

  template <class T>
  void insert_single_value(CassValueType type, T value) {
    std::string table_name =
        str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
    std::string type_name = test_utils::get_value_type(type);

    test_utils::execute_query(
        session, str(boost::format("CREATE TABLE %s (tweet_id uuid PRIMARY KEY, test_val %s)") %
                     table_name % type_name));

    /*
     * Bound parameters or prepared statement validation
     */

    // Create insert statement for bound parameters
    CassUuid tweet_id = test_utils::generate_random_uuid(uuid_gen);
    std::string insert_query =
        str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(?, ?)") % table_name);
    test_utils::CassStatementPtr insert_statement(cass_statement_new(insert_query.c_str(), 2));

    // Determine if bound parameters can be used based on C* version
    if (version.major_version == 1) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query);
      insert_statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }

    BOOST_REQUIRE(cass_statement_bind_uuid(insert_statement.get(), 0, tweet_id) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::bind(insert_statement.get(), 1, value) == CASS_OK);
    test_utils::CassFuturePtr insert_future(cass_session_execute(session, insert_statement.get()));
    test_utils::wait_and_check_error(insert_future.get());

    // Create select statement for bound parameters
    std::string select_query =
        str(boost::format("SELECT * FROM %s WHERE tweet_id = ?;") % table_name);
    test_utils::CassStatementPtr select_statement(cass_statement_new(select_query.c_str(), 1));

    // Determine if bound parameters can be used based on C* version
    if (version.major_version == 1) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, select_query);
      select_statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }

    BOOST_REQUIRE(cass_statement_bind_uuid(select_statement.get(), 0, tweet_id) == CASS_OK);
    test_utils::CassFuturePtr select_future(cass_session_execute(session, select_statement.get()));
    test_utils::wait_and_check_error(select_future.get());

    test_utils::CassResultPtr result(cass_future_get_result(select_future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

    const CassValue* column = cass_row_get_column(cass_result_first_row(result.get()), 1);
    T result_value;
    BOOST_REQUIRE(cass_value_type(column) == type);
    BOOST_REQUIRE(test_utils::Value<T>::get(column, &result_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::equal(result_value, value));

    /*
     * Simple statement validation
     */

    // Generate the appropriate formatted string for the simple statement
    std::string tweet_id_string = test_utils::generate_random_uuid_str(uuid_gen);
    std::string value_string = test_utils::Value<T>::to_string(value);
    if (type == CASS_VALUE_TYPE_BLOB) {
      value_string = "0x" + test_utils::to_hex(value_string.c_str());
    } else if (type == CASS_VALUE_TYPE_VARINT) {
      value_string = test_utils::BigNumber::to_string(value_string.c_str());
    } else if (type == CASS_VALUE_TYPE_ASCII || type == CASS_VALUE_TYPE_TEXT ||
               type == CASS_VALUE_TYPE_VARCHAR || type == CASS_VALUE_TYPE_INET) {
      value_string = "'" + value_string + "'";
    }

    insert_query = str(boost::format(test_utils::replaceAll(insert_query, "?", "%s")) %
                       tweet_id_string % value_string);
    insert_statement = test_utils::CassStatementPtr(cass_statement_new(insert_query.c_str(), 0));
    insert_future =
        test_utils::CassFuturePtr(cass_session_execute(session, insert_statement.get()));
    test_utils::wait_and_check_error(insert_future.get());

    select_query =
        str(boost::format(test_utils::replaceAll(select_query, "?", "%s")) % tweet_id_string);
    select_statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 0));
    select_future =
        test_utils::CassFuturePtr(cass_session_execute(session, select_statement.get()));
    test_utils::wait_and_check_error(select_future.get());

    result = test_utils::CassResultPtr(cass_future_get_result(select_future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

    column = cass_row_get_column(cass_result_first_row(result.get()), 1);
    BOOST_REQUIRE(cass_value_type(column) == type);
    BOOST_REQUIRE(test_utils::Value<T>::get(column, &result_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::equal(result_value, value));
  }

  template <class T>
  void insert_min_max_value(CassValueType type) {
    std::string table_name =
        str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
    std::string type_name = test_utils::get_value_type(type);

    test_utils::execute_query(
        session,
        str(boost::format("CREATE TABLE %s (tweet_id uuid PRIMARY KEY, min_val %s, max_val %s)") %
            table_name % type_name % type_name));

    /*
     * Bound parameters or prepared statement validation
     */

    // Create insert statement for bound parameters
    CassUuid tweet_id = test_utils::generate_random_uuid(uuid_gen);
    std::string insert_query = str(
        boost::format("INSERT INTO %s (tweet_id, min_val, max_val) VALUES(?, ?, ?);") % table_name);
    test_utils::CassStatementPtr insert_statement(cass_statement_new(insert_query.c_str(), 3));

    // Determine if bound parameters can be used based on C* version
    if (version.major_version == 1) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query);
      insert_statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }

    BOOST_REQUIRE(cass_statement_bind_uuid(insert_statement.get(), 0, tweet_id) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::bind(insert_statement.get(), 1,
                                             test_utils::Value<T>::min_value()) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::bind(insert_statement.get(), 2,
                                             test_utils::Value<T>::max_value()) == CASS_OK);
    test_utils::CassFuturePtr result_future(cass_session_execute(session, insert_statement.get()));
    test_utils::wait_and_check_error(result_future.get());

    // Create select statement for bound parameters
    std::string select_query =
        str(boost::format("SELECT * FROM %s WHERE tweet_id = ?;") % table_name);
    test_utils::CassStatementPtr select_statement(cass_statement_new(select_query.c_str(), 1));

    // Determine if bound parameters can be used based on C* version
    if (version.major_version == 1) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, select_query.c_str());
      select_statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }

    BOOST_REQUIRE(cass_statement_bind_uuid(select_statement.get(), 0, tweet_id) == CASS_OK);
    test_utils::CassFuturePtr select_future(cass_session_execute(session, select_statement.get()));
    test_utils::wait_and_check_error(select_future.get());

    test_utils::CassResultPtr result(cass_future_get_result(select_future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 3);

    T min_value;
    BOOST_REQUIRE(
        test_utils::Value<T>::get(cass_row_get_column(cass_result_first_row(result.get()), 2),
                                  &min_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::equal(min_value, test_utils::Value<T>::min_value()));

    T max_value;
    BOOST_REQUIRE(
        test_utils::Value<T>::get(cass_row_get_column(cass_result_first_row(result.get()), 1),
                                  &max_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::equal(max_value, test_utils::Value<T>::max_value()));

    /*
     * Simple statement validation
     */

    // Generate the appropriate formatted strings for the simple statement
    std::string tweet_id_string = test_utils::generate_random_uuid_str(uuid_gen);
    std::string min_value_string =
        test_utils::Value<T>::to_string(test_utils::Value<T>::min_value());
    std::string max_value_string =
        test_utils::Value<T>::to_string(test_utils::Value<T>::max_value());
    if (type == CASS_VALUE_TYPE_INET) {
      min_value_string = "'" + min_value_string + "'";
      max_value_string = "'" + max_value_string + "'";
    }

    insert_query = str(boost::format(test_utils::replaceAll(insert_query, "?", "%s")) %
                       tweet_id_string % min_value_string % max_value_string);
    insert_statement = test_utils::CassStatementPtr(cass_statement_new(insert_query.c_str(), 0));
    result_future =
        test_utils::CassFuturePtr(cass_session_execute(session, insert_statement.get()));
    test_utils::wait_and_check_error(result_future.get());

    select_query =
        str(boost::format(test_utils::replaceAll(select_query, "?", "%s")) % tweet_id_string);
    select_statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 0));
    select_future =
        test_utils::CassFuturePtr(cass_session_execute(session, select_statement.get()));
    test_utils::wait_and_check_error(select_future.get());

    result = test_utils::CassResultPtr(cass_future_get_result(select_future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 3);

    BOOST_REQUIRE(
        test_utils::Value<T>::get(cass_row_get_column(cass_result_first_row(result.get()), 2),
                                  &min_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::equal(min_value, test_utils::Value<T>::min_value()));

    BOOST_REQUIRE(
        test_utils::Value<T>::get(cass_row_get_column(cass_result_first_row(result.get()), 1),
                                  &max_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::equal(max_value, test_utils::Value<T>::max_value()));
  }

  template <class T>
  void insert_null_value(CassValueType type) {
    std::string table_name =
        str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
    std::string type_name = test_utils::get_value_type(type);

    if (type == CASS_VALUE_TYPE_LIST || type == CASS_VALUE_TYPE_SET) {
      type_name.append("<text>");
    } else if (type == CASS_VALUE_TYPE_MAP) {
      type_name.append("<text, text>");
    }

    test_utils::execute_query(
        session, str(boost::format("CREATE TABLE %s (tweet_id uuid PRIMARY KEY, test_val %s)") %
                     table_name % type_name));

    /*
     * Bound parameters or prepared statement validation
     */

    // Create insert statement for bound parameters
    CassUuid tweet_id = test_utils::generate_random_uuid(uuid_gen);
    std::string insert_query =
        str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(?, ?);") % table_name);
    test_utils::CassStatementPtr insert_statement(cass_statement_new(insert_query.c_str(), 2));

    // Determine if bound parameters can be used based on C* version
    if (version.major_version == 1) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query);
      insert_statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }

    BOOST_REQUIRE(cass_statement_bind_uuid(insert_statement.get(), 0, tweet_id) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_null(insert_statement.get(), 1) == CASS_OK);
    test_utils::CassFuturePtr insert_future(cass_session_execute(session, insert_statement.get()));
    test_utils::wait_and_check_error(insert_future.get());

    // Create select statement for bound parameters
    std::string select_query =
        str(boost::format("SELECT * FROM %s WHERE tweet_id = ?;") % table_name);
    test_utils::CassStatementPtr select_statement(cass_statement_new(select_query.c_str(), 1));

    // Determine if bound parameters can be used based on C* version
    if (version.major_version == 1) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, select_query);
      select_statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }

    BOOST_REQUIRE(cass_statement_bind_uuid(select_statement.get(), 0, tweet_id) == CASS_OK);
    test_utils::CassFuturePtr select_future(cass_session_execute(session, select_statement.get()));
    test_utils::wait_and_check_error(select_future.get());

    test_utils::CassResultPtr result(cass_future_get_result(select_future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

    // Ensure the test value is NULL
    const CassValue* column_value = cass_row_get_column(cass_result_first_row(result.get()), 1);
    BOOST_REQUIRE(cass_value_is_null(column_value));
    // Handle text alias case
    CassValueType check_type = type;
    if (type == CASS_VALUE_TYPE_TEXT) {
      check_type = CASS_VALUE_TYPE_VARCHAR;
    }
    BOOST_REQUIRE_EQUAL(check_type, cass_data_type_type(cass_value_data_type(column_value)));
    BOOST_REQUIRE_EQUAL(check_type, cass_value_type(column_value));
    T value;
    BOOST_REQUIRE(test_utils::Value<T>::get(column_value, &value) == CASS_ERROR_LIB_NULL_VALUE);

    /*
     * Simple statement validation
     */

    // Generate the appropriate formatted strings for the simple statement
    std::string tweet_id_string = test_utils::generate_random_uuid_str(uuid_gen);

    insert_query = str(boost::format(test_utils::replaceAll(insert_query, "?", "%s")) %
                       tweet_id_string % "NULL");
    insert_statement = test_utils::CassStatementPtr(cass_statement_new(insert_query.c_str(), 0));
    insert_future =
        test_utils::CassFuturePtr(cass_session_execute(session, insert_statement.get()));
    test_utils::wait_and_check_error(insert_future.get());

    select_query =
        str(boost::format(test_utils::replaceAll(select_query, "?", "%s")) % tweet_id_string);
    select_statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 0));
    select_future =
        test_utils::CassFuturePtr(cass_session_execute(session, select_statement.get()));
    test_utils::wait_and_check_error(select_future.get());

    result = test_utils::CassResultPtr(cass_future_get_result(select_future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

    // Ensure the test value is NULL
    column_value = cass_row_get_column(cass_result_first_row(result.get()), 1);
    BOOST_REQUIRE(cass_value_is_null(column_value));
    BOOST_REQUIRE(test_utils::Value<T>::get(column_value, &value) == CASS_ERROR_LIB_NULL_VALUE);
  }

  bool is_result_empty(const CassResult* result) {
    bool is_empty = true;

    // Go through each row in the result and ensure it is empty
    if (result) {
      CassIterator* rows = cass_iterator_from_result(result);
      while (cass_iterator_next(rows)) {
        const CassRow* row = cass_iterator_get_row(rows);
        if (row) {
          is_empty = false;
          break;
        }
      }
      cass_iterator_free(rows);
    }

    return is_empty;
  }
};

BOOST_FIXTURE_TEST_SUITE_WITH_DECOR(basics, BasicTests, *boost::unit_test::disabled())

BOOST_AUTO_TEST_CASE(basic_types) {
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    insert_single_value<cass_int8_t>(CASS_VALUE_TYPE_TINY_INT, 123);
    insert_single_value<cass_int16_t>(CASS_VALUE_TYPE_SMALL_INT, 123);
    insert_single_value<CassDate>(CASS_VALUE_TYPE_DATE,
                                  test_utils::Value<CassDate>::min_value() + 1u);
    insert_single_value<CassTime>(CASS_VALUE_TYPE_TIME, 123);
  }
  insert_single_value<cass_int32_t>(CASS_VALUE_TYPE_INT, 123);

  insert_single_value<cass_int64_t>(CASS_VALUE_TYPE_BIGINT, 1234567890);
  insert_single_value<cass_int64_t>(CASS_VALUE_TYPE_TIMESTAMP, 1234567890);

  insert_single_value<cass_bool_t>(CASS_VALUE_TYPE_BOOLEAN, cass_true);
  insert_single_value<cass_bool_t>(CASS_VALUE_TYPE_BOOLEAN, cass_false);

  insert_single_value<cass_float_t>(CASS_VALUE_TYPE_FLOAT, 3.1415926f);

  insert_single_value<cass_double_t>(CASS_VALUE_TYPE_DOUBLE, 3.141592653589793);

  {
    CassString value("Test Value.");
    insert_single_value<CassString>(CASS_VALUE_TYPE_ASCII, value);
    insert_single_value<CassString>(CASS_VALUE_TYPE_VARCHAR, value);
  }

  {
    CassBytes value = test_utils::bytes_from_string(
        "012345678900123456789001234567890012345678900123456789001234567890");
    insert_single_value<CassBytes>(CASS_VALUE_TYPE_BLOB, value);
    insert_single_value<CassBytes>(CASS_VALUE_TYPE_VARINT, value);
  }

  {
    CassInet value = test_utils::inet_v4_from_int(16777343); // 127.0.0.1
    insert_single_value<CassInet>(CASS_VALUE_TYPE_INET, value);
  }

  {
    CassUuid value;
    cass_uuid_gen_random(uuid_gen, &value);
    insert_single_value<CassUuid>(CASS_VALUE_TYPE_UUID, value);
  }

  {
    CassUuid value;
    cass_uuid_gen_time(uuid_gen, &value);
    insert_single_value<CassUuid>(CASS_VALUE_TYPE_TIMEUUID, value);
  }

  {
    // Pi to a 100 digits
    const cass_int32_t scale = 100;
    const cass_uint8_t varint[] = { 57,  115, 235, 135, 229, 215, 8,   125, 13,  43,  1,
                                    25,  32,  135, 129, 180, 112, 176, 158, 120, 246, 235,
                                    29,  145, 238, 50,  108, 239, 219, 100, 250, 84,  6,
                                    186, 148, 76,  230, 46,  181, 89,  239, 247 };
    CassDecimal value(varint, sizeof(varint), scale);
    insert_single_value<CassDecimal>(CASS_VALUE_TYPE_DECIMAL, value);
  }

  if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
    CassDuration value = CassDuration(0, 0, 0);
    insert_single_value<CassDuration>(CASS_VALUE_TYPE_DURATION, value);

    value = CassDuration(1, 2, 3);
    insert_single_value<CassDuration>(CASS_VALUE_TYPE_DURATION, value);

    value = CassDuration(-1, -2, -3);
    insert_single_value<CassDuration>(CASS_VALUE_TYPE_DURATION, value);
  }
}

BOOST_AUTO_TEST_CASE(min_max) {
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    insert_min_max_value<cass_int8_t>(CASS_VALUE_TYPE_TINY_INT);
    insert_min_max_value<cass_int16_t>(CASS_VALUE_TYPE_SMALL_INT);
    insert_min_max_value<CassDate>(CASS_VALUE_TYPE_DATE);
    insert_min_max_value<CassTime>(CASS_VALUE_TYPE_TIME);
  }
  insert_min_max_value<cass_int32_t>(CASS_VALUE_TYPE_INT);

  insert_min_max_value<cass_int64_t>(CASS_VALUE_TYPE_BIGINT);
  insert_min_max_value<cass_int64_t>(CASS_VALUE_TYPE_TIMESTAMP);

  insert_min_max_value<cass_float_t>(CASS_VALUE_TYPE_FLOAT);

  insert_min_max_value<cass_double_t>(CASS_VALUE_TYPE_DOUBLE);

  insert_min_max_value<CassInet>(CASS_VALUE_TYPE_INET);

  insert_min_max_value<CassUuid>(CASS_VALUE_TYPE_UUID);

  {
    CassUuid value;
    cass_uuid_min_from_time(0, &value);
    insert_single_value<CassUuid>(CASS_VALUE_TYPE_TIMEUUID, value);
  }

  {
    CassUuid value;
    cass_uuid_max_from_time(CASS_UINT64_MAX, &value);
    insert_single_value<CassUuid>(CASS_VALUE_TYPE_TIMEUUID, value);
  }
}

BOOST_AUTO_TEST_CASE(null) {
  insert_null_value<CassString>(CASS_VALUE_TYPE_ASCII);
  insert_null_value<cass_int64_t>(CASS_VALUE_TYPE_BIGINT);
  insert_null_value<CassBytes>(CASS_VALUE_TYPE_BLOB);
  insert_null_value<cass_bool_t>(CASS_VALUE_TYPE_BOOLEAN);
  insert_null_value<CassDecimal>(CASS_VALUE_TYPE_DECIMAL);
  if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
    insert_null_value<CassDuration>(CASS_VALUE_TYPE_DURATION);
  }
  insert_null_value<cass_double_t>(CASS_VALUE_TYPE_DOUBLE);
  insert_null_value<cass_float_t>(CASS_VALUE_TYPE_FLOAT);
  insert_null_value<cass_int32_t>(CASS_VALUE_TYPE_INT);
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    insert_null_value<cass_int8_t>(CASS_VALUE_TYPE_TINY_INT);
    insert_null_value<cass_int16_t>(CASS_VALUE_TYPE_SMALL_INT);
    insert_null_value<CassDate>(CASS_VALUE_TYPE_DATE);
    insert_null_value<CassTime>(CASS_VALUE_TYPE_TIME);
  }
  insert_null_value<CassString>(CASS_VALUE_TYPE_TEXT);
  insert_null_value<cass_int64_t>(CASS_VALUE_TYPE_TIMESTAMP);
  insert_null_value<CassUuid>(CASS_VALUE_TYPE_UUID);
  insert_null_value<CassString>(CASS_VALUE_TYPE_VARCHAR);
  insert_null_value<CassBytes>(CASS_VALUE_TYPE_VARINT);
  insert_null_value<CassUuid>(CASS_VALUE_TYPE_TIMEUUID);
  insert_null_value<CassInet>(CASS_VALUE_TYPE_INET);
  insert_null_value<CassBytes>(CASS_VALUE_TYPE_LIST);
  insert_null_value<CassBytes>(CASS_VALUE_TYPE_MAP);
  insert_null_value<CassBytes>(CASS_VALUE_TYPE_SET);
}

BOOST_AUTO_TEST_CASE(timestamp) {
  test_utils::execute_query(session, "CREATE TABLE test(tweet_id int PRIMARY KEY, test_val int);");

  test_utils::execute_query(session, "INSERT INTO test(tweet_id, test_val) VALUES(0, 42);");
  test_utils::CassResultPtr timestamp_result1;
  test_utils::execute_query(session, "SELECT WRITETIME (test_val) FROM test;", &timestamp_result1);
  BOOST_REQUIRE(cass_result_row_count(timestamp_result1.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(timestamp_result1.get()) == 1);

  cass_int64_t pause_duration = 5 * test_utils::ONE_SECOND_IN_MICROS;
  boost::this_thread::sleep_for(boost::chrono::microseconds(pause_duration));

  test_utils::execute_query(session, "INSERT INTO test(tweet_id, test_val) VALUES(0, 43);");
  test_utils::CassResultPtr timestamp_result2;
  test_utils::execute_query(session, "SELECT WRITETIME (test_val) FROM test;", &timestamp_result2);
  BOOST_REQUIRE(cass_result_row_count(timestamp_result2.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(timestamp_result2.get()) == 1);

  cass_int64_t timestamp1 = 0;
  cass_value_get_int64(cass_row_get_column(cass_result_first_row(timestamp_result1.get()), 0),
                       &timestamp1);

  cass_int64_t timestamp2 = 0;
  cass_value_get_int64(cass_row_get_column(cass_result_first_row(timestamp_result2.get()), 0),
                       &timestamp2);

  BOOST_REQUIRE(timestamp1 != 0 && timestamp2 != 0);
  BOOST_REQUIRE(std::abs(timestamp2 - timestamp1 - pause_duration) < 100000); // Tolerance
}

BOOST_AUTO_TEST_CASE(counters) {
  test_utils::execute_query(session,
                            "CREATE TABLE test(tweet_id int PRIMARY KEY, incdec counter);");

  int tweet_id = 0;
  for (int i = 0; i < 100; ++i) {
    // Create update statement for bound parameters
    std::string update_query =
        str(boost::format("UPDATE %s SET incdec = incdec %s ? WHERE tweet_id = %d;") %
            test_utils::SIMPLE_TABLE % ((i % 2) == 0 ? "-" : "+") % tweet_id);
    test_utils::CassStatementPtr statement(cass_statement_new(update_query.c_str(), 1));

    // Determine if bound parameters can be used based on C* version
    if (version.major_version == 1) {
      update_query = str(boost::format("UPDATE %s SET incdec = incdec %s %d WHERE tweet_id = %d;") %
                         test_utils::SIMPLE_TABLE % ((i % 2) == 0 ? "-" : "+") % i % tweet_id);
      statement = test_utils::CassStatementPtr(cass_statement_new(update_query.c_str(), 0));
    } else {
      BOOST_REQUIRE(cass_statement_bind_int64(statement.get(), 0, i) == CASS_OK);
    }

    test_utils::CassFuturePtr result_future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(result_future.get());
  }

  std::string select_query = str(boost::format("SELECT * FROM %s;") % test_utils::SIMPLE_TABLE);
  test_utils::CassResultPtr result;
  test_utils::execute_query(session, select_query, &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(result.get()) > 0);

  cass_int64_t counter_value;
  BOOST_REQUIRE(cass_value_get_int64(cass_row_get_column(cass_result_first_row(result.get()), 1),
                                     &counter_value) == CASS_OK);
  BOOST_REQUIRE(counter_value == 50);
}

BOOST_AUTO_TEST_CASE(rows_in_rows_out) {
  CassConsistency consistency = CASS_CONSISTENCY_ONE;

  // TODO(mpenick): In "master" were there more tests planned here?

  {
    test_utils::execute_query(
        session,
        str(boost::format(
                "CREATE TABLE %s (tweet_id bigint PRIMARY KEY, t1 bigint, t2 bigint, t3 bigint);") %
            test_utils::SIMPLE_TABLE),
        NULL, consistency);

    // Create insert statement for bound parameters
    std::string insert_query(
        boost::str(boost::format("INSERT INTO %s (tweet_id, t1, t2, t3) VALUES (?, ?, ?, ?);") %
                   test_utils::SIMPLE_TABLE));

    const size_t num_rows = 1000;
    for (size_t i = 0; i < num_rows; ++i) {
      test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 4));

      // Determine if bound parameters can be used based on C* version
      if (version.major_version == 1) {
        std::string insert_query(boost::str(
            boost::format("INSERT INTO %s (tweet_id, t1, t2, t3) VALUES (%s, %s, %s, %s);") %
            test_utils::SIMPLE_TABLE % i % (i + 1) % (i + 2) % (i + 3)));
        statement = test_utils::CassStatementPtr(cass_statement_new(insert_query.c_str(), 0));
      } else {
        BOOST_REQUIRE(test_utils::Value<cass_int64_t>::bind(statement.get(), 0, i) == CASS_OK);
        BOOST_REQUIRE(test_utils::Value<cass_int64_t>::bind(statement.get(), 1, i + 1) == CASS_OK);
        BOOST_REQUIRE(test_utils::Value<cass_int64_t>::bind(statement.get(), 2, i + 2) == CASS_OK);
        BOOST_REQUIRE(test_utils::Value<cass_int64_t>::bind(statement.get(), 3, i + 3) == CASS_OK);
      }

      cass_statement_set_consistency(statement.get(), consistency);
      test_utils::CassFuturePtr result_future(cass_session_execute(session, statement.get()));
      test_utils::wait_and_check_error(result_future.get(), 30 * test_utils::ONE_SECOND_IN_MICROS);
    }

    std::string select_query(str(boost::format("SELECT tweet_id, t1, t2, t3 FROM %s LIMIT %d;") %
                                 test_utils::SIMPLE_TABLE % num_rows));
    test_utils::CassResultPtr result;
    test_utils::execute_query(session, select_query, &result, consistency);
    BOOST_REQUIRE(cass_result_row_count(result.get()) == num_rows);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 4);

    test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));
    size_t row_count = 0;
    while (cass_iterator_next(iterator.get())) {
      cass_int64_t tweet_id = 0;
      cass_int64_t t1 = 0, t2 = 0, t3 = 0;
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::get(
                        cass_row_get_column(cass_iterator_get_row(iterator.get()), 0), &tweet_id) ==
                    CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::get(
                        cass_row_get_column(cass_iterator_get_row(iterator.get()), 1), &t1) ==
                    CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::get(
                        cass_row_get_column(cass_iterator_get_row(iterator.get()), 2), &t2) ==
                    CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::get(
                        cass_row_get_column(cass_iterator_get_row(iterator.get()), 3), &t3) ==
                    CASS_OK);
      BOOST_REQUIRE(t1 == tweet_id + 1 && t2 == tweet_id + 2 && t3 == tweet_id + 3);
      row_count++;
    }

    BOOST_REQUIRE(row_count == num_rows);
  }
}

BOOST_AUTO_TEST_CASE(column_name) {
  test_utils::execute_query(
      session, "CREATE TABLE test (key int PRIMARY KEY, v1 text, v2 int, v3 bigint, v4 float);");
  test_utils::execute_query(
      session, "INSERT INTO test (key, v1, v2, v3, v4) VALUES (0, 'abc', 123, 456, 0.123456);");

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, "SELECT * FROM test;", &result);

  BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);

  CassString key, v1, v2, v3, v4;
  cass_result_column_name(result.get(), 0, &key.data, &key.length);
  cass_result_column_name(result.get(), 1, &v1.data, &v1.length);
  cass_result_column_name(result.get(), 2, &v2.data, &v2.length);
  cass_result_column_name(result.get(), 3, &v3.data, &v3.length);
  cass_result_column_name(result.get(), 4, &v4.data, &v4.length);

  BOOST_REQUIRE(strncmp(key.data, "key", key.length) == 0);
  BOOST_REQUIRE(strncmp(v1.data, "v1", v1.length) == 0);
  BOOST_REQUIRE(strncmp(v2.data, "v2", v2.length) == 0);
  BOOST_REQUIRE(strncmp(v3.data, "v3", v3.length) == 0);
  BOOST_REQUIRE(strncmp(v4.data, "v4", v4.length) == 0);
}

/**
 * Empty Results From Executed Statements
 *
 * This test is for ensuring the result set is empty (CassRow* == NULL) when
 * executing statements that do not return values from Cassandra.
 *
 * @since 1.0.0-rc1
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(empty_results) {
  test_utils::CassResultPtr result;
  test_utils::execute_query(session, "CREATE TABLE test (key int, value int, PRIMARY KEY (key))",
                            &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 0);
  BOOST_REQUIRE(is_result_empty(result.get()));

  result.reset();
  test_utils::execute_query(session, "INSERT INTO test (key, value) VALUES (0, 0)", &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 0);
  BOOST_REQUIRE(is_result_empty(result.get()));

  result.reset();
  test_utils::execute_query(session, "DELETE FROM test WHERE key=0", &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 0);
  BOOST_REQUIRE(is_result_empty(result.get()));

  result.reset();
  test_utils::execute_query(session, "SELECT * FROM test WHERE key=0", &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 0);
  BOOST_REQUIRE(is_result_empty(result.get()));
}

/**
 * Unset Parameters
 *
 * This test ensures that unset parameters return an error for C* 2.2 or less
 * and correctly uses the UNSET values for C*2.2 or greater.
 *
 * @since 2.2.0-beta1
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(unset_parameters) {
  std::string table_name =
      str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));

  test_utils::execute_query(
      session,
      str(boost::format("CREATE TABLE %s (key text PRIMARY KEY, value text)") % table_name));

  std::string insert_query =
      str(boost::format("INSERT INTO %s (key, value) VALUES(?, ?)") % table_name);
  test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));

  // Determine if bound parameters can be used based on C* version
  if (version.major_version == 1) {
    test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query);
    statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
  }

  // Insert known values
  test_utils::execute_query(
      session,
      str(boost::format("INSERT INTO %s (key, value) VALUES ('key1', 'value1')") % table_name));

  cass_statement_bind_string(statement.get(), 0, "key1");

  // Don't bind the second parameter

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));

  CassError rc = cass_future_error_code(future.get());

  if (version >= "2.2.0") {
    // C* 2.2+ uses the value UNSET and that makes this statement a no-op
    BOOST_REQUIRE(rc == CASS_OK);
  } else {
    BOOST_REQUIRE(rc == CASS_ERROR_LIB_PARAMETER_UNSET);
  }

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s") % table_name), &result);

  // Check to make sure the known values are still present
  CassString key;
  CassString value;
  const CassRow* row = cass_result_first_row(result.get());
  test_utils::Value<CassString>::get(cass_row_get_column(row, 0), &key);
  test_utils::Value<CassString>::get(cass_row_get_column(row, 1), &value);
  BOOST_CHECK(test_utils::Value<CassString>::equal(key, CassString("key1")));
  BOOST_CHECK(test_utils::Value<CassString>::equal(value, CassString("value1")));
}

/**
 * Bind a blob as a string (char*).
 *
 * Verify that a "blob" can be bound using the string function. This was
 * allowed in previous versions of the driver.
 *
 * @since 2.3
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(bind_blob_as_string) {
  std::string table_name =
      str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));

  test_utils::execute_query(
      session,
      str(boost::format("CREATE TABLE %s (key text PRIMARY KEY, value blob)") % table_name));

  std::string insert_query =
      str(boost::format("INSERT INTO %s (key, value) VALUES(?, ?)") % table_name);

  // Prepared needed to validate bind type information
  test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query);
  test_utils::CassStatementPtr statement(
      test_utils::CassStatementPtr(cass_prepared_bind(prepared.get())));

  BOOST_CHECK_EQUAL(cass_statement_bind_string(statement.get(), 0, "key1"), CASS_OK);

  // Verify that a blob can be bound as a string
  BOOST_CHECK_EQUAL(cass_statement_bind_string(statement.get(), 1, "value1"), CASS_OK);

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));

  CassError rc = cass_future_error_code(future.get());
  BOOST_REQUIRE(rc == CASS_OK);

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s") % table_name), &result);

  // Check to make sure the known values are still present
  CassString key;
  CassString value;
  const CassRow* row = cass_result_first_row(result.get());
  test_utils::Value<CassString>::get(cass_row_get_column(row, 0), &key);
  test_utils::Value<CassString>::get(cass_row_get_column(row, 1), &value);
  BOOST_CHECK(test_utils::Value<CassString>::equal(key, CassString("key1")));
  BOOST_CHECK(test_utils::Value<CassString>::equal(value, CassString("value1")));
}

BOOST_AUTO_TEST_SUITE_END()
