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

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

#define INSERT_BY_NAME "INSERT INTO by_name (key, a, b, c) VALUES (?, ?, ?, ?)"
#define INSERT_BY_NAME_CASE_SENSITIVE "INSERT INTO by_name (key, abc, \"ABC\", \"aBc\") VALUES (?, ?, ?, ?)"
#define INSERT_BY_NAME_NULL "INSERT INTO by_name (key, a, b, c, abc, \"ABC\", \"aBc\") VALUES (?, ?, ?, ?, ?, ?, ?)"

struct ByNameTests : public test_utils::SingleSessionTest {
  ByNameTests() : test_utils::SingleSessionTest(1, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

    test_utils::execute_query(session, "CREATE TABLE by_name (key uuid PRIMARY KEY, a int, b boolean, c text, abc float, \"ABC\" float, \"aBc\" float)");

    test_utils::execute_query(session, "CREATE TABLE bytes_by_name (key uuid PRIMARY KEY, blobs blob, varints varint)");
  }

  ~ByNameTests() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(session,
      str(boost::format(test_utils::DROP_KEYSPACE_FORMAT)
        % test_utils::SIMPLE_KEYSPACE));
  }

  test_utils::CassResultPtr select_all_from_by_name() {
    test_utils::CassResultPtr result;
    test_utils::execute_query(session, "SELECT * FROM by_name", &result);
    BOOST_REQUIRE(cass_result_column_count(result.get()) ==  7);
    BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);
    return result;
  }

  test_utils::CassResultPtr select_all_from_bytes_by_name() {
    test_utils::CassResultPtr result;
    test_utils::execute_query(session, "SELECT * FROM bytes_by_name", &result);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 3);
    BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);
    return result;
  }

  void test_bind_and_get(test_utils::CassStatementPtr statement) {
    CassUuid key = test_utils::generate_time_uuid(uuid_gen);

    BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid_by_name(statement.get(), "key", key), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_int32_by_name(statement.get(), "a", 9042), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_bool_by_name(statement.get(), "b", cass_true), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_string_by_name(statement.get(), "c", "xyz"), CASS_OK);

    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());

    test_utils::CassResultPtr result = select_all_from_by_name();

    const CassRow* row = cass_result_first_row(result.get());

    const CassValue* value;

    value = cass_row_get_column_by_name(row, "key");
    BOOST_REQUIRE(value != NULL);

    CassUuid result_key;
    BOOST_REQUIRE_EQUAL(cass_value_get_uuid(value, &result_key), CASS_OK);
    BOOST_CHECK(test_utils::Value<CassUuid>::equal(result_key, key));

    value = cass_row_get_column_by_name(row, "a");
    BOOST_REQUIRE(value != NULL);

    cass_int32_t a;
    BOOST_REQUIRE_EQUAL(cass_value_get_int32(value, &a), CASS_OK);
    BOOST_CHECK_EQUAL(a, 9042);

    value = cass_row_get_column_by_name(row, "b");
    BOOST_REQUIRE(value != NULL);

    cass_bool_t b;
    BOOST_REQUIRE_EQUAL(cass_value_get_bool(value, &b), CASS_OK);
    BOOST_CHECK_EQUAL(b, cass_true);

    value = cass_row_get_column_by_name(row, "c");
    BOOST_REQUIRE(value != NULL);

    CassString c;
    BOOST_REQUIRE_EQUAL(cass_value_get_string(value, &c.data, &c.length), CASS_OK);
    BOOST_CHECK(test_utils::Value<CassString>::equal(c, "xyz"));
  }

  void test_bind_and_get_case_sensitive(test_utils::CassStatementPtr statement) {
    CassUuid key = test_utils::generate_time_uuid(uuid_gen);

    BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid_by_name(statement.get(), "key", key), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_float_by_name(statement.get(), "\"abc\"", 1.1f), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_float_by_name(statement.get(), "\"ABC\"", 2.2f), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_float_by_name(statement.get(), "\"aBc\"", 3.3f), CASS_OK);

    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());

    test_utils::CassResultPtr result = select_all_from_by_name();

    const CassRow* row = cass_result_first_row(result.get());

    const CassValue* value;

    value = cass_row_get_column_by_name(row, "key");
    BOOST_REQUIRE(value != NULL);

    CassUuid result_key;
    BOOST_REQUIRE_EQUAL(cass_value_get_uuid(value, &result_key), CASS_OK);
    BOOST_CHECK(test_utils::Value<CassUuid>::equal(result_key, key));

    value = cass_row_get_column_by_name(row, "\"abc\"");
    BOOST_REQUIRE(value != NULL);

    cass_float_t abc;
    BOOST_REQUIRE_EQUAL(cass_value_get_float(value, &abc), CASS_OK);
    BOOST_CHECK(abc == 1.1f);

    value = cass_row_get_column_by_name(row, "\"ABC\"");
    BOOST_REQUIRE(value != NULL);

    BOOST_REQUIRE_EQUAL(cass_value_get_float(value, &abc), CASS_OK);
    BOOST_CHECK(abc == 2.2f);

    value = cass_row_get_column_by_name(row, "\"aBc\"");
    BOOST_REQUIRE(value != NULL);

    BOOST_REQUIRE_EQUAL(cass_value_get_float(value, &abc), CASS_OK);
    BOOST_CHECK(abc == 3.3f);
  }

  void test_null(test_utils::CassStatementPtr statement) {
    CassUuid key = test_utils::generate_time_uuid(uuid_gen);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid_by_name(statement.get(), "key", key), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_null_by_name(statement.get(), "a"), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_null_by_name(statement.get(), "b"), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_null_by_name(statement.get(), "c"), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_null_by_name(statement.get(), "abc"), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_null_by_name(statement.get(), "\"ABC\""), CASS_OK);
    BOOST_REQUIRE_EQUAL(cass_statement_bind_null_by_name(statement.get(), "\"aBc\""), CASS_OK);

    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());

    test_utils::CassResultPtr result = select_all_from_by_name();
    const CassRow* row = cass_result_first_row(result.get());

    BOOST_CHECK(cass_value_is_null(cass_row_get_column_by_name(row, "a")));
    BOOST_CHECK(cass_value_is_null(cass_row_get_column_by_name(row, "b")));
    BOOST_CHECK(cass_value_is_null(cass_row_get_column_by_name(row, "c")));
    BOOST_CHECK(cass_value_is_null(cass_row_get_column_by_name(row, "abc")));
    BOOST_CHECK(cass_value_is_null(cass_row_get_column_by_name(row, "\"ABC\"")));
    BOOST_CHECK(cass_value_is_null(cass_row_get_column_by_name(row, "\"aBc\"")));
  }
};

BOOST_FIXTURE_TEST_SUITE(by_name, ByNameTests)

BOOST_AUTO_TEST_CASE(bind_and_get_prepared)
{
  test_utils::CassPreparedPtr prepared = test_utils::prepare(session, INSERT_BY_NAME);
  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
  test_bind_and_get(statement);
}

BOOST_AUTO_TEST_CASE(bind_and_get)
{
  if (version >= "2.1.0") {
    test_utils::CassStatementPtr statement(cass_statement_new(INSERT_BY_NAME, 4));
    test_bind_and_get(statement);
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping by_name/bind_and_get" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_CASE(bind_and_get_case_sensitive_prepared)
{
  test_utils::CassPreparedPtr prepared = test_utils::prepare(session, INSERT_BY_NAME_CASE_SENSITIVE);
  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
  test_bind_and_get_case_sensitive(statement);
}

BOOST_AUTO_TEST_CASE(bind_and_get_case_sensitive)
{
  if (version >= "2.1.0") {
    test_utils::CassStatementPtr statement(cass_statement_new(INSERT_BY_NAME_CASE_SENSITIVE, 4));
    test_bind_and_get_case_sensitive(statement);
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping by_name/bind_and_get_case_sensitive" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_CASE(bind_multiple_columns)
{
  test_utils::CassPreparedPtr prepared = test_utils::prepare(session, "INSERT INTO by_name (key, abc, \"ABC\", \"aBc\") VALUES (?, ?, ?, ?)");

  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));

  CassUuid key = test_utils::generate_time_uuid(uuid_gen);

  BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid_by_name(statement.get(), "key", key), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_statement_bind_float_by_name(statement.get(), "abc", 1.23f), CASS_OK);

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
  test_utils::wait_and_check_error(future.get());

  test_utils::CassResultPtr result = select_all_from_by_name();

  const CassRow* row = cass_result_first_row(result.get());

  const CassValue* value;

  value = cass_row_get_column_by_name(row, "key");
  BOOST_REQUIRE(value != NULL);

  CassUuid result_key;
  BOOST_REQUIRE_EQUAL(cass_value_get_uuid(value, &result_key), CASS_OK);
  BOOST_CHECK(test_utils::Value<CassUuid>::equal(result_key, key));

  value = cass_row_get_column_by_name(row, "\"abc\"");
  BOOST_REQUIRE(value != NULL);

  cass_float_t abc;
  BOOST_REQUIRE_EQUAL(cass_value_get_float(value, &abc), CASS_OK);
  BOOST_CHECK(abc == 1.23f);

  value = cass_row_get_column_by_name(row, "\"ABC\"");
  BOOST_REQUIRE(value != NULL);

  BOOST_REQUIRE_EQUAL(cass_value_get_float(value, &abc), CASS_OK);
  BOOST_CHECK(abc == 1.23f);

  value = cass_row_get_column_by_name(row, "\"aBc\"");
  BOOST_REQUIRE(value != NULL);

  BOOST_REQUIRE_EQUAL(cass_value_get_float(value, &abc), CASS_OK);
  BOOST_CHECK(abc == 1.23f);
}

BOOST_AUTO_TEST_CASE(bind_invalid_name)
{
  test_utils::CassPreparedPtr prepared = test_utils::prepare(session, "INSERT INTO by_name (key, a, b, c, abc, \"ABC\", \"aBc\") VALUES (?, ?, ?, ?, ?, ?, ?)");

  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));

  BOOST_REQUIRE_EQUAL(cass_statement_bind_int32_by_name(statement.get(), "d", 0), CASS_ERROR_LIB_NAME_DOES_NOT_EXIST);
  BOOST_REQUIRE_EQUAL(cass_statement_bind_float_by_name(statement.get(), "\"aBC\"", 0.0), CASS_ERROR_LIB_NAME_DOES_NOT_EXIST);
  BOOST_REQUIRE_EQUAL(cass_statement_bind_float_by_name(statement.get(), "\"abC\"", 0.0), CASS_ERROR_LIB_NAME_DOES_NOT_EXIST);
}

BOOST_AUTO_TEST_CASE(get_invalid_name)
{
  // Create insert statement for bound parameters
  std::string insert_query = "INSERT INTO by_name (key, a) VALUES (?, ?)";
  test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));

  // Determine if bound parameters can be used based on C* version
  if (version.major_version == 1) {
    test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query.c_str());
    statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
  }

  CassUuid key = test_utils::generate_time_uuid(uuid_gen);

  BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid(statement.get(), 0, key), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_statement_bind_int32(statement.get(), 1, 9042), CASS_OK);

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
  test_utils::wait_and_check_error(future.get());

  test_utils::CassResultPtr result = select_all_from_by_name();

  const CassRow* row = cass_result_first_row(result.get());

  BOOST_CHECK(cass_row_get_column_by_name(row, "d") == NULL);
  BOOST_CHECK(cass_row_get_column_by_name(row, "\"aBC\"") == NULL);
  BOOST_CHECK(cass_row_get_column_by_name(row, "\"abC\"") == NULL);
}

BOOST_AUTO_TEST_CASE(null_prepared)
{
  test_utils::CassPreparedPtr prepared = test_utils::prepare(session, INSERT_BY_NAME_NULL);
  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
  test_null(statement);
}

BOOST_AUTO_TEST_CASE(null)
{
  if (version >= "2.1.0") {
    test_utils::CassStatementPtr statement(cass_statement_new(INSERT_BY_NAME_NULL, 7));
    test_null(statement);
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping by_name/null" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 * Bind bytes by name
 *
 * This test handles the binding of a prepared statement with a
 * CASS_VALUE_TYPE_BLOB and CASS_VALUE_TYPE_VARINT datatype.
 *
 * @since 2.1.0-beta
 * @test_category prepared_statements:binding
 * @jira_ticket CPP-272 [https://datastax-oss.atlassian.net/browse/CPP-272]
 */
BOOST_AUTO_TEST_CASE(bind_bytes_by_name)
{
  test_utils::CassPreparedPtr prepared = test_utils::prepare(session, "INSERT INTO bytes_by_name (key, blobs, varints) VALUES (?, ?, ?)");
  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));

  CassUuid key = test_utils::generate_time_uuid(uuid_gen);
  CassBytes blob;
  blob.data = reinterpret_cast<const cass_byte_t *>("68971169783116971203269110116101114112114105115101329911211245100114105118101114");
  blob.size = strlen(reinterpret_cast<const char*>(blob.data));
  CassBytes varint;
  varint.data = reinterpret_cast<const cass_byte_t *>("1234567890123456789012345678901234567890");
  varint.size = strlen(reinterpret_cast<const char*>(varint.data));

  BOOST_REQUIRE_EQUAL(cass_statement_bind_uuid_by_name(statement.get(), "key", key), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_statement_bind_bytes_by_name(statement.get(), "blobs", const_cast<cass_byte_t *>(blob.data), blob.size), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_statement_bind_bytes_by_name(statement.get(), "varints", const_cast<cass_byte_t *>(varint.data), varint.size), CASS_OK);

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
  test_utils::wait_and_check_error(future.get());
  test_utils::CassResultPtr result = select_all_from_bytes_by_name();
  const CassRow* row = cass_result_first_row(result.get());

  const CassValue* value = cass_row_get_column_by_name(row, "key");
  CassUuid result_key;
  BOOST_REQUIRE(value != NULL);
  BOOST_REQUIRE_EQUAL(cass_value_get_uuid(value, &result_key), CASS_OK);
  BOOST_CHECK(test_utils::Value<CassUuid>::equal(result_key, key));

  value = cass_row_get_column_by_name(row, "blobs");
  CassBytes result_blob;
  BOOST_REQUIRE(value != NULL);
  BOOST_REQUIRE_EQUAL(cass_value_get_bytes(value, &result_blob.data, &result_blob.size), CASS_OK);
  BOOST_REQUIRE(test_utils::Value<CassBytes>::equal(blob, result_blob));

  value = cass_row_get_column_by_name(row, "varints");
  CassBytes result_varint;
  BOOST_REQUIRE(value != NULL);
  BOOST_REQUIRE_EQUAL(cass_value_get_bytes(value, &result_varint.data, &result_varint.size), CASS_OK);
  BOOST_REQUIRE(test_utils::Value<CassBytes>::equal(varint, result_varint));
}

BOOST_AUTO_TEST_SUITE_END()
