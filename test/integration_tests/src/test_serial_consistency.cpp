/*
  Copyright (c) 2014-2015 DataStax

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

#ifdef STAND_ALONE
# define BOOST_TEST_MODULE cassandra
#endif

#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"
#include "policy_tools.hpp"

#include "cassandra.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp>
#include <boost/format.hpp>

struct SerialConsistencyTests : public test_utils::SingleSessionTest {
  SerialConsistencyTests() : test_utils::SingleSessionTest(1, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
    test_utils::execute_query(session, "CREATE TABLE test (key text PRIMARY KEY, value int);");
  }
};

BOOST_FIXTURE_TEST_SUITE(serial_consistency, SerialConsistencyTests)

test_utils::CassFuturePtr insert_row(CassSession* session, const std::string& key, int value, CassConsistency serial_consistency) {
  const char* insert_query = "INSERT INTO test (key, value) VALUES (?, ?) IF NOT EXISTS;";

  test_utils::CassStatementPtr statement(cass_statement_new(insert_query, 2));
  cass_statement_bind_string_n(statement.get(), 0, key.data(), key.size());
  cass_statement_bind_int32(statement.get(), 1, value);

  cass_statement_set_serial_consistency(statement.get(), serial_consistency);
  return test_utils::CassFuturePtr(cass_session_execute(session, statement.get()));
}

BOOST_AUTO_TEST_CASE(simple)
{
  for (int i = 0; i < 2; ++i) {
    test_utils::CassFuturePtr future = insert_row(session, "abc", 99, CASS_CONSISTENCY_SERIAL);
    BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);
    test_utils::CassResultPtr result(cass_future_get_result(future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);
    const CassValue* value = cass_row_get_column(cass_result_first_row(result.get()), 0);
    cass_bool_t applied;
    cass_value_get_bool(value, &applied);
    BOOST_REQUIRE_EQUAL(applied, i == 0 ? cass_true : cass_false);
  }
}

BOOST_AUTO_TEST_CASE(invalid)
{
  test_utils::CassFuturePtr future = insert_row(session, "abc", 99, CASS_CONSISTENCY_ONE); // Invalid

  CassError code = cass_future_error_code(future.get());
  BOOST_REQUIRE_EQUAL(code, CASS_ERROR_SERVER_INVALID_QUERY);

  CassString message;
  cass_future_error_message(future.get(), &message.data, &message.length);
  BOOST_REQUIRE(strncmp(message.data, "Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL", message.length) == 0);
}

BOOST_AUTO_TEST_SUITE_END()
