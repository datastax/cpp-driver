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

#include <string>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>

#include "cassandra.h"
#include "execute_request.hpp"
#include "statement.hpp"
#include "test_utils.hpp"

/**
 * Test harness for prepared from existing functionality.
 */
struct PreparedFromExistingTests : public test_utils::SingleSessionTest {
  PreparedFromExistingTests()
    : SingleSessionTest(1, 0)
    , keyspace(str(boost::format("ks_%s") % test_utils::generate_unique_str(uuid_gen))) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % keyspace
                                           % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % keyspace));
    test_utils::execute_query(session, "CREATE TABLE test (k text PRIMARY KEY, v text)");
    test_utils::execute_query(session, "INSERT INTO test (k, v) VALUES ('key1', 'value1')");
  }

  /**
   * Validate the result of the provided future.
   *
   * @param future The result future to validate
   */
  void validate_query_result(test_utils::CassFuturePtr future) {
    BOOST_REQUIRE(future);
    BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

    const CassResult* result = cass_future_get_result(future.get());
    BOOST_REQUIRE(result != NULL);
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result), 1u);
    BOOST_REQUIRE_EQUAL(cass_result_column_count(result), 1u);

    const CassRow* row = cass_result_first_row(result);
    BOOST_REQUIRE(row != NULL);

    const char* value;
    size_t value_length;
    BOOST_REQUIRE_EQUAL(cass_value_get_string(cass_row_get_column(row, 0), &value, &value_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(value, value_length), "value1");
  }

  /**
   * The test's keyspace
   */
  std::string keyspace;
};

BOOST_FIXTURE_TEST_SUITE(prepared_existing, PreparedFromExistingTests)

/**
 * Verify that a statement is correctly prepared from an existing simple
 * statement. The settings from the original statement should be inherited.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(prepare_from_existing_simple_statement) {
  test_utils::CassStatementPtr statement(cass_statement_new("SELECT v FROM test WHERE k = 'key1'", 0));

  test_utils::CassRetryPolicyPtr retry_policy(cass_retry_policy_downgrading_consistency_new());

  // Set unique settings to validate later
  BOOST_CHECK_EQUAL(cass_statement_set_consistency(statement.get(), CASS_CONSISTENCY_LOCAL_QUORUM), CASS_OK);
  BOOST_CHECK_EQUAL(cass_statement_set_serial_consistency(statement.get(), CASS_CONSISTENCY_SERIAL), CASS_OK);
  BOOST_CHECK_EQUAL(cass_statement_set_request_timeout(statement.get(), 99999u), CASS_OK);
  BOOST_CHECK_EQUAL(cass_statement_set_retry_policy(statement.get(), retry_policy.get()), CASS_OK);

  // Prepare from the existing simple statement
  test_utils::CassFuturePtr future(cass_session_prepare_from_existing(session, statement.get()));
  BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(future.get()));
  BOOST_REQUIRE(prepared);

  test_utils::CassStatementPtr bound_statement(cass_prepared_bind(prepared.get()));
  BOOST_REQUIRE(bound_statement);

  cass::ExecuteRequest* execute_request = static_cast<cass::ExecuteRequest*>(bound_statement->from());

  // Validate that the bound statement inherited the settings from the original statement
  BOOST_CHECK_EQUAL(execute_request->consistency(), CASS_CONSISTENCY_LOCAL_QUORUM);
  BOOST_CHECK_EQUAL(execute_request->serial_consistency(), CASS_CONSISTENCY_SERIAL);
  BOOST_CHECK_EQUAL(execute_request->request_timeout_ms(), 99999u);
  BOOST_CHECK_EQUAL(execute_request->retry_policy().get(), retry_policy.get());

  validate_query_result(test_utils::CassFuturePtr(cass_session_execute(session, bound_statement.get())));
}

/**
 * Verify that a statement is correctly prepared from an existing bound
 * statement. The settings from the original bound statement should be
 * inherited.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(prepare_from_existing_bound_statement) {
  test_utils::CassFuturePtr future1(cass_session_prepare(session, "SELECT v FROM test WHERE k = 'key1'"));
  BOOST_REQUIRE_EQUAL(cass_future_error_code(future1.get()), CASS_OK);

  test_utils::CassPreparedPtr prepared1(cass_future_get_prepared(future1.get()));
  BOOST_REQUIRE(prepared1);

  test_utils::CassStatementPtr bound_statement1(cass_prepared_bind(prepared1.get()));
  BOOST_REQUIRE(bound_statement1);

  test_utils::CassRetryPolicyPtr retry_policy(cass_retry_policy_downgrading_consistency_new());

  // Set unique settings to validate later
  BOOST_CHECK_EQUAL(cass_statement_set_consistency(bound_statement1.get(), CASS_CONSISTENCY_LOCAL_QUORUM), CASS_OK);
  BOOST_CHECK_EQUAL(cass_statement_set_serial_consistency(bound_statement1.get(), CASS_CONSISTENCY_SERIAL), CASS_OK);
  BOOST_CHECK_EQUAL(cass_statement_set_request_timeout(bound_statement1.get(), 99999u), CASS_OK);
  BOOST_CHECK_EQUAL(cass_statement_set_retry_policy(bound_statement1.get(), retry_policy.get()), CASS_OK);

  // Prepare from the existing bound statement
  test_utils::CassFuturePtr future2(cass_session_prepare_from_existing(session, bound_statement1.get()));
  BOOST_REQUIRE_EQUAL(cass_future_error_code(future2.get()), CASS_OK);

  test_utils::CassPreparedPtr prepared2(cass_future_get_prepared(future2.get()));
  BOOST_REQUIRE(prepared2);

  test_utils::CassStatementPtr bound_statement2(cass_prepared_bind(prepared2.get()));
  BOOST_REQUIRE(bound_statement2);

  cass::ExecuteRequest* execute_request = static_cast<cass::ExecuteRequest*>(bound_statement2->from());

  // Validate that the bound statement inherited the settings from the original bound statement
  BOOST_CHECK_EQUAL(execute_request->consistency(), CASS_CONSISTENCY_LOCAL_QUORUM);
  BOOST_CHECK_EQUAL(execute_request->serial_consistency(), CASS_CONSISTENCY_SERIAL);
  BOOST_CHECK_EQUAL(execute_request->request_timeout_ms(), 99999u);
  BOOST_CHECK_EQUAL(execute_request->retry_policy().get(), retry_policy.get());

  validate_query_result(test_utils::CassFuturePtr(cass_session_execute(session, bound_statement2.get())));
}

BOOST_AUTO_TEST_SUITE_END()
