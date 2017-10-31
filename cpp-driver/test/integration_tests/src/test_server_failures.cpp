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

#include "test_utils.hpp"
#include "cassandra.h"

#include <boost/format.hpp>
#include <boost/test/unit_test.hpp>

struct ServerFailuresTest : public test_utils::SingleSessionTest {
public:
  ServerFailuresTest() : test_utils::SingleSessionTest(1, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }

  ~ServerFailuresTest() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(session,
      str(boost::format(test_utils::DROP_KEYSPACE_FORMAT)
      % test_utils::SIMPLE_KEYSPACE));
  }
};

BOOST_AUTO_TEST_SUITE(server_failures)

/**
 *
 * Validate UDF Function_failures are returned from Cassandra
 *
 * Create a function that will throw an exception when invoked; ensure the
 * Function_failure is returned from Cassandra.
 *
 * @since 2.2.0-beta
 * @jira_ticket CPP-294
 * @test_category queries:basic
 * @cassandra_version 2.2.x
 */
 BOOST_AUTO_TEST_CASE(function_failure) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    ServerFailuresTest tester;
    std::string create_table = "CREATE TABLE server_function_failures (id int PRIMARY KEY, value double)";
    std::string insert_query = "INSERT INTO server_function_failures(id, value) VALUES (?, ?)";
    std::string failing_function = "CREATE FUNCTION " + test_utils::SIMPLE_KEYSPACE + ".function_failure(value double) RETURNS NULL ON NULL INPUT RETURNS double LANGUAGE java AS 'throw new RuntimeException(\"failure\");'";
    std::string select_query = "SELECT function_failure(value) FROM server_function_failures WHERE id = ?";

    // Create the table and associated failing function
    test_utils::execute_query(tester.session, create_table.c_str());
    test_utils::execute_query(tester.session, failing_function.c_str());

    // Bind and insert values into Cassandra
    test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 2));
    BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::bind_by_name(statement.get(), "id", 1), CASS_OK);
    BOOST_REQUIRE_EQUAL(test_utils::Value<cass_double_t>::bind_by_name(statement.get(), "value", 3.14), CASS_OK);
    test_utils::wait_and_check_error(test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());

    // Execute the failing function
    statement = test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 1));
    BOOST_REQUIRE_EQUAL(test_utils::Value<cass_int32_t>::bind(statement.get(), 0, 1), CASS_OK);
    CassError error_code = test_utils::wait_and_return_error(test_utils::CassFuturePtr(cass_session_execute(tester.session, statement.get())).get());
    BOOST_REQUIRE_EQUAL(error_code, CASS_ERROR_SERVER_FUNCTION_FAILURE);
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping server_failures/function_failure" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 *
 * Validate Already_exists failures are returned from Cassandra
 *
 * Create two identical tables and functions; Ensure Already_exist is returned
 * from Cassandra
 *
 * @since 2.2.0-beta
 * @jira_ticket CPP-294
 * @test_category queries:basic
 * @cassandra_version 2.2.x
 */
BOOST_AUTO_TEST_CASE(already_exists) {
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    ServerFailuresTest tester;
    std::string create_table = "CREATE TABLE already_exists_table (id int PRIMARY KEY, value double)";
    std::string create_keyspace = str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1");

    // Create the table
    BOOST_REQUIRE_EQUAL(test_utils::execute_query_with_error(tester.session, create_table.c_str()), CASS_OK);

    // Ensure Cassandra returns Already_exists for table and keyspace
    BOOST_REQUIRE_EQUAL(test_utils::execute_query_with_error(tester.session, create_table.c_str()), CASS_ERROR_SERVER_ALREADY_EXISTS);
    BOOST_REQUIRE_EQUAL(test_utils::execute_query_with_error(tester.session, create_keyspace.c_str()), CASS_ERROR_SERVER_ALREADY_EXISTS);
  }
  else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping server_failures/already_exists" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
