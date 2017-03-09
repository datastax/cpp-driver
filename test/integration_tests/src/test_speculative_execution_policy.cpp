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

#include "request_handler.hpp"
#include "statement.hpp"

#include <algorithm>

#include <boost/format.hpp>
#include <boost/test/unit_test.hpp>

#define SPEC_EX_TABLE_FORMAT "CREATE TABLE %s.%s (key int PRIMARY KEY, value int)"
#define SPEC_EX_INSERT_FORMAT "INSERT INTO %s.%s (key, value) VALUES (%d, %d)"
#define SPEC_EX_SELECT_FORMAT "SELECT timeout(value) FROM %s.%s WHERE key=%d"
#define SPEC_EX_TIMEOUT_UDF_FORMAT "CREATE OR REPLACE FUNCTION %s.timeout(arg int) " \
  "RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java " \
  "AS $$ long start = System.currentTimeMillis(); " \
    "while(System.currentTimeMillis() - start < arg) {" \
      ";;" \
    "}" \
  "return arg;" \
  "$$;"

/**
 * Speculative Execution Policy Integration Test Class
 *
 * The purpose of this class is to setup a single session integration test
 * while initializing a three node cluster through CCM in order to perform
 * speculative execution policy tests.
 */
struct TestSpeculativeExecutionPolicy : public test_utils::SingleSessionTest {
  TestSpeculativeExecutionPolicy()
    : SingleSessionTest(3, 0, false)
    , keyspace_("speculative_execution_policy")
    , test_name_(boost::unit_test::framework::current_test_case().p_name) {
  }

  ~TestSpeculativeExecutionPolicy() {
    // Drop the keyspace in between each test execution
    if (session) {
      test_utils::execute_query(session, str(boost::format(
        test_utils::DROP_KEYSPACE_IF_EXISTS_FORMAT)
        % keyspace_));
    }
  }

  /**
   * Initialize the test case by creating the session and creating the
   * necessary keyspaces, tables with data, and UDFs being utilized during
   * query exection.
   */
  void initialize() {
    create_session();
    test_utils::execute_query(session, str(boost::format(
      test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
        % keyspace_
        % "3"));
    test_utils::execute_query(session, str(boost::format(SPEC_EX_TABLE_FORMAT)
      % keyspace_
      % test_name_));
    test_utils::execute_query(session, str(boost::format(SPEC_EX_INSERT_FORMAT)
      % keyspace_
      % test_name_
      % 0
      % 1000));
    test_utils::execute_query(session, str(boost::format(SPEC_EX_TIMEOUT_UDF_FORMAT)
      % keyspace_));
  }

  /**
   * Execute a query that utilizes a UDF timeout and a given statement
   * indempotance
   *
   * @param is_idempotent True if statement should be idempotent;
   *                      false otherwise
   * @param timeout_ms Request timeout (in milliseconds) to apply to the
   *                   statement (DEFAULT: 30s)
   * @param expected_error_code Excepted error code when executing the statement
   *                            (DEFAULT: CASS_OK)
   * @return The future used during executing the query
   */
  test_utils::CassFuturePtr query(bool is_idempotent,
    cass_uint64_t timeout_ms = 30000, CassError expected_error_code = CASS_OK) {
    // Create and execute the statement that will utilize the timeout UDF
    std::string query = str(boost::format(SPEC_EX_SELECT_FORMAT)
      % keyspace_
      % test_name_
      % 0);
    test_utils::CassStatementPtr statement(cass_statement_new(query.c_str(), 0));
    cass_statement_set_is_idempotent(statement.get(),
      (is_idempotent ? cass_true : cass_false));
    cass_statement_set_request_timeout(statement.get(), timeout_ms);
    cass::Statement* native_statement = static_cast<cass::Statement*>(statement.get());
    native_statement->set_record_attempted_addresses(true);
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    CassError error_code = test_utils::wait_and_return_error(future.get());
    BOOST_REQUIRE_EQUAL(expected_error_code, error_code);
    return future;
  }

  /**
   * Get the list of attempted hosts for a given future
   *
   * @param future The future used during execution of a query
   * @return List of attempted hosts
   */
  std::vector<std::string> attempted_hosts(test_utils::CassFuturePtr future) {
    // Gather and return the attempted hosts from the response
    std::vector<std::string> attempted_hosts;
    cass::Future* native_future = static_cast<cass::Future*>(future.get());
    if (native_future->type() == cass::CASS_FUTURE_TYPE_RESPONSE) {
      cass::ResponseFuture* native_reponse_future = static_cast<cass::ResponseFuture*>(native_future);
      cass::AddressVec attempted_addresses = native_reponse_future->attempted_addresses();
      for (cass::AddressVec::iterator iterator = attempted_addresses.begin();
        iterator != attempted_addresses.end(); ++iterator) {
        attempted_hosts.push_back(iterator->to_string());
      }
      std::sort(attempted_hosts.begin(), attempted_hosts.end());
    }
    return attempted_hosts;
  }

  /**
   * Get the executed host for a given future
   *
   * @param future The future used during execution of a query
   * @return The host used
   */
  std::string executed_host(test_utils::CassFuturePtr future) {
    std::string host;
    cass::Future* native_future = static_cast<cass::Future*>(future.get());
    if (native_future->type() == cass::CASS_FUTURE_TYPE_RESPONSE) {
      cass::ResponseFuture* native_reponse_future = static_cast<cass::ResponseFuture*>(native_future);
      host = native_reponse_future->address().to_string();
    }
    return host;
  }

private:
  /**
   * Keyspace name to use for all tests in the test suite
   */
  std::string keyspace_;
  /**
   * Name of the test being executed
   */
  std::string test_name_;
};

BOOST_AUTO_TEST_SUITE(speculative_execution_policy)

/**
 * Speculative execution policy; all nodes are attempted
 *
 * This test will ensure that all nodes are attempted when executing a query
 * using the speculative execution policy.
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @test_category queries:speculative_execution
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
BOOST_AUTO_TEST_CASE(execute_on_all_nodes) {
  CCM::CassVersion version = test_utils::get_version();
  if (version < "2.2.0") {
    std::cout << "Speculative Execution Test Requires UDF Functionality: Cassandra v"
      << version.to_string() << " does not support UDFs" << std::endl;
    BOOST_REQUIRE(true);
  } else {
    // Create the session and initialize the server
    TestSpeculativeExecutionPolicy tester;
    cass_cluster_set_constant_speculative_execution_policy(tester.cluster,
      100, 20);
    tester.initialize();

    // Execute a query and ensure all hosts are attempted
    test_utils::CassFuturePtr future = tester.query(true);
    std::vector<std::string> attempted_hosts = tester.attempted_hosts(future);
    BOOST_REQUIRE_EQUAL(3, attempted_hosts.size());
    for (size_t n = 0; n < 3; ++n) {
      std::stringstream expected_host;
      expected_host << tester.ccm->get_ip_prefix() << (n + 1);
      BOOST_REQUIRE_EQUAL(expected_host.str(), attempted_hosts.at(n));
    }
    std::string executed_host = tester.executed_host(future);
    std::vector<std::string>::iterator iterator = std::find(
      attempted_hosts.begin(), attempted_hosts.end(), executed_host);
    BOOST_REQUIRE(iterator != attempted_hosts.end());
  }
}

/**
 * Speculative execution policy; one node is attempted with idempotent statement
 *
 * This test will ensure that one nodes is attempted when executing a query
 * using the speculative execution policy.
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @test_category queries:speculative_execution
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
BOOST_AUTO_TEST_CASE(execute_one_node_idempotent) {
  CCM::CassVersion version = test_utils::get_version();
  if (version < "2.2.0") {
    std::cout << "Speculative Execution Test Requires UDF Functionality: Cassandra v"
      << version.to_string() << " does not support UDFs" << std::endl;
    BOOST_REQUIRE(true);
  } else {
    // Create the session and initialize the server
    TestSpeculativeExecutionPolicy tester;
    cass_cluster_set_constant_speculative_execution_policy(tester.cluster,
      5000, 20);
    tester.initialize();

    // Execute a query and ensure one host is attempted (non-idempotent)
    test_utils::CassFuturePtr future = tester.query(true);
    std::vector<std::string> attempted_hosts = tester.attempted_hosts(future);
    BOOST_REQUIRE_EQUAL(1, attempted_hosts.size());
    std::string executed_host = tester.executed_host(future);
    BOOST_REQUIRE_EQUAL(executed_host, attempted_hosts.at(0));
  }
}

/**
 * Speculative execution policy; one node is attempted with non-idempotent
 * statement
 *
 * This test will ensure that one node is attempted when executing a query
 * using the speculative execution policy when the statement is non-idempotent;
 * other hosts will not be executed in this scenario.
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @test_category queries:speculative_execution
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
BOOST_AUTO_TEST_CASE(execute_one_node_non_idempotent) {
  CCM::CassVersion version = test_utils::get_version();
  if (version < "2.2.0") {
    std::cout << "Speculative Execution Test Requires UDF Functionality: Cassandra v"
      << version.to_string() << " does not support UDFs" << std::endl;
    BOOST_REQUIRE(true);
  } else {
    // Create the session and initialize the server
    TestSpeculativeExecutionPolicy tester;
    cass_cluster_set_constant_speculative_execution_policy(tester.cluster,
      100, 20);
    tester.initialize();

    // Execute a query and ensure one host is attempted (non-idempotent)
    test_utils::CassFuturePtr future = tester.query(false);
    std::vector<std::string> attempted_hosts = tester.attempted_hosts(future);
    BOOST_REQUIRE_EQUAL(1, attempted_hosts.size());
    std::string executed_host = tester.executed_host(future);
    BOOST_REQUIRE_EQUAL(executed_host, attempted_hosts.at(0));
  }
}

/**
 * Speculative execution policy; attempt one additional node
 *
 * This test will ensure that two nodes are attempted when executing a query
 * using the speculative execution policy.
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @test_category queries:speculative_execution
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
BOOST_AUTO_TEST_CASE(attempt_two_nodes) {
  CCM::CassVersion version = test_utils::get_version();
  if (version < "2.2.0") {
    std::cout << "Speculative Execution Test Requires UDF Functionality: Cassandra v"
      << version.to_string() << " does not support UDFs" << std::endl;
    BOOST_REQUIRE(true);
  } else {
    // Create the session and initialize the server
    TestSpeculativeExecutionPolicy tester;
    cass_cluster_set_constant_speculative_execution_policy(tester.cluster,
      100, 1);
    tester.initialize();

    // Execute a query and ensure two hosts are attempted
    test_utils::CassFuturePtr future = tester.query(true);
    std::vector<std::string> attempted_hosts = tester.attempted_hosts(future);
    BOOST_REQUIRE_EQUAL(2, attempted_hosts.size());
    std::string executed_host = tester.executed_host(future);
    std::vector<std::string>::iterator iterator = std::find(
      attempted_hosts.begin(), attempted_hosts.end(), executed_host);
    BOOST_REQUIRE(iterator != attempted_hosts.end());
  }
}

/**
 * Speculative execution policy disabled (default behavior)
 *
 * This test will ensure that one node is attempted when executing a query
 * using the speculative execution policy.
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @test_category queries:speculative_execution
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
BOOST_AUTO_TEST_CASE(without_speculative_execution_policy) {
  CCM::CassVersion version = test_utils::get_version();
  if (version < "2.2.0") {
    std::cout << "Speculative Execution Test Requires UDF Functionality: Cassandra v"
      << version.to_string() << " does not support UDFs" << std::endl;
    BOOST_REQUIRE(true);
  } else {
    // Create the session and initialize the server
    TestSpeculativeExecutionPolicy tester;
    tester.initialize();

    // Execute a query and ensure two hosts are attempted
    test_utils::CassFuturePtr future = tester.query(true);
    std::vector<std::string> attempted_hosts = tester.attempted_hosts(future);
    BOOST_REQUIRE_EQUAL(1, attempted_hosts.size());
    std::string executed_host = tester.executed_host(future);
    BOOST_REQUIRE_EQUAL(executed_host, attempted_hosts.at(0));
  }
}

/**
 * Speculative execution policy; all nodes attempted with timeout
 *
 * This test will ensure that one node is attempted when executing a query
 * using the speculative execution policy.
 *
 * @since 2.5.0
 * @jira_ticket CPP-399
 * @test_category queries:speculative_execution
 * @cassandra_version 2.2.x Required only for testing due to UDF usage
 */
BOOST_AUTO_TEST_CASE(execute_on_all_nodes_with_timeout) {
  CCM::CassVersion version = test_utils::get_version();
  if (version < "2.2.0") {
    std::cout << "Speculative Execution Test Requires UDF Functionality: Cassandra v"
      << version.to_string() << " does not support UDFs" << std::endl;
    BOOST_REQUIRE(true);
  } else {
    // Create the session and initialize the server
    TestSpeculativeExecutionPolicy tester;
    cass_cluster_set_constant_speculative_execution_policy(tester.cluster,
      100, 20);
    tester.initialize();

    // Execute a query and ensure all nodes are tested and timeout occurs
    test_utils::CassFuturePtr future = tester.query(true, 500,
      CASS_ERROR_LIB_REQUEST_TIMED_OUT);
    std::vector<std::string> attempted_hosts = tester.attempted_hosts(future);
    BOOST_REQUIRE_EQUAL(3, attempted_hosts.size());
  }
}

BOOST_AUTO_TEST_SUITE_END()
