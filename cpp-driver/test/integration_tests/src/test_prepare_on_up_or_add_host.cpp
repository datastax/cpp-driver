/*
  Copyright (c) 2017 DataStax

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

#include <boost/chrono.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/test/debug.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "execute_request.hpp"
#include "statement.hpp"
#include "test_utils.hpp"
#include "testing.hpp"

using namespace datastax::internal::testing;

/**
 * Test harness for prepare on up or add host functionality.
 */
struct PrepareOnUpOrAddHostTests : public test_utils::SingleSessionTest {

  /**
   * Create a basic schema (system table queries won't always prepare properly)
   * and initialize prepared query strings.
   */
  PrepareOnUpOrAddHostTests()
      : SingleSessionTest(1, 0)
      , keyspace(str(boost::format("ks_%s") % test_utils::generate_unique_str(uuid_gen))) {
    test_utils::execute_query(
        session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % keyspace % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % keyspace));

    for (int i = 1; i <= 3; ++i) {
      test_utils::execute_query(
          session, str(boost::format("CREATE TABLE test%d (k text PRIMARY KEY, v text)") % i));
      prepared_queries_.push_back(str(boost::format("SELECT * FROM %s.test%d") % keyspace % i));
    }

    // Make sure we equally try all available hosts
    cass_cluster_set_load_balance_round_robin(cluster);
  }

  /**
   * Get a session that is only connected to the given node.
   *
   * @param node The node the session should be connected to
   * @return The connected session
   */
  const test_utils::CassSessionPtr& session_for_node(int node) {
    if (node >= static_cast<int>(sessions.size()) || !sessions[node]) {
      sessions.resize(node + 1);

      std::stringstream ip_address;
      ip_address << ccm->get_ip_prefix() << node;

      test_utils::CassClusterPtr cluster(cass_cluster_new());
      cass_cluster_set_contact_points(cluster.get(), ip_address.str().c_str());
      cass_cluster_set_whitelist_filtering(cluster.get(), ip_address.str().c_str());
      sessions[node] = test_utils::create_session(cluster.get());
    }

    return sessions[node];
  }

  /**
   * Truncate the "system.prepared_statements" table on a given node
   *
   * @param node The node to truncate
   */
  void truncate_prepared_statements(int node) {
    test_utils::execute_query(session_for_node(node).get(),
                              "TRUNCATE TABLE system.prepared_statements");
  }

  /**
   * Verify that a nodes "system.prepared_statements" table is empty.
   *
   * @param node The node to check
   */
  void prepared_statements_is_empty(int node) {
    test_utils::CassResultPtr result;
    test_utils::execute_query(session_for_node(node).get(),
                              "SELECT * FROM system.prepared_statements", &result);
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 0u);
  }

  /**
   * Check to see if a query has been prepared on a given node.
   *
   * @param node The node to check
   * @param query A query string
   * @return true if the query has been prepared on the node, otherwise false
   */
  bool prepared_statement_is_present(int node, const std::string& query) {
    test_utils::CassResultPtr result;
    test_utils::execute_query(session_for_node(node).get(),
                              "SELECT * FROM system.prepared_statements", &result);

    test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));
    while (cass_iterator_next(iterator.get())) {
      const CassRow* row = cass_iterator_get_row(iterator.get());
      BOOST_REQUIRE(row);

      const CassValue* query_column = cass_row_get_column_by_name(row, "query_string");
      const char* query_string;
      size_t query_string_len;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(query_column, &query_string, &query_string_len),
                          CASS_OK);

      if (query == std::string(query_string, query_string_len)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Verify that all prepared queries are available on the specifed node.
   *
   * @param node The node to verify
   */
  void prepared_statements_are_present(int node) {
    for (std::vector<std::string>::const_iterator it = prepared_queries_.begin();
         it != prepared_queries_.end(); ++it) {
      BOOST_CHECK(prepared_statement_is_present(node, (*it)));
    }
  }

  /**
   * Verify that all prepared queries are not available on the specifed node.
   *
   * @param node The node to verify
   */
  void prepared_statements_are_not_present(int node) {
    for (std::vector<std::string>::const_iterator it = prepared_queries_.begin();
         it != prepared_queries_.end(); ++it) {
      BOOST_CHECK(!prepared_statement_is_present(node, (*it)));
    }
  }

  /**
   * Prepare all queries on a given session.
   *
   * @param session The session to prepare the queries on
   */
  void prepare_all_queries(test_utils::CassSessionPtr session) {
    for (std::vector<std::string>::const_iterator it = prepared_queries_.begin();
         it != prepared_queries_.end(); ++it) {
      test_utils::CassFuturePtr future(cass_session_prepare(session.get(), it->c_str()));
      BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);
    }
  }

  /**
   * Wait for a session to reconnect to a node.
   *
   * @param session The session to use for waiting
   * @param node The node to wait for
   */
  void wait_for_node(test_utils::CassSessionPtr session, int node) {
    std::stringstream ip_address;
    ip_address << ccm->get_ip_prefix() << node;

    for (int i = 0; i < 30; ++i) {
      test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.peers", 0));
      test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
      std::string host(get_host_from_future(future.get()).c_str());
      if (cass_future_error_code(future.get()) == CASS_OK && host == ip_address.str()) {
        return;
      }
      boost::this_thread::sleep_for(boost::chrono::seconds(1));
    }
    BOOST_REQUIRE_MESSAGE(false,
                          "Failed to wait for node " << ip_address.str() << " to become availble");
  }

  /**
   * A vector of sessions that are only connected to a single host (via the
   * whitelist policy).
   */
  std::vector<test_utils::CassSessionPtr> sessions;

  /**
   * The test's keyspace
   */
  std::string keyspace;

  /**
   * A vector of query strings to be prepared.
   */
  std::vector<std::string> prepared_queries_;
};

BOOST_FIXTURE_TEST_SUITE(prepare_on_up_or_add_host, PrepareOnUpOrAddHostTests)

/**
 * Verify that statements are not prepared when a node becomes available and
 * the prepare on up/add feature is disabled.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(statements_should_not_be_prepared_on_up_when_disabled) {
  if (!check_version("3.10")) return;

  // Disable the prepare on up/add setting
  cass_cluster_set_prepare_on_up_or_add_host(cluster, cass_false);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  // Verify that there are no statements prepared
  truncate_prepared_statements(1);
  prepared_statements_is_empty(1);

  // Populate the driver's prepared metadata cache
  prepare_all_queries(session);
  prepared_statements_are_present(1);

  // Clear all prepared queries on the server-side
  truncate_prepared_statements(1);
  prepared_statements_is_empty(1);

  // Simulate an UP event
  ccm->stop_node(1);
  ccm->start_node(1);

  // Wait for the node to become available and verify no statements have been
  // prepared
  wait_for_node(session, 1);
  prepared_statements_are_not_present(1);
}

/**
 * Verify that statements are prepared properly when a node becomes available
 * and the prepare on up/add feature is enabled.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(statements_should_be_prepared_on_up) {
  if (!check_version("3.10")) return;

  // Enable the prepare on up/add setting
  cass_cluster_set_prepare_on_up_or_add_host(cluster, cass_true);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  // Verify that there are no statements prepared
  truncate_prepared_statements(1);
  prepared_statements_is_empty(1);

  // Populate the driver's prepared metadata cache
  prepare_all_queries(session);
  prepared_statements_are_present(1);

  // Clear all prepared queries on the server-side
  truncate_prepared_statements(1);
  prepared_statements_is_empty(1);

  // Simulate an UP event
  ccm->stop_node(1);
  ccm->start_node(1);

  // Wait for the node to become available and verify that the statements
  // in the prepared metadata cache have been prepared
  wait_for_node(session, 1);
  prepared_statements_are_present(1);
}

/**
 * Verify that statements are not prepared when a new node is added to a cluster
 * and the prepare on up/add feature is disabled.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(statements_should_not_be_prepared_on_add_when_disabled) {
  if (!check_version("3.10")) return;

  // Disable the prepare on up/add setting
  cass_cluster_set_prepare_on_up_or_add_host(cluster, cass_false);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  // Verify that there are no statements prepared
  truncate_prepared_statements(1);
  prepared_statements_is_empty(1);

  // Populate the driver's prepared metadata cache
  prepare_all_queries(session);
  prepared_statements_are_present(1);

  // Add a new node
  int node = ccm->bootstrap_node();

  // Wait for the new node to become available and verify no statements have
  // been prepared
  wait_for_node(session, node);
  prepared_statements_are_not_present(node);
}

/**
 * Verify that statements are prepared properly when a new node is added to a
 * cluster and the prepare on up/add feature is enabled.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(statements_should_be_prepared_on_add) {
  if (!check_version("3.10")) return;

  // Enable the prepare on up/add setting
  cass_cluster_set_prepare_on_up_or_add_host(cluster, cass_true);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  // Verify that there are no statements prepared
  truncate_prepared_statements(1);
  prepared_statements_is_empty(1);

  // Populate the driver's prepared metadata cache
  prepare_all_queries(session);
  prepared_statements_are_present(1);

  // Add a new node
  int node = ccm->bootstrap_node();

  // Wait for the new node to become available and verify that the statements
  // in the prepared metadata cache have been prepared
  wait_for_node(session, node);
  prepared_statements_are_present(node);
}

BOOST_AUTO_TEST_SUITE_END()
