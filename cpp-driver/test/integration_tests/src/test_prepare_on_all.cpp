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
#include <boost/chrono.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "execute_request.hpp"
#include "statement.hpp"
#include "test_utils.hpp"

#define NUM_LOCAL_NODES 3

/**
 * Test harness for prepare on all host functionality.
 */
struct PrepareOnAllTests : public test_utils::SingleSessionTest {

  /**
   * Create a basic schema (system table queries won't always prepare properly)
   * and clear all prepared statements.
   */
  PrepareOnAllTests()
    : SingleSessionTest(NUM_LOCAL_NODES, 0)
    , keyspace(str(boost::format("ks_%s") % test_utils::generate_unique_str(uuid_gen)))
    , prepared_query_(str(boost::format("SELECT * FROM %s.test") % keyspace)) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % keyspace
                                           % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % keyspace));
    test_utils::execute_query(session, "CREATE TABLE test (k text PRIMARY KEY, v text)");

    // The "system.prepare_statements" table only exists in C* 3.10+
    if (version >= "3.10") {
      for (int node = 1; node <= NUM_LOCAL_NODES; ++node) {
        test_utils::execute_query(session_for_node(node).get(),
                                  "TRUNCATE TABLE system.prepared_statements");
      }
    }

    // Ensure existing prepared statements are not re-prepared when they become
    // available again.
    cass_cluster_set_prepare_on_up_or_add_host(cluster, cass_false);
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
   * Verify that all nodes have empty "system.prepared_statements" tables.
   */
  void prepared_statements_is_empty_on_all_nodes() {
    for (int node = 1; node <= NUM_LOCAL_NODES; ++node) {
      prepared_statements_is_empty(node);
    }
  }

  /**
   * Verify that a nodes "system.prepared_statements" table is empty.
   *
   * @param node The node to check
   */
  void prepared_statements_is_empty(int node) {
    test_utils::CassResultPtr result;
    test_utils::execute_query(session_for_node(node).get(), "SELECT * FROM system.prepared_statements", &result);
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
    test_utils::execute_query(session_for_node(node).get(), "SELECT * FROM system.prepared_statements", &result);

    test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));
    while (cass_iterator_next(iterator.get())) {
      const CassRow* row = cass_iterator_get_row(iterator.get());
      BOOST_REQUIRE(row);

      const CassValue* query_column = cass_row_get_column_by_name(row, "query_string");
      const char* query_string;
      size_t query_string_len;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(query_column, &query_string, &query_string_len), CASS_OK);

      if (query == std::string(query_string, query_string_len)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Get the count of nodes in the cluster where the provided query is prepared.
   *
   * @param query A query string
   * @return The number of nodes that have the prepared statement
   */
  int prepared_statement_is_present_count(const std::string& query) {
    int count = 0;
    for (int node = 1; node <= NUM_LOCAL_NODES; ++node) {
      if (prepared_statement_is_present(node, query)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Verify that a given number of nodes contain a prepared statement.
   *
   * @param count The number of nodes the query should be prepared on
   */
  void verify_prepared_statement_count(int count) {
    prepared_statements_is_empty_on_all_nodes();

    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    test_utils::CassFuturePtr future(cass_session_prepare(session.get(), prepared_query_.c_str()));
    BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

    test_utils::CassPreparedPtr prepared(cass_future_get_prepared(future.get()));
    BOOST_REQUIRE(prepared);

    BOOST_CHECK_EQUAL(prepared_statement_is_present_count(prepared_query_), count);
  }

  /**
   * Wait for a session to reconnect to a node.
   *
   * @param node The node to wait for
   */
  void wait_for_node(int node) {
    for (int i = 0; i < 10; ++i) {
      test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.peers", 0));
      test_utils::CassFuturePtr future(cass_session_execute(session_for_node(node).get(),
                                                            statement.get()));
      if (cass_future_error_code(future.get()) == CASS_OK) {
        return;
      }
      boost::this_thread::sleep_for(boost::chrono::seconds(1));
    }
    BOOST_REQUIRE(false);
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
   * The query to be prepared
   */
  std::string prepared_query_;
};

BOOST_FIXTURE_TEST_SUITE(prepare_on_all, PrepareOnAllTests)

/**
 * Verify that only a single node is prepared when the prepare on all hosts
 * setting is disabled.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(only_prepares_a_single_node_when_disabled) {
  if (!check_version("3.10")) return;

  // Prepare on all hosts disabled
  cass_cluster_set_prepare_on_all_hosts(cluster, cass_false);

  // Only a single host should have the statement prepared
  verify_prepared_statement_count(1);
}

/**
 * Verify that all nodes are prepared properly when the prepare on all hosts
 * setting is enabled.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(prepares_on_all_nodes_when_enabled) {
  if (!check_version("3.10")) return;

  // Prepare on all hosts enabled
  cass_cluster_set_prepare_on_all_hosts(cluster, cass_true);

  // All hosts should have the statement prepared
  verify_prepared_statement_count(3);
}

/**
 * Verify that all available nodes are prepared properly when the prepare on
 * all hosts setting is enabled and one of the nodes is not available.
 *
 * The statement should be prepared on all available nodes, but not the node
 * that was down.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(prepare_on_all_handles_node_outage) {
  if (!check_version("3.10")) return;

  // Prepare on all hosts enabled
  cass_cluster_set_prepare_on_all_hosts(cluster, cass_true);

  // Ensure there are no existing prepared statements
  prepared_statements_is_empty_on_all_nodes();

  ccm->kill_node(2);

  { // Prepare the statement
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    test_utils::CassFuturePtr future(cass_session_prepare(session.get(), prepared_query_.c_str()));
    BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

    test_utils::CassPreparedPtr prepared(cass_future_get_prepared(future.get()));
    BOOST_REQUIRE(prepared);
  }

  ccm->start_node(2);

  // Wait for the session to reconnect to the node
  wait_for_node(2);

  // The statement should only be prepared on the previously available nodes
  BOOST_CHECK_EQUAL(prepared_statement_is_present_count(prepared_query_), 2);
}

BOOST_AUTO_TEST_SUITE_END()
