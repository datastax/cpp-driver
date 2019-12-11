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

#include <vector>

/**
 * Base class for "prepare on" tests.
 */
class PrepareOn : public Integration {
public:
  void SetUp() {
    Integration::SetUp();
    CHECK_VERSION(3.10);
    sessions_.reserve(number_dc1_nodes_ + 1);
    for (size_t node = 1; node <= number_dc1_nodes_; ++node) {
      truncate_prepared_statements(node);
    }
  }

  /**
   * Get a session that is only connected to the given node.
   *
   * @param node The node the session should be connected to
   * @return The connected session
   */
  Session session_for_node(size_t node) {
    if (node >= sessions_.size() || !sessions_[node]) {
      sessions_.resize(node + 1);

      std::stringstream ip_address;
      ip_address << ccm_->get_ip_prefix() << node;

      Cluster cluster;
      cluster.with_contact_points(ip_address.str());
      cluster.with_whitelist_filtering(ip_address.str());
      sessions_[node] = cluster.connect(keyspace_name_);
    }

    return sessions_[node];
  }

  /**
   * Verify that all nodes have empty "system.prepared_statements" tables.
   */
  void prepared_statements_is_empty_on_all_nodes() {
    for (size_t node = 1; node <= number_dc1_nodes_; ++node) {
      prepared_statements_is_empty(node);
    }
  }

  /**
   * Verify that a nodes "system.prepared_statements" table is empty.
   *
   * @param node The node to check
   */
  void prepared_statements_is_empty(size_t node) {
    EXPECT_EQ(
        session_for_node(node).execute("SELECT * FROM system.prepared_statements").row_count(), 0u);
  }

  /**
   * Check to see if a query has been prepared on a given node.
   *
   * @param node The node to check
   * @param query A query string
   * @return true if the query has been prepared on the node, otherwise false
   */
  bool prepared_statement_is_present(size_t node, const std::string& query) {
    Result result = session_for_node(node).execute("SELECT * FROM system.prepared_statements");
    if (!result) return false;

    Rows rows = result.rows();
    for (size_t i = 0; rows.row_count(); ++i) {
      Row row = rows.next();
      if (row.column_by_name<Varchar>("query_string").str() == query) {
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
    for (size_t node = 1; node <= number_dc1_nodes_; ++node) {
      if (prepared_statement_is_present(node, query)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Truncate the "system.prepared_statements" table on a given node
   *
   * @param node The node to truncate
   */
  void truncate_prepared_statements(size_t node) {
    session_for_node(node).execute("TRUNCATE TABLE system.prepared_statements");
  }

  /**
   * Wait for a session to reconnect to a node.
   *
   * @param session The session to use for waiting
   * @param node The node to wait for
   */
  void wait_for_node(size_t node) {
    for (int i = 0; i < 300; ++i) {
      Result result = session_for_node(node).execute(SELECT_ALL_SYSTEM_LOCAL_CQL,
                                                     CASS_CONSISTENCY_LOCAL_ONE, false, false);
      CassError rc = result.error_code();
      if (rc == CASS_OK) return;
      msleep(200);
    }
    ASSERT_TRUE(false) << "Node " << node << " didn't become available within the allocated time";
  }

private:
  std::vector<Session> sessions_;
};

/**
 * Prepare on all hosts test suite.
 */
class PrepareOnAllTests : public PrepareOn {
public:
  PrepareOnAllTests() { number_dc1_nodes_ = 3; }

  void SetUp() {
    PrepareOn::SetUp();
    prepared_query_ = default_select_all();
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
  }

  /**
   * Verify that a given number of nodes contain a prepared statement.
   *
   * @param session The session to use for preparing the query
   * @param count The number of nodes the query should be prepared on
   */
  void verify_prepared_statement_count(Session session, int count) {
    prepared_statements_is_empty_on_all_nodes();
    session.prepare(prepared_query_);
    EXPECT_EQ(prepared_statement_is_present_count(prepared_query_), count);
  }

  const std::string& prepared_query() const { return prepared_query_; }

  Cluster cluster() {
    // Ensure existing prepared statements are not re-prepared when they become
    // available again.
    return default_cluster().with_prepare_on_up_or_add_host(false).with_constant_reconnect(200);
  }

private:
  std::string prepared_query_;
};

/**
 * Verify that only a single node is prepared when the prepare on all hosts
 * setting is disabled.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(PrepareOnAllTests, SingleNodeWhenDisabled) {
  CHECK_FAILURE;
  CHECK_VERSION(3.10);

  // Prepare on all hosts disabled
  Session session = cluster().with_prepare_on_all_hosts(false).connect();

  // Only a single host should have the statement prepared
  verify_prepared_statement_count(session, 1);
}

/**
 * Verify that all nodes are prepared properly when the prepare on all hosts
 * setting is enabled.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(PrepareOnAllTests, AllNodesWhenEnabled) {
  CHECK_FAILURE;
  CHECK_VERSION(3.10);

  // Prepare on all hosts enabled
  Session session = cluster().with_prepare_on_all_hosts(true).connect();

  // All hosts should have the statement prepared
  verify_prepared_statement_count(session, 3);
}

/**
 * Verify that all available nodes are prepared properly when the prepare on
 * all hosts setting is enabled and one of the nodes is not available.
 *
 * The statement should be prepared on all available nodes, but not the node
 * that was down.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(PrepareOnAllTests, NodeOutage) {
  CHECK_FAILURE;
  CHECK_VERSION(3.10);

  // Ensure there are no existing prepared statements
  prepared_statements_is_empty_on_all_nodes();

  stop_node(2, true);

  { // Prepare the statement with prepare on all enabled
    Session session = cluster().with_prepare_on_all_hosts(true).connect();
    session.prepare(prepared_query());
  }

  start_node(2);

  // Wait for the session to reconnect to the node
  wait_for_node(2);

  // The statement should only be prepared on the previously available nodes
  EXPECT_EQ(prepared_statement_is_present_count(prepared_query()), 2);
}

/**
 * Prepare on host UP and ADD test suite.
 */
class PrepareOnUpAndAddTests : public PrepareOn {
public:
  PrepareOnUpAndAddTests() { number_dc1_nodes_ = 1; }

  void SetUp() {
    PrepareOn::SetUp();

    for (int i = 1; i <= 3; ++i) {
      std::stringstream ss;
      ss << table_name_ << i;
      std::string table_name_with_suffix(ss.str());
      session_.execute(format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT,
                                     table_name_with_suffix.c_str(), "int", "int"));
      prepared_queries_.push_back(format_string("SELECT * FROM %s.%s", keyspace_name_.c_str(),
                                                table_name_with_suffix.c_str()));
    }
  }

  /**
   * Prepare all queries on a given session.
   *
   * @param session The session to prepare the queries on
   */
  void prepare_all_queries(Session session) {
    for (std::vector<std::string>::const_iterator it = prepared_queries_.begin(),
                                                  end = prepared_queries_.end();
         it != end; ++it) {
      session.prepare(*it);
    }
  }

  /**
   * Verify that all prepared queries are available on the specified node.
   *
   * @param node The node to verify
   */
  void prepared_statements_are_present(size_t node) {
    wait_for_node(node);
    for (std::vector<std::string>::const_iterator it = prepared_queries_.begin(),
                                                  end = prepared_queries_.end();
         it != end; ++it) {
      EXPECT_TRUE(prepared_statement_is_present(node, (*it)))
          << "Prepared statement should be present on node " << node;
    }
  }

  /**
   * Verify that all prepared queries are not available on the specified node.
   *
   * @param node The node to verify
   */
  void prepared_statements_are_not_present(size_t node) {
    wait_for_node(node);
    for (std::vector<std::string>::const_iterator it = prepared_queries_.begin(),
                                                  end = prepared_queries_.end();
         it != end; ++it) {
      EXPECT_FALSE(prepared_statement_is_present(node, (*it)))
          << "Prepare statement should not be present on node " << node;
    }
  }

  /**
   * Wait for a session to reconnect to a node.
   *
   * @param session The session to use for waiting
   * @param node The node to wait for
   */
  void wait_for_node_on_session(Session session, size_t node) {
    std::stringstream ip_address;
    ip_address << ccm_->get_ip_prefix() << node;

    for (int i = 0; i < 300; ++i) {
      Result result =
          session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL, CASS_CONSISTENCY_LOCAL_ONE, false, false);
      if (result && result.error_code() == CASS_OK && result.host() == ip_address.str()) {
        return;
      }
      msleep(200);
    }
    ASSERT_TRUE(false) << "Node " << node << " didn't become available within the allocated time";
  }

  Cluster cluster() {
    // Make sure we equally try all available hosts
    return default_cluster().with_load_balance_round_robin().with_constant_reconnect(200);
  }

private:
  std::vector<std::string> prepared_queries_;
};

/**
 * Verify that statements are not prepared when a node becomes available and
 * the prepare on up/add feature is disabled.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(PrepareOnUpAndAddTests, NotPreparedOnUpWhenDisabled) {
  CHECK_FAILURE;
  CHECK_VERSION(3.10);

  // Disable the prepare on up/add setting
  Session session = cluster().with_prepare_on_up_or_add_host(false).connect();

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
  stop_node(1);
  start_node(1);

  // Wait for the node to become available and verify no statements have been
  // prepared
  wait_for_node_on_session(session, 1);
  prepared_statements_are_not_present(1);
}

/**
 * Verify that statements are prepared properly when a node becomes available
 * and the prepare on up/add feature is enabled.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(PrepareOnUpAndAddTests, PreparedOnUpWhenEnabled) {
  CHECK_FAILURE;
  CHECK_VERSION(3.10);

  // Enable the prepare on up/add setting
  Session session = cluster().with_prepare_on_up_or_add_host(true).connect();

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
  stop_node(1);
  start_node(1);

  // Wait for the node to become available and verify that the statements
  // in the prepared metadata cache have been prepared
  wait_for_node_on_session(session, 1);
  prepared_statements_are_present(1);
}

/**
 * Verify that statements are not prepared when a new node is added to a cluster
 * and the prepare on up/add feature is disabled.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(PrepareOnUpAndAddTests, NotPreparedOnAddWhenDisabled) {
  CHECK_FAILURE;
  CHECK_VERSION(3.10);
  is_test_chaotic_ = true;

  // Disable the prepare on up/add setting
  Session session = cluster().with_prepare_on_up_or_add_host(false).connect();

  // Verify that there are no statements prepared
  truncate_prepared_statements(1);
  prepared_statements_is_empty(1);

  // Populate the driver's prepared metadata cache
  prepare_all_queries(session);
  prepared_statements_are_present(1);

  // Add a new node
  size_t node = ccm_->bootstrap_node();

  // Wait for the new node to become available and verify no statements have
  // been prepared
  wait_for_node_on_session(session, node);
  prepared_statements_are_not_present(node);
}

/**
 * Verify that statements are prepared properly when a new node is added to a
 * cluster and the prepare on up/add feature is enabled.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(PrepareOnUpAndAddTests, PreparedOnAddWhenEnabled) {
  CHECK_FAILURE;
  CHECK_VERSION(3.10);
  is_test_chaotic_ = true;

  // Enable the prepare on up/add setting
  Session session = cluster().with_prepare_on_up_or_add_host(true).connect();

  // Verify that there are no statements prepared
  truncate_prepared_statements(1);
  prepared_statements_is_empty(1);

  // Populate the driver's prepared metadata cache
  prepare_all_queries(session);
  prepared_statements_are_present(1);

  // Add a new node
  size_t node = ccm_->bootstrap_node();

  // Wait for the new node to become available and verify that the statements
  // in the prepared metadata cache have been prepared
  wait_for_node_on_session(session, node);
  prepared_statements_are_present(node);
}
