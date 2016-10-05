/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "scassandra_integration.hpp"

#define CORE_CONNECTIONS_PER_HOST 32

/**
 * Connection integration tests using SCassandra
 */
class ConnectionTest : public SCassandraIntegration {
public:
  void SetUp() {
    is_scc_start_requested_ = false;
    is_scc_for_test_case_ = false;
    is_session_requested_ = false;
    SCassandraIntegration::SetUp();
  }

  /**
   * Assert/Validate the active connections on the SCassandra cluster
   *
   * @param host_connections Connections per host (expected connections)
   */
  void assert_active_connections(int host_connections = 1) {
    for (size_t n = 0; n < scc_->nodes().size(); ++n) {
      // Determine if control connection is being asserted
      size_t connections = (n == 0 ? host_connections + 1 : host_connections);
      ASSERT_EQ(connections, scc_->active_connections(n + 1).size());
    }
  }

  /**
   * Start the SCC, prime the tables and establish a connection to the
   * SCassandra with the given data center configuration
   *
   * @param data_center_nodes Data center(s) to create in the SCassandra cluster
   * @param cluster Cluster configuration to use when establishing the
   *                connection (default: NULL; uses default configuration)
   */
  void connect(std::vector<unsigned int> data_center_nodes,
    test::driver::Cluster cluster = NULL) {
    // Start the SCC, prime the tables, and establish a connection
    start_scc(data_center_nodes);
    scc_->prime_system_tables();
    contact_points_ = scc_->cluster_contact_points();
    if (!cluster) {
      Integration::connect();
    } else {
      cluster.with_contact_points(scc_->cluster_contact_points());
      Integration::connect(cluster);
    }
  }

  /**
   * Start the SCC, prime the tables and establish a connection to the
   * SCassandra with the given data center configuration
   *
   * @param number_dc1_nodes Number of nodes in data center one
   * @param number_dc2_nodes Number of nodes in data center two
   * @param cluster Cluster configuration to use when establishing the
   *                connection (default: NULL; uses default configuration)
   */
  void connect(unsigned int number_dc1_nodes, unsigned int number_dc2_nodes,
    test::driver::Cluster cluster = NULL) {
    // Initialize the number of nodes in the standard data centers
    std::vector<unsigned int> data_center_nodes;
    number_dc1_nodes_ = number_dc1_nodes;
    number_dc2_nodes_ = number_dc2_nodes;
    data_center_nodes.push_back(number_dc1_nodes_);
    data_center_nodes.push_back(number_dc2_nodes_);

    // Perform the connection
    connect(data_center_nodes, cluster);
  }
};

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains one
 * node and validate the number of active connections (including control
 * connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest, ConnectOneNode) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(1, 0);
  assert_active_connections();
}

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains
 * three nodes and validate the number of active connections (including control
 * connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest, ConnectThreeNodes) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(3, 0);
  assert_active_connections();
}

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains
 * one node on each data center and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest, ConnectOneNodeTwoDataCenters) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(1, 1);
  assert_active_connections();
}

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains
 * three nodes on each data center and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest, ConnectThreeNodesTwoDataCenters) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(3, 3);
  assert_active_connections();
}

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains
 * one node on nine data centers and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest, ConnectOneNodeNineDataCenters) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Create the nine data centers
  std::vector<unsigned int> data_center_nodes;
  for (int i = 0; i < 9; ++i) {
    data_center_nodes.push_back(1u);
  }

  // Ensure the control connection and normal node connection is established
  connect(data_center_nodes);
  assert_active_connections();
}

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains one
 * node with multiple connections per host and validate the number of active
 * connections (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest,
  ConnectOneNodeMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(1, 0, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST);
}

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains
 * three nodes with multiple connections per host and validate the number of
 * active connections (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest,
  ConnectThreeNodesMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(3, 0, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST);
}

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains
 * one node on each data center and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest,
  ConnectOneNodeTwoDataCentersMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(1, 1, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST);
}

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains
 * three nodes on each data center and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest,
  ConnectThreeNodesTwoDataCentersMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(3, 3, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST);
}

/**
 * Perform connection to the SCassandra cluster
 *
 * This test will perform a connection to a SCassandra server that contains
 * one node on nine data centers and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SCASSANDRA_INTEGRATION_TEST_F(ConnectionTest,
  ConnectOneNodeNineDataCentersMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SCC_UNAVAILABLE;

  // Create the nine data centers
  std::vector<unsigned int> data_center_nodes;
  for (int i = 0; i < 9; ++i) {
    data_center_nodes.push_back(1u);
  }

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(data_center_nodes, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST);
}
