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

#include "simulacron_integration.hpp"

#define CORE_CONNECTIONS_PER_HOST 32

/**
 * Connection integration tests using Simulacron
 */
class ConnectionTest : public SimulacronIntegration {
using SimulacronIntegration::connect;
public:
  void SetUp() {
    is_sc_start_requested_ = false;
    is_session_requested_ = false;
    SimulacronIntegration::SetUp();
  }

  /**
   * Assert/Validate the active connections on the Simulacron cluster
   *
   * @param host_connections Connections per host (expected connections)
   * @param is_across_dcs True if connections are going to be made on all DCs;
   *                      false otherwise.
   */
  void assert_active_connections(int host_connections = 1,
    bool is_across_dcs = true) {
    std::vector<SimulacronCluster::Cluster::DataCenter> data_centers = sc_->data_centers();
    for (std::vector<SimulacronCluster::Cluster::DataCenter>::iterator it = data_centers.begin();
         it != data_centers.end(); ++it) {
      for (size_t n = 0; n < it->nodes.size(); ++n) {
        // Determine if control connection is being asserted (or remote DC node)
        size_t expected_connections = host_connections;
        if (n == 0 && it == data_centers.begin()) {
          expected_connections += 1; // Control connection
        }
        if (!is_across_dcs && it->name.compare("dc1") != 0) {
          expected_connections = 0;
        }

        ASSERT_EQ(expected_connections, (*it).nodes[n].active_connections);
      }
    }
  }

  /**
   * Start the SC, prime the tables and establish a connection to the
   * Simulacron with the given data center configuration
   *
   * @param data_center_nodes Data center(s) to create in the Simulacron cluster
   * @param cluster Cluster configuration to use when establishing the
   *                connection (default: NULL; uses default configuration)
   */
  void connect(std::vector<unsigned int> data_center_nodes,
    test::driver::Cluster cluster = NULL) {
    // Start the SC, prime the tables, and establish a connection
    start_sc(data_center_nodes);
    contact_points_ = sc_->get_ip_address(1);
    if (!cluster) {
      Integration::connect();
    } else {
      cluster.with_contact_points(contact_points_);
      Integration::connect(cluster);
    }
  }

  /**
   * Start the SC, prime the tables and establish a connection to the
   * Simulacron with the given data center configuration
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
    if (number_dc1_nodes > 0) data_center_nodes.push_back(number_dc1_nodes_);
    if (number_dc2_nodes > 0) data_center_nodes.push_back(number_dc2_nodes_);

    // Perform the connection
    connect(data_center_nodes, cluster);
  }
};

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains one
 * node and validate the number of active connections (including control
 * connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest, ConnectOneNode) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(1, 0);
  assert_active_connections();
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains
 * three nodes and validate the number of active connections (including control
 * connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest, ConnectThreeNodes) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(3, 0);
  assert_active_connections();
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains one
 * thousand nodes and validate the number of active connections (including
 * control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest, ConnectOneThousandNodes) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(1000, 0);
  assert_active_connections();
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains one
 * node on each data center and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest, ConnectOneNodeTwoDataCenters) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(1, 1);
  assert_active_connections(1, false);
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains one
 * node on each data center and validate the number of active connections across
 * the data centers (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 *                  across the data centers
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectOneNodeTwoDataCentersAcrossDCs) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_load_balance_round_robin();
  connect(1, 1, cluster);
  assert_active_connections();
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains
 * three nodes on each data center and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest, ConnectThreeNodesTwoDataCenters) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(3, 3);
  assert_active_connections(1, false);
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains
 * three nodes on each data center and validate the number of active connections
 * across the data centers (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 *                  across the data centers
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectThreeNodesTwoDataCentersAcrossDCs) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_load_balance_round_robin();
  connect(3, 3, cluster);
  assert_active_connections();
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains five
 * hundred nodes on each data center and validate the number of active
 * connections (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectFiveHundredNodesTwoDataCenters) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  connect(500, 500);
  assert_active_connections(1, false);
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains five
 * hundred nodes on each data center and validate the number of active
 * connections across the data centers (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 *                  across the data centers
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectFiveHundredNodesTwoDataCentersAcrossDCs) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_load_balance_round_robin();
  connect(500, 500, cluster);
  assert_active_connections();
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains
 * one node on nine data centers and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest, ConnectOneNodeNineDataCenters) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Create the nine data centers
  std::vector<unsigned int> data_center_nodes;
  for (int i = 0; i < 9; ++i) {
    data_center_nodes.push_back(1u);
  }

  // Ensure the control connection and normal node connection is established
  connect(data_center_nodes);
  assert_active_connections(1, false);
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains
 * one node on nine data centers and validate the number of active connections
 * across the data centers (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 *                  across the data centers
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectOneNodeNineDataCentersAcrossDCs) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Create the nine data centers
  std::vector<unsigned int> data_center_nodes;
  for (int i = 0; i < 9; ++i) {
    data_center_nodes.push_back(1u);
  }

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_load_balance_round_robin();
  connect(data_center_nodes, cluster);
  assert_active_connections();
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains one
 * node with multiple connections per host and validate the number of active
 * connections (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectOneNodeMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(1, 0, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST);
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains
 * three nodes with multiple connections per host and validate the number of
 * active connections (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectThreeNodesMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(3, 0, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST);
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains
 * one node on each data center and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectOneNodeTwoDataCentersMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(1, 1, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST, false);
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains
 * three nodes on each data center and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectThreeNodesTwoDataCentersMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(3, 3, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST, false);
}

/**
 * Perform connection to the Simulacron cluster
 *
 * This test will perform a connection to a Simulacron server that contains
 * one node on nine data centers and validate the number of active connections
 * (including control connection).
 *
 * @test_category connection
 * @since 1.0.0
 * @expected_result Successful connection and validation of active connections
 */
SIMULACRON_INTEGRATION_TEST_F(ConnectionTest,
                              ConnectOneNodeNineDataCentersMultipleConnectionsPerHost) {
  SKIP_TEST_IF_SIMULACRON_UNAVAILABLE;

  // Create the nine data centers
  std::vector<unsigned int> data_center_nodes;
  for (int i = 0; i < 9; ++i) {
    data_center_nodes.push_back(1u);
  }

  // Ensure the control connection and normal node connection is established
  test::driver::Cluster cluster = default_cluster()
    .with_core_connections_per_host(CORE_CONNECTIONS_PER_HOST);
  connect(data_center_nodes, cluster);
  assert_active_connections(CORE_CONNECTIONS_PER_HOST, false);
}
