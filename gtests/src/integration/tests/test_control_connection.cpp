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

#define LOGGER_WAIT_FOR_NAP 100

/**
 * Control connection integration tests; single node cluster
 */
class ControlConnectionTests : public Integration {

public:

  void SetUp() {
    // Call the parent setup function (don't automatically start session,
    // because we don't want any connections established until we have
    // set up the cluster).
    is_session_requested_ = false;
    Integration::SetUp();
  }

protected:
  /**
   * Execute multiple requests and ensure the expected nodes are used during
   * those executions
   *
   * @param session Session to execute the requests with
   * @param expected_nodes The nodes that
   */
  void check_hosts(Session session,
                   const std::set<unsigned short>& expected_nodes) {
    // Execute multiple requests and store the hosts used
    std::set<std::string> hosts;
    for (size_t i = 0; i < (expected_nodes.size() + 2); ++i) {
      Statement statement("SELECT * FROM " + system_schema_keyspaces_);
      Result result = session.execute(statement, false);
      if (result.error_code() == CASS_OK) {
        std::string host = result.host();
        if (!host.empty()) hosts.insert(host);
      } else {
        TEST_LOG_ERROR("Failed to query host:" << result.error_message()
          << "[" << result.error_code() << "]");
      }
    }

    // Validate the hosts used during request execution and the expected
    ASSERT_EQ(expected_nodes.size(), hosts.size());
    for (std::set<unsigned short>::const_iterator it = expected_nodes.begin();
         it != expected_nodes.end(); ++it) {
      std::stringstream node_ip_address;
      node_ip_address << ccm_->get_ip_prefix() << *it;
      ASSERT_GT(hosts.count(node_ip_address.str()), 0u);
    }
  }

  /**
   * Update the logger criteria to listen for driver reconnect messages
   *
   * @param nodes List of nodes to listen for
   */
  void reset_logger_criteria_for_reconnect(
    const std::set<unsigned short>& nodes) {
    logger_.reset();
    for (std::set<unsigned short>::const_iterator it = nodes.begin();
         it != nodes.end(); ++it) {
      std::stringstream node_ip_address;
      node_ip_address << ccm_->get_ip_prefix() << *it;
      logger_.add_critera("Host " + node_ip_address.str() + " already present " \
                          "attempting to initiate immediate connection");
    }
  }

  /**
   * Wait for the logger count to reach expected count
   *
   * NOTE: This may wait up to 10s
   *
   * @param expected_count Expected logger count
   * @return True if expected count is equal to logger count; false otherwise
   */
  bool wait_for_logger(size_t expected_count) {
    start_timer();
    while (elapsed_time() < 10000u && logger_.count() < expected_count) {
      msleep(LOGGER_WAIT_FOR_NAP);
    }
    return logger_.count() == expected_count;
  }
};

/**
 * Control connection integration tests; two node cluster
 */
class ControlConnectionTwoNodeClusterTests : public ControlConnectionTests {
public:
  ControlConnectionTwoNodeClusterTests() {
    number_dc1_nodes_ = 2;
  }
};

/**
 * Control connection integration tests; three node cluster
 */
class ControlConnectionThreeNodeClusterTests : public ControlConnectionTests {
public:
  ControlConnectionThreeNodeClusterTests() {
    number_dc1_nodes_ = 3;
  }
};

/**
 * Control connection integration tests; four node cluster
 */
class ControlConnectionFourNodeClusterTests : public ControlConnectionTests {
public:
  ControlConnectionFourNodeClusterTests() {
    number_dc1_nodes_ = 4;
  }
};

/**
 * Control connection integration tests; two data centers with a single node
 * each
 */
class ControlConnectionSingleNodeDataCentersClusterTests
  : public ControlConnectionTests {
public:
  ControlConnectionSingleNodeDataCentersClusterTests() {
    number_dc1_nodes_ = 1;
    number_dc2_nodes_ = 1;
  }
};

/**
 * Perform session connection using invalid IP address
 *
 * This test will attempt to perform a connection using an invalid IP address
 * and ensure the control connection is not established against a single node
 * cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will not be established
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTests,
                             ConnectUsingInvalidIpAddress) {
  CHECK_FAILURE;

  // Attempt to connect to the server using an invalid IP address
  logger_.add_critera("Unable to establish a control connection to host " \
                      "1.1.1.1 because of the following error: Connection " \
                      "timeout");
  Cluster cluster = Cluster::build().with_contact_points("1.1.1.1");
  try {
    cluster.connect();
    FAIL() << "Connection was established using invalid IP address";
  } catch (Session::Exception& se) {
    ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, se.error_code());
    ASSERT_GE(logger_.count(), 1u);
  }
}

/**
 * Perform session connection using invalid port
 *
 * This test will attempt to perform a connection using an invalid port number
 * and ensure the control connection is not established against a single node
 * cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will not be established
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTests, ConnectUsingInvalidPort) {
  CHECK_FAILURE;

  // Attempt to connect to the server using an invalid port number
  Cluster cluster = default_cluster().with_port(9999);
  try {
    cluster.connect();
    FAIL() << "Connection was established using invalid port assignment";
  } catch (Session::Exception& se) {
    ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, se.error_code());
  }
}

/**
 * Perform session connection using unresolvable local IP address
 *
 * This test will attempt to perform a connection using an unresolvable local
 * IP address and ensure the control connection is not established against a
 * single node cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will not be established
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTests,
                             ConnectUsingUnresolvableLocalIpAddress) {
  CHECK_FAILURE;

  // Attempt to connect to the server using an unresolvable local IP address
  Cluster cluster = default_cluster();
  EXPECT_EQ(CASS_ERROR_LIB_HOST_RESOLUTION,
            cass_cluster_set_local_address(cluster.get(), "unknown.invalid"));
}

/**
 * Perform session connection using unbindable local IP address
 *
 * This test will attempt to perform a connection using an unbindable local IP
 * address and ensure the control connection is not established against a
 * single node cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will not be established
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTests,
                             ConnectUsingUnbindableLocalIpAddress) {
  CHECK_FAILURE;

  // Attempt to connect to the server using an unbindable local IP address
  logger_.add_critera("Unable to bind local address: address not available");
  Cluster cluster = default_cluster().with_local_address("1.1.1.1");
  try {
    cluster.connect();
    FAIL() << "Connection was established using unbindable local IP address";
  } catch (Session::Exception& se) {
    ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, se.error_code());
    ASSERT_GE(logger_.count(), 1u);
  }
}

/**
 * Perform session connection using valid local IP address but invalid
 * remote address
 *
 * This test will attempt to perform a connection using a valid local IP
 * address and invalid remote address and ensure the control connection is
 * not established against a single node cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will not be established
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTests,
                             ConnectUsingValidLocalIpAddressButInvalidRemote) {
  CHECK_FAILURE;

  // Attempt to connect to the server using an valid local IP address
  // but invalid remote address. The specified remote is not routable
  // from the specified local.
  logger_.add_critera("Unable to establish a control connection to host " \
                      "1.1.1.1 because of the following error: " \
                      "Connect error 'operation not permitted'");
  Cluster cluster = Cluster::build().with_contact_points("1.1.1.1")
    .with_local_address("127.0.0.1");
  try {
    cluster.connect();
    FAIL() << "Connection was established using invalid IP address";
  } catch (Session::Exception& se) {
    ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, se.error_code());
    ASSERT_GE(logger_.count(), 1u);
  }
}

/**
 * Perform session connection while forcing a control connection reconnect
 *
 * This test will perform a connection and ensure the control connection
 * reconnects to the second node in the cluster when the first node is
 * terminated. During the reconnect process a new node will be added and
 * verified it is available to the new control connection. The new control
 * connection node will reconnect a second time as the second node is stopped
 * ensuring reconnects against a two node cluster with an added node after
 * startup.
 *
 * NOTE: The cluster starts with two nodes
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will reconnect to each node active in the
 *                  cluster
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTwoNodeClusterTests,
                             Reconnection) {
  CHECK_FAILURE;
  is_test_chaotic_ = true; // Destroy the cluster after the test completes

  /*
   * Create a new session connection using the round robin load balancing policy
   * and ensure only the first node is used as the contact point for automatic
   * node discovery of the second node
   */
  Cluster cluster = default_cluster()
    .with_load_balance_round_robin()
    .with_contact_points(generate_contact_points(ccm_->get_ip_prefix(), 1));
  Session session = cluster.connect();

  // Stop the first node and bootstrap a third node into the cluster
  stop_node(1);
  // Allow this new third node to come up without node 1
  std::vector<std::string> jvm_arguments;
  jvm_arguments.push_back("-Dcassandra.consistent.rangemovement=false");
  jvm_arguments.push_back("-Dcassandra.ring_delay_ms=10000");
  unsigned int node_3 = ccm_->bootstrap_node(jvm_arguments);

  // Stop the second node and ensure there is only one host active (node 3)
  stop_node(2);
  std::set<unsigned short> expected_nodes;
  expected_nodes.insert(node_3);
  check_hosts(session, expected_nodes);
}

/**
 * Perform session connection while adding and decommissioning nodes
 *
 * This test will perform a connection while adding and decommissioning a node
 * to ensure the control connections topology of the cluster is updated and
 * reflected in the request execution (using Round Robin Load Balancing
 * Policy).
 *
 * NOTE: The cluster starts with a single node
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will add and remove node
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTests, TopologyChange) {
  CHECK_FAILURE;
  is_test_chaotic_ = true; // Destroy the cluster after the test completes

  /*
   * Create a new session connection using the round robin load balancing policy
   * to ensure all nodes can be accessed during request execution
   */
  Cluster cluster = default_cluster()
    .with_load_balance_round_robin();
  Session session = cluster.connect();

  // Bootstrap a second node and ensure all hosts are actively used
  unsigned int node_2 = ccm_->bootstrap_node(); // Triggers a `NEW_NODE` event
  std::set<unsigned short> expected_nodes;
  expected_nodes.insert(1);
  expected_nodes.insert(node_2);
  check_hosts(session, expected_nodes);

  /*
   * Decommission the bootstrapped node and ensure only the first node is
   * actively used
   */
  force_decommission_node(node_2); // Triggers a `REMOVE_NODE` event
  expected_nodes.erase(node_2);
  check_hosts(session, expected_nodes);
}

/**
 * Perform session connection adding and decommissioning nodes
 *
 * This test will perform a connection while stopping and starting a node
 * to ensure the control connections status of the cluster is updated and
 * reflected in the request execution (using Round Robin Load Balancing
 * Policy) against a two node cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will update status of stopped and started
 *                  node
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTwoNodeClusterTests,
                             StatusChange) {
  CHECK_FAILURE;

  /*
   * Create a new session connection using the round robin load balancing policy
   * to ensure all nodes can be accessed during request execution
   */
  Cluster cluster = default_cluster()
    .with_load_balance_round_robin();
  Session session = cluster.connect();

  // Ensure all hosts are actively used
  std::set<unsigned short> expected_nodes;
  expected_nodes.insert(1);
  expected_nodes.insert(2);
  check_hosts(session, expected_nodes);

  // Stop the second node and ensure only the first node is actively used
  stop_node(2); // Triggers a `DOWN` event
  expected_nodes.erase(2);
  check_hosts(session, expected_nodes);

  // Restart the second node and ensure all hosts are actively used
  ccm_->start_node(2); // Triggers a `UP` event
  expected_nodes.insert(2);
  check_hosts(session, expected_nodes);
}

/**
 * Perform session connection ensuring automatic node discovery
 *
 * This test will perform a connection to a single node in the cluster to ensure
 * the control connection automatically discovers all other nodes of the cluster
 * in a three node cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will automatically discover other nodes
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionThreeNodeClusterTests,
                             NodeDiscovery) {
  CHECK_FAILURE;

  /*
   * Create a new session connection using the round robin load balancing policy
   * and ensure only the first node is used as the contact point for automatic
   * node discovery
   */
  Cluster cluster = default_cluster()
    .with_load_balance_round_robin()
    .with_contact_points(generate_contact_points(ccm_->get_ip_prefix(), 1));
  Session session = cluster.connect();

  // Ensure all hosts are actively used
  std::set<unsigned short> expected_nodes;
  expected_nodes.insert(1);
  expected_nodes.insert(2);
  expected_nodes.insert(3);
  check_hosts(session, expected_nodes);
}

/**
 * Perform session connection with invalid contact points while ensuring
 * automatic node discovery of other nodes
 *
 * This test will perform a connection to a single node in the cluster along
 * with invalid IP addresses in the list of contact point to ensure the control
 * connection automatically discovers all other nodes of the cluster in a three
 * node cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will ignore invalid contact points and
 *                  automatically discover other nodes
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionThreeNodeClusterTests,
                             NodeDiscoveryInvalidIpAddresses) {
  CHECK_FAILURE;

  /*
   * Create a new session connection using the round robin load balancing
   * policy, initial invalid IP addresses, and ensure only the first node is
   * used as the valid contact point for automatic node discovery
   */
  logger_.add_critera("Unable to reach contact point 192.0.2.");
  Cluster cluster = default_cluster()
    .with_load_balance_round_robin()
    .with_contact_points(generate_contact_points("192.0.2.", 3)) // Invalid IPs
    .with_contact_points(generate_contact_points(ccm_->get_ip_prefix(), 1)) // Single valid contact point
    .with_connect_timeout(60000u); // Handle initial invalid IPs
  Session session = cluster.connect();

  // Ensure the invalid IPs were not reached
  ASSERT_EQ(3u, logger_.count());

  // Ensure all hosts are actively used
  std::set<unsigned short> expected_nodes;
  expected_nodes.insert(1);
  expected_nodes.insert(2);
  expected_nodes.insert(3);
  check_hosts(session, expected_nodes);
}

/**
 * Perform session connection ensuring automatic node discovery with the
 * deletion of the `local` control connection information from the system table
 *
 * This test will perform a connection to a single node in the cluster, delete
 * the `local` control connection node information from the system table, and
 * ensure the control connection automatically discovers all other nodes of the
 * cluster in a three node cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will automatically discover other nodes
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionThreeNodeClusterTests,
                             NodeDiscoveryNoLocalRows) {
  CHECK_FAILURE;

  /*
   * Create a new session connection using the round robin load balancing policy
   * and ensure only the first node is used as the contact point for automatic
   * node discovery
   */
  Cluster cluster = default_cluster()
    .with_load_balance_round_robin()
    .with_contact_points(generate_contact_points(ccm_->get_ip_prefix(), 1));
  Session session = cluster.connect();

  // Delete the `local` row from the system table (control connection info)
  session.execute("DELETE FROM system.local WHERE key= 'local'");

  // Ensure all hosts are actively used
  std::set<unsigned short> expected_nodes;
  expected_nodes.insert(1);
  expected_nodes.insert(2);
  expected_nodes.insert(3);
  check_hosts(session, expected_nodes);
}

/**
 * Perform session connection ensuring automatic node discovery of the second
 * node with the `NULL` setting of the RPC address for the third node
 *
 * This test will perform a connection to a single node in the cluster, update
 * the `rpc_address` for the third node in the control connection node system
 * table, and ensure the control connection automatically discovers the second
 * node of the cluster in a three node cluster.
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will automatically discover other nodes
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionThreeNodeClusterTests,
                             NodeDiscoveryNoRpcAddress) {
  CHECK_FAILURE;
  is_test_chaotic_ = true; // Destroy the cluster after the test completes

  // Remove the `rpc_address` from the third node in the system table
  std::stringstream update_system_table;
  update_system_table
    << "UPDATE system.peers SET rpc_address = null WHERE peer = '"
    << ccm_->get_ip_prefix() << "3'";
  session_.execute(update_system_table.str());

  /*
   * Create a new session connection using the round robin load balancing policy
   * and ensure only the first node is used as the contact point for automatic
   * node discovery
   */
  Cluster cluster = default_cluster()
    .with_load_balance_round_robin()
    .with_contact_points(generate_contact_points(ccm_->get_ip_prefix(), 1));
  Session session = cluster.connect();
  session_ = cluster.connect();

  // Ensure nodes one and two are actively used
  std::set<unsigned short> expected_nodes;
  expected_nodes.insert(1);
  expected_nodes.insert(2);
  check_hosts(session, expected_nodes);
}

/**
 * Perform session connection and perform requests with full cluster outage
 *
 * This test will attempt to perform a connection using a cluster and attempt
 * requests against a cluster that has complete cluster outage and ensure the
 * requests fail. After full outage the cluster will be restarted and requests
 * will again succeed.
 *
 * NOTE: This is using a single node cluster
 *
 * @test_category control_connection
 * @since core:1.0.0
 * @expected_result Control connection will not be established and request will
 *                  fail during full cluster outage and request will succeed
 *                  after cluster is restarted
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTests,
                             FullOutage) {
  CHECK_FAILURE;

  // Stop the cluster and attempt to perform a request
  connect();
  ccm_->stop_cluster();
  Result result = session_.execute(SELECT_ALL_SYSTEM_LOCAL_CQL,
                                   CASS_CONSISTENCY_ONE, false, false);
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, result.error_code());

  // Update logger criteria to wait for nodes to reconnect
  std::set<unsigned short> nodes;
  std::vector<std::string> cluster_ip_addresses = ccm_->cluster_ip_addresses();
  for (unsigned short i = 0; i < cluster_ip_addresses.size(); ++i) {
    nodes.insert(i + 1);
  }
  reset_logger_criteria_for_reconnect(nodes);

  // Restart the cluster and wait for the nodes to reconnect
  ccm_->start_cluster();
  ASSERT_TRUE(wait_for_logger(nodes.size()));

  // Ensure all nodes are actively used
  std::set<unsigned short> expected_nodes;
  expected_nodes.insert(1);
  check_hosts(session_, expected_nodes);
}

/**
 * Perform session connection and decommission the control connection
 *
 * This test will perform a connection and ensure the driver does not attempt to
 * reconnect to a decommissioned node in the cluster; in this case the control
 * connection itself is decommissioned.
 *
 * NOTE: The cluster starts with two nodes
 *
 * @jira_ticket: CPP-210
 * @test_category control_connection
 * @since core:1.0.1
 * @expected_result Driver will not attempt to reconnect to a decommissioned
 *                  node
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTwoNodeClusterTests,
                             NodeDecommission) {
  CHECK_FAILURE;

  // Create a session connection
  Cluster cluster = default_cluster();
  Session session = cluster.connect();

  //Ensure all hosts are actively used
  std::set<unsigned short> expected_nodes;
  expected_nodes.insert(1);
  expected_nodes.insert(2);
  check_hosts(session, expected_nodes);

  /*
   * Decommission the control connection node and ensure reconnect is not
   * attempted by waiting a period of time
   */
  logger_.reset();
  logger_.add_critera("Spawning new connection to host "
                      + ccm_->get_ip_prefix() + "1");
  force_decommission_node(1);
  TEST_LOG("Node Decommissioned [" << ccm_->get_ip_prefix() << "1]: Sleeping "
           << "for 30 seconds");
  msleep(30000u);
  ASSERT_EQ(0u, logger_.count());
}

/**
 * Perform session connection and randomize the contact points
 *
 * This test will perform a connection and ensure the driver establishes a
 * randomized control connection against a four node cluster.
 *
 * @jira_ticket: CPP-193
 * @test_category control_connection
 * @since core:2.4.3
 * @expected_result Driver will randomize contact points when establishing a
 *                  connection
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionFourNodeClusterTests,
                             RandomizedContactPoints) {
  CHECK_FAILURE;

  // Create a cluster object using randomized contact points
  Cluster cluster = default_cluster()
    .with_load_balance_round_robin()
    .with_randomized_contact_points(true);

  // Establish a connect and ensure the first established host is not node 1
  Session session;
  std::string node_1_ip_address = ccm_->get_ip_prefix() + "1";
  std::string starting_host = node_1_ip_address;
  start_timer();
  while(starting_host.compare(node_1_ip_address) == 0
        && elapsed_time() < 5000u) { // Allow five seconds to ensure node 1 is not the starting host
    // Connect to the cluster and determine the executing host
    session = cluster.connect();
    starting_host = session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL).host();
  }
  ASSERT_STRNE(node_1_ip_address.c_str(), starting_host.c_str());

  // Ensure the remaining hosts are executed (in order based on LBP RR)
  int current_node = starting_host.at(starting_host.length() - 1) - '0';
  for (int i = 0; i < 3; ++i) {
    // Update the current node to the next item in the list (circular)
    current_node = (current_node + 1 > 4) ? 1 : current_node + 1;
    std::stringstream expected_host;
    expected_host << ccm_->get_ip_prefix() << current_node;

    // Ensure the next host is the expected host
    std::string current_host =
      session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL).host();
    ASSERT_STREQ(expected_host.str().c_str(), current_host.c_str());
  }

  // Ensure the next host is the starting host
  ASSERT_STREQ(starting_host.c_str(),
               session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL).host().c_str());
}

/**
 * Perform connection and ensure the control connection is closed when passing
 * in an invalid data center
 *
 * This test will perform a connection using the data center aware load
 * balancing policy and ensure the driver will not hang when terminating the
 * the control connection against a single node (each) two data center cluster.
 *
 * @jira_ticket: CPP-398
 * @test_category control_connection
 * @since core:2.6.0
 * @expected_result Driver will not hang and session/control connection will
 *                  terminate; CASS_ERROR_LIB_NO_HOSTS_AVAILABLE
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionSingleNodeDataCentersClusterTests,
                             InvalidDataCenter) {
  CHECK_FAILURE;

  /*
   * Create a new session connection using the data center aware load balancing
   * policy
   */
  Cluster cluster = default_cluster()
    .with_load_balance_dc_aware("invalid_data_center", 0, false);
  try {
    Session session = cluster.connect();
    FAIL() << "Connection was established using invalid data center";
  } catch (Session::Exception& se) {
    ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, se.error_code());
    ASSERT_STREQ("No hosts available for connection using the current load " \
                 "balancing policy", se.error_message().c_str());
  }
}

/**
 * Perform connection and ensure the control connection is closed when passing
 * in an invalid data center
 *
 * This test will perform a connection using more than one IO thread multiple
 * times ensuring each time that the driver properly terminates when using an
 * invalid keyspace name during the session connection. The invalid keyspace is
 * being used to tease out a previous bug that was fixed in v2.3.0; however was
 * not tested using multiple IO threads which caused the driver to "hang" which
 * was the direct result of the control connection not closing properly.
 *
 * @jira_ticket: CPP-398
 * @test_category control_connection
 * @since core:2.6.0
 * @expected_result Driver will not hang and session/control connection will
 *                  terminate with error
 */
CASSANDRA_INTEGRATION_TEST_F(ControlConnectionTests,
                             TerminatedUsingMultipleIoThreadsWithError) {
  CHECK_FAILURE;

  // Create multiple session connection while incrementing the I/O threads used
  for (int i = 2; i <= 16; ++i) {

    // Update the logger criteria for the expected driver messages
    logger_.reset();
    std::stringstream expected_message;
    // Message to validate number of I/O worker threads
    expected_message << "Creating " << i << " IO worker threads";
    logger_.add_critera(expected_message.str());
    // Message to validate connection/host is ready/up
    expected_message.str("");
    expected_message << "Connection ready on host " << ccm_->get_ip_prefix()
      << "1";
    logger_.add_critera(expected_message.str());
    // Message to validate the session has been disconnected (will not hang)
    logger_.add_critera("Session is disconnected");


    // Create a new session connection using increasing number of I/O threads
    Cluster cluster = default_cluster().with_num_threads_io(i);
    try {
      Session session = cluster.connect("invalid_keyspace");
      FAIL() << "Connection was established using invalid keyspace";
    } catch (Session::Exception& se) {
      ASSERT_EQ(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE, se.error_code());
      ASSERT_EQ(3u, logger_.count());
    }
  }
}
