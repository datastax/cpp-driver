/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse_integration.hpp"
#include "options.hpp"

#define ALTER_DSE_LEASES_FORMAT "ALTER KEYSPACE dse_leases " \
  " WITH REPLICATION = { " \
    "'class': 'NetworkTopologyStrategy', 'dc1' : '%d' " \
  "}"

#define GRAPH_OLAP_QUERY \
  "g.V().count();"

#define GRAPH_OLAP_TIMEOUT 240000 // 4 minutes

#define REPLICATION_STRATEGY "{ 'class': %s }"

#define SELECT_ALL_SYSTEM_LOCAL_CQL "SELECT * FROM system.local"

#define WAIT_FOR_WORKERS_SLEEP 120000 // 2 minutes

/**
 * Graph OLAP integration tests
 *
 * @dse_version 5.0.0
 */
class GraphOlapTest : public DseIntegration {
public:
  void SetUp() {
    CHECK_VERSION(5.0.0);

    // Call the parent setup function
    number_dc1_nodes_ = 2;
    replication_factor_ = 2;
    is_ccm_start_requested_ = false;
    is_session_requested_ = false;
    dse_workload_.push_back(CCM::DSE_WORKLOAD_SPARK);
    dse_workload_.push_back(CCM::DSE_WORKLOAD_GRAPH);
    DseIntegration::SetUp();

    // Wait for the spark master to become available
    ccm_->start_node(1);
    wait_for_port(1, 7080);
    master_host_ip_address_ = ccm_->cluster_ip_addresses(true)[0];

    /*
     * Update the `dse_leases` keyspace replication factor to the number of
     * nodes in the cluster. This will prevent the election of a new job tracker
     * until all nodes are available, preventing nodes from electing the wrong
     * master node.
     */
    Cluster cluster = Cluster::build()
      .with_contact_points(master_host_ip_address_);
    try {
      Session session = cluster.connect();
      session.execute(format_string(ALTER_DSE_LEASES_FORMAT,
        number_dc1_nodes_));
      session.close();
    } catch (Session::Exception e) {
      FAIL() << e.what();
    }

    // Bootstrap the remaining nodes
    for (unsigned int n = 2; n <= number_dc1_nodes_; ++n) {
      ccm_->start_node(n);
      std::stringstream worker_ip_address;
      worker_ip_address << ccm_->get_ip_prefix() << n;
      worker_hosts_ip_addresses_.push_back(worker_ip_address.str());
    }

    //TODO: Remove sleep and add parsing of HTML body from spark master to look for live workers
    TEST_LOG("Waiting for workers: Sleeping " << WAIT_FOR_WORKERS_SLEEP / 1000
      << "s");
    msleep(WAIT_FOR_WORKERS_SLEEP);

    // Create the DSE session
    cluster = default_cluster()
      .with_connection_heartbeat_interval(0)
      .with_request_timeout(GRAPH_OLAP_TIMEOUT);
    DseIntegration::connect(cluster);

    // Create the graph
    create_graph();
    CHECK_FAILURE;
    populate_classic_graph(test_name_);
    CHECK_FAILURE;
  }

  /**
   * Execute a graph query
   *
   * @param iterations Number of queries to perform
   * @param source Graph source to query
   * @param hosts Hosts used during execution of graph query
   */
  void execute_query(unsigned int number_of_queries,
    const std::string& source, std::vector<std::string> *hosts) {
    // Initialize the graph options and set analytics source
    test::driver::DseGraphOptions graph_options;
    graph_options.set_name(test_name_);
    if (!source.empty()) {
      graph_options.set_source(source);
    }
    graph_options.set_timeout(GRAPH_OLAP_TIMEOUT);

    // Execute the graph query and return the hosts used during execution
    for (unsigned int n = 0; n < number_of_queries; ++n) {
      // Execute the graph query and get the host address used
      test::driver::DseGraphResultSet result_set = dse_session_.execute(
        GRAPH_OLAP_QUERY, graph_options);
      CHECK_FAILURE;
      std::string host = result_set.host_address();
      if (std::find(hosts->begin(), hosts->end(), host) == hosts->end()) {
        hosts->push_back(host);
      }

      // Validate the result
      ASSERT_EQ(1u, result_set.count());
      test::driver::DseGraphResult result = result_set.next();
      ASSERT_EQ(DSE_GRAPH_RESULT_TYPE_NUMBER, result.type());
      ASSERT_TRUE(result.is_type<Integer>());
      ASSERT_EQ(6, result.value<Integer>().value());
    }
  }

protected:
  /**
   * Host address for the master analytics node
   */
  std::string master_host_ip_address_;
  /**
   * Host addresses for the workers
   */
  std::vector<std::string> worker_hosts_ip_addresses_;

private:
  /**
   * Wait for the port on a node to become available
   *
   * @param node Node to wait for port availability
   * @param port Port to connect to
   * @return True if port on IP address is available; false otherwise
   */
  bool wait_for_port(unsigned int node, unsigned short port) {
    // Get the IP address for the node
    std::string ip_address = ccm_->cluster_ip_addresses(true)[node - 1];

    // Wait for the port to become available
    return Utils::wait_for_port(ip_address, port);
  }
};

/**
 * Perform simple graph analytics statement execution - Ensure node is targeted
 *
 * This test will execute a graph statement to determine if the analytics node
 * is the single node targeted during execution.
 *
 * @jira_ticket CPP-374
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result Graph analytics node will be targeted during query
 */
DSE_INTEGRATION_TEST_F(GraphOlapTest, AnalyticsNodeTargeted) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Perform the query multiple times to ensure targeted master node
  std::vector<std::string> hosts;
  execute_query(12, "a", &hosts);
  CHECK_FAILURE;
  ASSERT_EQ(1ul, hosts.size());
  ASSERT_EQ(master_host_ip_address_, hosts[0]);
}

/**
 * Perform simple graph statement execution using default graph source
 *
 * This test will execute a graph statement to ensure that all nodes are
 * utilized when host targeting policy is enabled.
 *
 * @see cass_cluster_new_dse()
 *
 * @jira_ticket CPP-374
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result All graph nodes will be targeted during query
 */
DSE_INTEGRATION_TEST_F(GraphOlapTest, AnalyticsNodeNotTargeted) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Generate the list of expected hosts for validation
  std::vector<std::string> expected_hosts;
  expected_hosts.push_back(master_host_ip_address_);
  expected_hosts.insert(expected_hosts.end(),
    worker_hosts_ip_addresses_.begin(), worker_hosts_ip_addresses_.end());

  // Ensure all nodes are targeted when using graph source
  std::vector<std::string> hosts;
  execute_query(12, "g", &hosts);
  CHECK_FAILURE;
  ASSERT_EQ(number_dc1_nodes_, hosts.size());
  ASSERT_TRUE(std::equal(expected_hosts.begin(),
    expected_hosts.begin() + expected_hosts.size(), hosts.begin()));

  // Ensure all nodes are targeted when using default graph source
  hosts.clear();
  execute_query(12, "", &hosts);
  CHECK_FAILURE;
  ASSERT_EQ(number_dc1_nodes_, hosts.size());
  ASSERT_TRUE(std::equal(expected_hosts.begin(),
    expected_hosts.begin() + expected_hosts.size(), hosts.begin()));
}
