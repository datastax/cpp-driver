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

#include "dse_integration.hpp"
#include "options.hpp"
#include "rapidjson/document.h"
#include "rest_client.hpp"

#define ALTER_DSE_LEASES_FORMAT                       \
  "ALTER KEYSPACE dse_leases "                        \
  " WITH REPLICATION = { "                            \
  "'class': 'NetworkTopologyStrategy', 'dc1' : '%d' " \
  "}"

#define GRAPH_OLAP_QUERY "g.V().count();"

#define GRAPH_OLAP_TIMEOUT 240000 // 4 minutes

#define REPLICATION_STRATEGY "{ 'class': %s }"

#define SELECT_ALL_SYSTEM_LOCAL_CQL "SELECT * FROM system.local"

#define SPARK_PORT 7080

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
    number_dc1_nodes_ = 3;
    replication_factor_ = 3;
    is_ccm_start_requested_ = false;
    is_session_requested_ = false;
    dse_workload_.push_back(CCM::DSE_WORKLOAD_SPARK);
    dse_workload_.push_back(CCM::DSE_WORKLOAD_GRAPH);
    DseIntegration::SetUp();

    // Wait for the spark master to become available
    if (server_version_ >= "6.8.0") {
      ccm_->update_cluster_configuration("dserm_options.override_legacy_rm", "false", true);
    }
    start_node(1);
    wait_for_port(1, SPARK_PORT);
    master_host_ip_address_ = ccm_->cluster_ip_addresses(true)[0];

    /*
     * Update the `dse_leases` keyspace replication factor to the number of
     * nodes in the cluster. This will prevent the election of a new job tracker
     * until all nodes are available, preventing nodes from electing the wrong
     * master node.
     */
    dse::Cluster cluster = dse::Cluster::build().with_contact_points(master_host_ip_address_);
    try {
      Session session = cluster.connect();
      session.execute(format_string(ALTER_DSE_LEASES_FORMAT, number_dc1_nodes_));
      session.close();
    } catch (Session::Exception e) {
      FAIL() << e.what();
    }

    // Bootstrap the remaining nodes and wait for the spark workers to become available
    for (unsigned int n = 2; n <= number_dc1_nodes_; ++n) {
      start_node(n);
      std::stringstream worker_ip_address;
      worker_ip_address << ccm_->get_ip_prefix() << n;
      worker_hosts_ip_addresses_.push_back(worker_ip_address.str());
    }
    if (wait_for_workers(master_host_ip_address_, number_dc1_nodes_)) {
      // Create the DSE session
      cluster = default_cluster().with_connection_heartbeat_interval(0).with_request_timeout(
          GRAPH_OLAP_TIMEOUT);
      DseIntegration::connect(cluster);

      // Create the graph
      create_graph();
      CHECK_FAILURE;
      populate_classic_graph(test_name_);
      CHECK_FAILURE;
    } else {
      FAIL() << "Spark workers are not available";
    }
  }

  /**
   * Execute a graph query
   *
   * @param iterations Number of queries to perform
   * @param source Graph source to query
   * @param hosts Hosts used during execution of graph query
   */
  void execute_query(unsigned int number_of_queries, const std::string& source,
                     std::vector<std::string>* hosts) {
    // Initialize the graph options and set analytics source
    dse::GraphOptions graph_options;
    graph_options.set_name(test_name_);
    if (!source.empty()) {
      graph_options.set_source(source);
    }
    graph_options.set_timeout(GRAPH_OLAP_TIMEOUT);

    // Execute the graph query and return the hosts used during execution
    for (unsigned int n = 0; n < number_of_queries; ++n) {
      // Execute the graph query and get the host address used
      dse::GraphResultSet result_set = dse_session_.execute(GRAPH_OLAP_QUERY, graph_options);
      CHECK_FAILURE;
      std::string host = result_set.host_address();
      if (std::find(hosts->begin(), hosts->end(), host) == hosts->end()) {
        hosts->push_back(host);
      }

      // Validate the result
      ASSERT_EQ(1u, result_set.count());
      dse::GraphResult result = result_set.next();
      ASSERT_EQ(DSE_GRAPH_RESULT_TYPE_NUMBER, result.type());
      ASSERT_TRUE(result.is_type<Integer>());
      ASSERT_EQ(6, result.value<Integer>().value());
    }
    std::sort(hosts->begin(), hosts->end());
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
   * Representation of a Spark master
   */
  struct SparkMaster {
    struct Slave {
      std::string host;
      std::string state;

      Slave(const rapidjson::Value& slave) {
        if (!slave.IsObject()) {
          throw Exception("Slave is not an object");
        }
        if (!slave.HasMember("host") || !slave.HasMember("state")) {
          throw Exception("JSON is not a valid slave");
        }

        host = slave["host"].GetString();
        state = slave["state"].GetString();
      }
    };

    std::vector<Slave> slaves;

    SparkMaster(const rapidjson::Document& master) {
      if (!master.IsObject()) {
        throw Exception("JSON document is not an object");
      }
      if (!master.HasMember("workers")) {
        throw Exception("JSON object is not a master object");
      }
      const rapidjson::Value& workers = master["workers"];
      if (!workers.IsArray()) {
        throw Exception("Slaves are not valid for the master object");
      }

      for (rapidjson::SizeType i = 0; i < workers.Size(); ++i) {
        Slave slave(workers[i]);
        slaves.push_back(slave);
      }
    }
  };

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

  /**
   * Wait for the Spark slaves/workers to become available
   *
   * @param master_ip_address Spark master IP address
   * @param number_of_worker Number of workers to look for
   * @return True if workers are 'ACTIVE'' false otherwise
   */
  bool wait_for_workers(const std::string& master_ip_address, unsigned short number_of_workers) {
    for (int n = 0; n < 1200; ++n) { // Check for up to 2 minutes (sleep is 100ms)
      // Create and send a request to the Spark server
      Request request;
      request.method = Request::HTTP_METHOD_GET;
      request.address = master_ip_address;
      request.port = SPARK_PORT;
      request.endpoint = "json/";
      Response rest_response = RestClient::send_request(request);

      // Parse the JSON document from the Spark server
      rapidjson::Document document;
      if (document.Parse(rest_response.message.c_str()).HasParseError()) {
        msleep(100);
        break; // Attempt to request JSON from Spark master again
      }
      SparkMaster master(document);

      // Iterate over the Spark workers and count the active slaves
      unsigned short active_slaves = 0;
      for (std::vector<SparkMaster::Slave>::const_iterator it = master.slaves.begin();
           it != master.slaves.end(); ++it) {
        SparkMaster::Slave slave = *it;
        if (to_lower(slave.state).compare("alive") == 0) {
          ++active_slaves;
        }
      }

      // Determine if all the Spark workers are active
      if (active_slaves >= number_of_workers) {
        return true;
      } else {
        msleep(100);
      }
    }

    // Not all Spark workers are active
    return false;
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
  expected_hosts.insert(expected_hosts.end(), worker_hosts_ip_addresses_.begin(),
                        worker_hosts_ip_addresses_.end());

  // Ensure all nodes are targeted when using graph source
  std::vector<std::string> hosts;
  execute_query(12, "g", &hosts);
  CHECK_FAILURE;
  ASSERT_EQ(number_dc1_nodes_, hosts.size());
  ASSERT_TRUE(std::equal(expected_hosts.begin(), expected_hosts.begin() + expected_hosts.size(),
                         hosts.begin()));

  // Ensure all nodes are targeted when using default graph source
  hosts.clear();
  execute_query(12, "", &hosts);
  CHECK_FAILURE;
  ASSERT_EQ(number_dc1_nodes_, hosts.size());
  ASSERT_TRUE(std::equal(expected_hosts.begin(), expected_hosts.begin() + expected_hosts.size(),
                         hosts.begin()));
}
