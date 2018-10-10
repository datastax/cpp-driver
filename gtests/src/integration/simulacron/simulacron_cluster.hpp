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

#ifndef __TEST_SIMULACRON_CLUSTER_HPP__
#define __TEST_SIMULACRON_CLUSTER_HPP__

// Make sure Simulacron server is available during compile time
#ifdef USE_SIMULACRON_SERVER
#include "exception.hpp"
#include "priming_requests.hpp"
#include "shared_ptr.hpp"
#include "rest_client.hpp"

#ifdef _CRT_SECURE_NO_WARNINGS
# undef _CRT_SECURE_NO_WARNINGS
#endif
#include <rapidjson/document.h>
#include <uv.h>

#include <algorithm>
#include <cstdio>
#include <map>
#include <string>
#include <string.h>
#include <vector>

namespace test {

/**
 * Simulacron cluster for easily creating simulated DSE/Cassandra nodes
 */
class SimulacronCluster {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
      : test::Exception(message) { }
  };

  /**
   * Representation of a Simulacron cluster
   */
  struct Cluster {
    struct DataCenter {
      struct Node {
        struct PeerInfo {
          std::vector<std::string> tokens;

          PeerInfo() { };
          PeerInfo(const rapidjson::Value& peer_info) {
            if (peer_info.IsObject()) {
              if (peer_info.HasMember("tokens")) {
                // Get the name of the node
                tokens = test::Utils::explode(peer_info["tokens"].GetString(), ',');
              } else {
                throw Exception("JSON is not a valid peer info");
              }
            } else {
              throw Exception("Peer info object is not an object");
            }
          }
        };

        std::string name;
        unsigned int id;
        unsigned int data_center_id;
        unsigned int active_connections;
        std::string ip_address;
        unsigned short port;
        PeerInfo peer_info;

        Node(const rapidjson::Value& node, unsigned int dc_id)
          : data_center_id(dc_id) {
          if (node.IsObject()) {
            if (node.HasMember("name") && node.HasMember("id") &&
              node.HasMember("active_connections") &&
              node.HasMember("address") && node.HasMember("peer_info")) {
              name = node["name"].GetString();
              id = node["id"].GetUint();
              active_connections = node["active_connections"].GetUint();

              const rapidjson::Value& address = node["address"];
              std::vector<std::string> tokens = test::Utils::explode(
                address.GetString(), ':');
              if (tokens.size() == 2) {
                ip_address = tokens[0];
                std::stringstream valueStream(tokens[1]);
                if ((valueStream >> port).fail()) {
                  throw Exception("Port is not a valid short");
                }
              } else {
                throw Exception("Unable to parse IP address and port for node");
              }

              peer_info = PeerInfo(node["peer_info"]);
            } else {
              throw Exception("JSON is not a valid node");
            }
          } else {
            throw Exception("Node object is not an object");
          }
        }

        bool operator< (const Node& rhs) const {
          return id < rhs.id;
        }
      };

      std::string name;
      unsigned int id;
      unsigned int active_connections;
      std::vector<Node> nodes;

      DataCenter(const rapidjson::Value& data_center) {
        if (data_center.IsObject()) {
          if (data_center.HasMember("name") && data_center.HasMember("id") &&
            data_center.HasMember("active_connections")) {
            name = data_center["name"].GetString();
            id = data_center["id"].GetUint();
            active_connections = data_center["active_connections"].GetUint();

            if (data_center.HasMember("nodes")) {
              const rapidjson::Value& dc_nodes = data_center["nodes"];
              if (dc_nodes.IsArray()) {
                for (rapidjson::SizeType i = 0; i < dc_nodes.Size(); ++i) {
                  Node node(dc_nodes[i], id);
                  nodes.insert(std::lower_bound(nodes.begin(), nodes.end(),
                    node), node);
                }
              } else {
                throw Exception("Nodes are not valid for the data center object");
              }
            }
          } else {
            throw Exception("JSON is not a valid data center");
          }
        } else {
          throw Exception("Data center object is not an object");
        }
      }

      bool operator< (const DataCenter& rhs) const {
        return id < rhs.id;
      }
    };

    std::string name;
    unsigned int id;
    unsigned int active_connections;
    std::string cassandra_version;
    std::string dse_version;
    std::vector<DataCenter> data_centers;

    Cluster(const rapidjson::Document *cluster) {
      if (cluster) {
        if (cluster->IsObject()) {
          if (cluster->HasMember("name") && cluster->HasMember("id") &&
            cluster->HasMember("active_connections") &&
            cluster->HasMember("data_centers")) {
            name = (*cluster)["name"].GetString();
            id = (*cluster)["id"].GetUint();
            active_connections = (*cluster)["active_connections"].GetUint();

            if (cluster->HasMember("cassandra_version")) {
              cassandra_version = (*cluster)["cassandra_version"].GetString();
            }
            if (cluster->HasMember("dse_version")) {
              dse_version = (*cluster)["dse_version"].GetString();
            }

            const rapidjson::Value& dcs = (*cluster)["data_centers"];
            if (dcs.IsArray()) {
              for (rapidjson::SizeType i = 0; i < dcs.Size(); ++i) {
                DataCenter dc(dcs[i]);
                data_centers.insert(std::lower_bound(data_centers.begin(),
                  data_centers.end(), dc), dc);
              }
            } else {
              throw Exception("Data centers are not valid for the cluster " \
                "object");
            }
          } else {
            throw Exception("JSON object is not a cluster object");
          }
        } else {
          throw Exception("JSON document is not an object");
        }
      } else {
        throw Exception("JSON document cannot be NULL");
      }
    }
  };

  /**
   * Default number of data centers to apply
   */
  static const std::vector<unsigned int> DEFAULT_DATA_CENTER_NODES;

  /**
   * Initialize the Simulacron cluster
   *
   * @throws SimulacronCluster::Exception If Simulacron JAR is unavailable
   */
  SimulacronCluster();

  /**
   * Terminate all Simulacron clusters and perform any additional cleanup
   * operations
   */
  ~SimulacronCluster();

  /**
   * Get a comma separated list of IPv4 addresses for nodes in the active
   * Simulacron cluster
   *
   * @param is_all True if all node IPv4 addresses should be returned; false
   *               if only the `UP` nodes (default: true)
   * @return Comma separated list of IPv4 addresses
   */
  std::string cluster_contact_points(bool is_all = true);

  /**
   * Create the Simulacron cluster; data centers and nodes within each data
   * center
   *
   * @param data_center_nodes Data center(s) to create in the Simulacron cluster
   *                          (default: 1 data center with 1 node)
   * @param with_vnode True if vnodes should be enabled; false otherwise
   *                   (default: false)
   */
  void create_cluster(const std::vector<unsigned int>& data_center_nodes = DEFAULT_DATA_CENTER_NODES,
                      bool with_vnodes = false);

  /**
   * Create the Simulacron cluster; number of nodes in data center 1 and 2
   *
   * @param data_center_one_nodes Number of nodes in data center 1
   * @param data_center_two_nodes Number of nodes in data center 2 (default: 0)
   * @param with_vnode True if vnodes should be enabled; false otherwise
   *                   (default: false)
   */
  void create_cluster(unsigned int data_center_one_nodes,
                      unsigned int data_center_two_nodes = 0,
                      bool with_vnodes = false);

  /**
   * Remove all Simulacron clusters
   */
  void remove_cluster();

  /**
   * Get the IPv4 address being utilized for a given node
   *
   * @param node Node to get IPv4 address
   * @return IPv4 address being used by the node
   * @throws SimulacronCluster::Exception If node is not valid
   */
  std::string get_ip_address(unsigned int node);

  /**
   * Check to see if a node is no longer accepting connections
   *
   * NOTE: This method may check the status of the node multiple times
   *
   * @param node Node to check 'DOWN' status
   * @return True if node is no longer accepting connections; false otherwise
   * @throws Exception if node is not valid
   */
  bool is_node_down(unsigned int node);

  /**
   * Check to see if a node is ready to accept connections
   *
   * NOTE: This method may check the status of the node multiple times
   *
   * @param node Node to check 'UP' status
   * @return True if node is ready to accept connections; false otherwise
   * @throws Exception if node is not valid or process is not running
   */
  bool is_node_up(unsigned int node);

  /**
   * Get the current cluster
   *
   * @return Cluster
   */
  Cluster cluster();

  /**
   * Get the data centers in the cluster
   *
   * @return List of data centers in the cluster
   */
  std::vector<Cluster::DataCenter> data_centers();

  /**
   * Get the nodes in the cluster
   *
   * @return List of nodes in the cluster
   */
  std::vector<Cluster::DataCenter::Node> nodes();

  /**
   * Get the active (current) connections on a given node in the Simulacron
   * cluster
   *
   * @param node Node to remove recorded prepared statements from
   * @return Number of connections to the node
   */
  unsigned int active_connections(unsigned int node);

  /**
   * Get the active (current) connections in the Simulacron cluster
   *
   * @return Number of connections to the cluster
   */
  unsigned int active_connections();

  ///////////////////////// PRIMING QUERIES /////////////////////////////////

  /**
   * Prime the queries on Simulacron cluster (or node) using the REST API
   *
   * @param request Request to prime
   * @param node Node to prime request on (defaultL 0 - all nodes)
   */
  void prime_query(prime::Request& request, unsigned int node = 0);

  /**
   * Remove all the primed queries in the Simulacron cluster (or node)
   *
   * @param node Node to remove prime queries on (default 0: - all nodes)
   */
  void remove_primed_queries(unsigned int node = 0);

private:
  /**
   * DSE release version
   */
  std::string dse_version_;
  /**
   * Cassandra release version
   */
  std::string cassandra_version_;
  /**
   * The current cluster ID
   */
  unsigned int current_cluster_id_;
  /**
   * Mutex for Simulacron process piped buffer allocation and reads
   */
  static uv_mutex_t mutex_;
  /**
   * Thread for Simulacron process
   */
  static uv_thread_t thread_;
  /**
   * Flag to determine if the Simulacron is ready to accept payload(s)
   */
  static bool is_ready_;
  /**
   * Flag to determine if the Simulacron process is already running
   */
  static bool is_running_;

  /**
   * uv_read_start callback for allocating memory for the buffer in the pipe
   *
   * @param handle Handle information for the pipe being read
   * @param suggested_size Suggested size for the buffer
   * @param buffer Buffer to allocate bytes for
   */
  static void handle_allocation(uv_handle_t* handle,
                                size_t suggested_size,
                                uv_buf_t* buffer);

  /**
   * uv_spawn callback for handling the completion of the process
   *
   * @param process Process
   * @param error_code Error/Exit code
   * @param term_signal Terminating signal
   */
  static void handle_exit(uv_process_t* process,
                          int64_t error_code,
                          int term_signal);

  /**
   * uv_read_start callback for processing the buffer in the pipe
   *
   * @param stream Stream to process (stdout/stderr)
   * @param buffer_length Length of the buffer
   * @param buffer Buffer to process
   */
  static void handle_read(uv_stream_t* stream,
                          ssize_t buffer_length,
                          const uv_buf_t* buffer);

  /**
   * uv_thread_create callback for executing the Simulacron process
   *
   * @param arg Information to start the Simulacron process
   */
  static void handle_thread_create(void* arg);

  /**
   * DELETE request to send to the Simulacron REST server
   *
   * @param endpoint The endpoint (URI) for the request
   * @throws SimulacronCluster::Exception If status code is not 202
   */
  void send_delete(const std::string& endpoint);

  /**
   * GET request to send to the Simulacron REST server
   *
   * @param endpoint The endpoint (URI) for the request
   * @throws SimulacronCluster::Exception If status code is not 200
   */
  const std::string send_get(const std::string& endpoint);

  /**
   * POST request to send to the Simulacron REST server.
   *
   * @param endpoint The endpoint (URI) for the request
   * @param content The content of the POST (default: empty)
   * @return REST server response
   * @throws SimulacronCluster::Exception If status code is not 201
   */
  const std::string send_post(const std::string& endpoint,
                              const std::string& content = "");

  /**
   * Send the request to the Simulacron REST server
   *
   * @param method Type of request to send to the REST server
   * @param request Request to send to the REST server
   * @param content The content of the request (default: empty)
   * @return REST server response
   * @throws SimulacronCluster::Exception
   */
  Response send_request(Request::Method method,
                        const std::string& endpoint,
                        const std::string& content = "");

  /**
   * Determine if a node is available
   *
   * @param node Simulacron node to check
   * @return True if node is available; false otherwise
   * @throws Exception if node is not valid or process is not running
   */
  bool is_node_available(unsigned int node);

  /**
   * Determine if a node is available
   *
   * @param ip_address IPv4 address of the Simulacron node
   * @param port Port of the Simulacron node
   * @return True if node is available; false otherwise
   */
  bool is_node_available(const std::string& ip_address, unsigned short port);

  /**
   * Generate a node endpoint from the current nodes in the cluster
   *
   * @param node Node to generate endpoint from
   * @return <data_center_id>/<node_id> if node is valid; 0 node will return
   *         empty string
   * @throws Exception Invalid node
   */
  const std::string generate_node_endpoint(unsigned int node);
};

} // namespace test

#endif // USE_SIMULACRON_SERVER

#endif // __TEST_SIMULACRON_CLUSTER_HPP__
