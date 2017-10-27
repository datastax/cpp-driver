/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_SCASSANDRA_CLUSTER_HPP__
#define __TEST_SCASSANDRA_CLUSTER_HPP__

// Make sure SCassandra server is available during compile time
#ifdef DSE_USE_STANDALONE_SCASSANDRA_SERVER
#include "exception.hpp"
#include "priming_requests.hpp"
#include "shared_ptr.hpp"
#include "scassandra_rest_client.hpp"

#ifdef _CRT_SECURE_NO_WARNINGS
# undef _CRT_SECURE_NO_WARNINGS
#endif
#include <rapidjson/document.h>
#include <uv.h>

#include <cstdio>
#include <map>
#include <string>
#include <string.h>

// Forward declare for process struct in source
struct Process;

namespace test {

/**
 * SCassandra cluster for easily creating SCassandra instances/nodes
 */
class SCassandraCluster {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
      : test::Exception(message) {}
  };
  typedef std::map<unsigned int, std::vector<std::string> > ActiveConnectionsMap;
  typedef std::pair<unsigned int, std::vector<std::string> > ActiveConnectionsPair;
  /**
   * Default DSE workload to apply (Cassandra)
   */
  static const std::vector<unsigned int> DEFAULT_DATA_CENTER_NODES;

  /**
   * Initialize the SCassandra cluster
   *
   * @throws SCassandraCluster::Exception If SCassandra JAR is unavailable
   */
  SCassandraCluster();

  /**
   * Terminate all SCassandra clusters and perform any additional cleanup
   * operations
   */
  ~SCassandraCluster();

  /**
   * Get a comma separated list of IPv4 addresses for nodes in the active
   * SCassandra cluster
   *
   * @param is_all True if all node IPv4 addresses should be returned; false
   *               if only the `UP` nodes (default: true)
   * @return Comma separated list of IPv4 addresses
   */
  std::string cluster_contact_points(bool is_all = true);

  /**
   * Create the SCassandra cluster; data centers and nodes within each data
   * center
   *
   * @param data_center_nodes Data center(s) to create in the SCassandra cluster
   *                          (default: 1 data center with 1 node)
   */
  void create_cluster(
    std::vector<unsigned int> data_center_nodes = DEFAULT_DATA_CENTER_NODES);

  /**
   * Create the SCassandra cluster; number of nodes in data center 1 and 2
   *
   * @param data_center_one_nodes Number of nodes in data center 1
   * @param data_center_two_nodes Number of nodes in data center 2 (default: 0)
   */
  void create_cluster(unsigned int data_center_one_nodes,
    unsigned int data_center_two_nodes = 0);

  /**
   * Get the IPv4 address being utilized for a given node
   *
   * @param node Node to get IPv4 address
   * @return IPv4 address being used by the node
   * @throws SCassandraCluster::Exception If node is not valid
   */
  std::string get_ip_address(unsigned int node) const;

  /**
   * Get the IPv4 address prefix being utilized for the SCassandra cluster for a
   * given data center
   *
   * @param data_center Data center to get IPv4 address prefix (default: 1)
   * @return IPv4 address prefix used
   * @throws SCassandraCluster::Exception If data center is not valid
   */
  std::string get_ip_prefix(unsigned int data_center = 1) const;

  /**
   * Stop (terminate the SCassandra process) and destroy SCassandra cluster
   * (requires cluster to be re-created; processes and peers are cleared)
   *
   * @return True is cluster is down; false otherwise
   */
  bool destroy_cluster();

  /**
   * Start the SCassandra cluster
   *
   * @return True is cluster is up; false otherwise
   */
  bool start_cluster();

  /**
   * Stop the SCassandra cluster (terminate the SCassandra process)
   *
   * @return True is cluster is down; false otherwise
   */
  bool stop_cluster();

  /**
   * Start a node on the SCassandra cluster
   *
   * NOTE: If node is not valid false is returned
   *
   * @param node Node to start
   * @param wait_for_up True if waiting for node to be up; false otherwise
   * @return True if node was started (and is up if wait_for_up is true); false
   *         otherwise
   */
  bool start_node(unsigned int node, bool wait_for_up = true);

  /**
   * Stop a node on the SCassandra cluster
   *
   * NOTE: If node is not valid false is returned
   *
   * @param node Node to stop
   * @param wait_for_down True if waiting for node to be down; false otherwise
   * @return True if node was stopped (and is dpwn if wait_for_down is true);
   *         false otherwise
   */
  bool stop_node(unsigned int node, bool wait_for_down = true);

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
   * Get the nodes in the cluster
   *
   * @param is_available True if only active/available nodes should be returned;
   *                     false otherwise (DEFAULT: true)
   * @return List of nodes in the cluster; available nodes only if
   *         `is_available` is true
   */
  std::vector<unsigned int> nodes(bool is_available = true);

  /**
   * Prime the system tables (local and peers) for all the active nodes in the
   * SCassandra cluster
   */
  void prime_system_tables();

  /**
   * Reset the cluster; remove all activity from the cluster including primed
   * queries.
   */
  void reset_cluster();

  ///////////////////////////// ACTIVITY ////////////////////////////////////

  /**
   * Remove all the recorded connections on a given node in the SCassandra
   * cluster
   *
   * @param node Node to remove recorded connections from
   */
  void remove_recorded_connections(unsigned int node);

  /**
   * Remove all the recorded connections in the SCassandra cluster
   */
  void remove_recorded_connections();

  /**
   * Remove all the recorded executed batch statements on a given node in the
   * SCassandra cluster
   *
   * @param node Node to remove recorded executed batch statements from
   */
  void remove_recorded_executed_batch_statements(unsigned int node);

  /**
   * Remove all the recorded executed batch statements in the SCassandra cluster
   */
  void remove_recorded_executed_batch_statements();

  /**
   * Remove all the recorded executed prepared statements on a given node in the
   * SCassandra cluster
   *
   * @param node Node to remove recorded execute prepared statements from
   */
  void remove_recorded_executed_prepared_statements(unsigned int node);

  /**
   * Remove all the recorded executed prepared statements in the SCassandra
   * cluster
   */
  void remove_recorded_executed_prepared_statements();

  /**
   * Remove all the recorded executed queries on a given node in the SCassandra
   * cluster
   *
   * @param node Node to remove recorded executed queries from
   */
  void remove_recorded_executed_queries(unsigned int node);

  /**
   * Remove all the recorded executed queries in the SCassandra cluster
   */
  void remove_recorded_executed_queries();

  /**
   * Remove all the recorded prepared statements on a given node in the
   * SCassandra cluster
   *
   * @param node Node to remove recorded prepared statements from
   */
  void remove_recorded_prepared_statements(unsigned int node);

  /**
   * Remove all the recorded prepared statements in the SCassandra cluster
   */
  void remove_recorded_prepared_statements();

  ///////////////////////// CURRENT ENDPOINT ////////////////////////////////

  /**
   * Get the active (current) connections on a given node in the SCassandra
   * cluster
   *
   * @param node Node to remove recorded prepared statements from
   * @return List of connections on the node
   * @throws Exception if JSON response is invalid
   */
  std::vector<std::string> active_connections(unsigned int node);

  /**
   * Get the active (current) connections in the SCassandra cluster
   *
   * @return Map of connections on each node
   * @throws Exception if JSON response is invalid
   */
  ActiveConnectionsMap active_connections();

  ///////////////////////// PRIMING QUERIES /////////////////////////////////

  /**
   * Prime the queries on SCassandra cluster using the REST API
   *
   * @param request Request to prime
   */
  void prime_query(PrimingRequest request);

  /**
   * Prime the queries on SCassandra using the REST API
   *
   * @param node Node to prime the query on
   * @param request Request to prime
   */
  void prime_query(unsigned int node, PrimingRequest request);

  /**
   * Remove all the primed queries in the SCassandra cluster
   */
  void remove_primed_queries();

  /**
   * Remove the primed queries on a given node in the SCassandra cluster
   *
   * @param node Node to remove primed queries from
   */
  void remove_primed_queries(unsigned int node);

private:
  typedef std::map<unsigned int, Process> ProcessMap;
  typedef std::pair<unsigned int, Process> ProcessPair;
  typedef std::map<unsigned int, std::vector<Process*> > PeersMap;
  typedef std::pair<unsigned int, std::vector<Process*> > PeersPair;
  /**
   * Processes for each node in the SCassandra cluster
   */
  ProcessMap processes_;
  /**
   * Peers for a node in the SCassandra cluster
   */
  PeersMap peers_;
  /**
   * Cassandra release version
   */
  std::string release_version_;
  /**
   * Schema version
   */
  std::string schema_version_;
  /**
   * Mutex for process piped buffer allocation and reads
   */
  static uv_mutex_t mutex_;

#if UV_VERSION_MAJOR == 0
  /**
   * uv_read_start callback for allocating memory for the buffer in the pipe
   *
   * @param handle Handle information for the pipe being read
   * @param suggested_size Suggested size for the buffer
   */
  static uv_buf_t handle_allocation(uv_handle_t* handle, size_t suggested_size);
#else
  /**
   * uv_read_start callback for allocating memory for the buffer in the pipe
   *
   * @param handle Handle information for the pipe being read
   * @param suggested_size Suggested size for the buffer
   * @param buffer Buffer to allocate bytes for
   */
  static void handle_allocation(uv_handle_t* handle, size_t suggested_size,
    uv_buf_t* buffer);
#endif

  /**
   * uv_spawn callback for handling the completion of the process
   *
   * @param process Process
   * @param error_code Error/Exit code
   * @param term_signal Terminating signal
   */
#if UV_VERSION_MAJOR == 0
  static void handle_exit(uv_process_t* process, int error_code,
    int term_signal);
#else
  static void handle_exit(uv_process_t* process, int64_t error_code,
    int term_signal);
#endif

  /**
   * uv_read_start callback for processing the buffer in the pipe
   *
   * @param stream Stream to process (stdout/stderr)
   * @param buffer_length Length of the buffer
   * @param buffer Buffer to process
   */
#if UV_VERSION_MAJOR == 0
  static void handle_read(uv_stream_t* stream, ssize_t buffer_length,
    uv_buf_t buffer);
#else
  static void handle_read(uv_stream_t* stream, ssize_t buffer_length,
    const uv_buf_t* buffer);
#endif

  /**
   * uv_thread_create callback for executing the SCassandra process
   *
   * @param arg Information to start the SCassandra process
   */
  static void handle_thread_create(void* arg);

  /**
   * Create/Initialize the SCassandra processes for each node
   *
   * @param nodes Data center(s) to create in the SCassandra cluster
   */
  void create_processes(std::vector<unsigned int> nodes);

  /**
   * DELETE request to send to the SCassandra REST server
   *
   * @param node Node to send request to
   * @param endpoint The endpoint (URI) for the request
   * @throws SCassandraCluster::Exception
   */
  void send_delete(unsigned int node, const std::string& endpoint);

  /**
   * GET request to send to the SCassandra REST server
   *
   * @param node Node to send request to
   * @param endpoint The endpoint (URI) for the request
   * @throws SCassandraCluster::Exception
   */
  const std::string send_get(unsigned int node, const std::string& endpoint);

  /**
   * POST request to send to the SCassandra REST server.
   *
   * @param node Node to send request to
   * @param endpoint The endpoint (URI) for the request
   * @param content The content of the POST
   * @throws SCassandraCluster::Exception
   */
  void send_post(unsigned int node, const std::string& endpoint,
    const std::string& content);

  /**
   * Send the request to the SCassandra REST server
   *
   * @param method Type of request to send to the REST server
   * @param node Node to send request to
   * @param request Request to send to the REST server
   * @param content The content of the request (default: empty)
   * @return REST server response
   * @throws SCassandraCluster::Exception
   */
  Response send_request(Request::Method method, unsigned int node,
    const std::string& endpoint, const std::string& content = "");

  /**
   * Determine if a node is available
   *
   * @param node SCassandra node to check
   * @return True if node is available; false otherwise
   * @throws Exception if node is not valid or process is not running
   */
  bool is_node_available(unsigned int node);

  /**
   * Determine if a node is available
   *
   * @param ip_address IPv4 address of the SCassandra node
   * @param port Port of the SCassandra node
   * @return True if node is available; false otherwise
   */
  bool is_node_available(const std::string& ip_address, unsigned short port);

  /**
   * Generate the token ranges (no v-nodes) for a single data center
   *
   * @param nodes Data center to generate tokens for
   * @param nodes Number of nodes to generate tokens for
   * @return Token ranges for each node
   */
  std::vector<std::string> generate_token_ranges(unsigned int data_center,
    unsigned int nodes);

  /**
   * Prime the system tables (local and peers) on the selected node
   *
   * @param node Node being primed
   */
  void prime_system_tables(unsigned int node);
};

} // namespace test

#endif

#endif // __TEST_SCASSANDRA_CLUSTER_HPP__
