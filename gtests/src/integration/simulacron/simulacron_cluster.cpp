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

#include "simulacron_cluster.hpp"
#include "options.hpp"
#include "test_utils.hpp"
#include "tlog.hpp"

#include "scoped_lock.hpp"
#include "tsocket.hpp"
#include "values/uuid.hpp"
#include "objects/uuid_gen.hpp"

#include <algorithm>
#include <sstream>

#define SIMULACRON_LISTEN_ADDRESS "127.0.0.1"
#define SIMULACRON_ADMIN_PORT 8187
#define SIMULACRON_LOG_LEVEL "DEBUG"

#define HTTP_EOL "\r\n"
#define OUTPUT_BUFFER_SIZE 10240ul
#define SIMULACRON_NAP 100
#define SIMULACRON_CONNECTION_RETRIES 600 // Up to 60 seconds for retry based on SIMULACRON_NAP
#define SIMULACRON_PROCESS_RETRIS 100 // Up to 10 seconds for retry based on SIMULACRON_NAP
#define MAX_TOKEN static_cast<uint64_t>(INT64_MAX) - 1
#define DATA_CENTER_PREFIX "dc"

// Initialize the mutex, running status, thread, and default data center nodes
uv_mutex_t test::SimulacronCluster::mutex_;
bool test::SimulacronCluster::is_ready_ = false;
bool test::SimulacronCluster::is_running_ = false;
uv_thread_t test::SimulacronCluster::thread_;
const unsigned int DEFAULT_NODE[] = { 1 };
const std::vector<unsigned int> test::SimulacronCluster::DEFAULT_DATA_CENTER_NODES(
  DEFAULT_NODE, DEFAULT_NODE + sizeof(DEFAULT_NODE) / sizeof(DEFAULT_NODE[0]));

test::SimulacronCluster::SimulacronCluster()
  : dse_version_("")
  , cassandra_version_("")
  , current_cluster_id_(-1)
{
  // Initialize the mutex
  uv_mutex_init(&mutex_);

  // Determine if Simulacron file exists
  if (!test::Utils::file_exists(SIMULACRON_SERVER_JAR)) {
    std::stringstream message;
    message << "Unable to find Simulacron JAR file ["
      << SIMULACRON_SERVER_JAR << "]";
    throw Exception(message.str());
  }

  // Determine the release version (for priming nodes)
  CCM::CassVersion cassandra_version = Options::server_version();
  if (Options::is_dse()) {
    CCM::DseVersion dse_version(cassandra_version);
    cassandra_version = dse_version.get_cass_version();
    if (cassandra_version == "0.0.0") {
      throw Exception("Unable to determine Cassandra version from DSE version");
    }
    dse_version_ = test::Utils::replace_all(dse_version.to_string(), "-",
      ".");
  }
  cassandra_version_ = test::Utils::replace_all(cassandra_version.to_string(),
    "-", ".");

  // Create Simulacron process (threaded) and wait for completely availability
  uv_thread_create(&thread_, handle_thread_create, NULL);
  uint64_t elapsed = 0ul;
  uint64_t start_time = uv_hrtime();
  while (!is_ready_ && elapsed < 30000) { // Wait for a maximum of 30s
    Utils::msleep(SIMULACRON_NAP);
    TEST_LOG("Waiting for Simulacron Availability: Elapsed wait " << elapsed << "ms");
    elapsed = (uv_hrtime() - start_time) / 1000000UL;
  }
  TEST_LOG("Simulacron Status: " << (is_ready_ ? "Available" : "Not available"));
  test::Utils::wait_for_port(SIMULACRON_LISTEN_ADDRESS, SIMULACRON_ADMIN_PORT);
}

test::SimulacronCluster::~SimulacronCluster() {
  remove_cluster();
}

std::string test::SimulacronCluster::cluster_contact_points(bool is_all /*= true*/) {
  // Iterate over each node and collect the list of active contact points
  std::vector<Cluster::DataCenter::Node> cluster_nodes = nodes();
  size_t index = 1;
  std::stringstream contact_points;
  for (std::vector<Cluster::DataCenter::Node>::iterator it =
    cluster_nodes.begin(); it != cluster_nodes.end(); ++it) {
    if (is_all || is_node_up(index++)) {
      if (!contact_points.str().empty()) {
        contact_points << ",";
      }
      contact_points << it->ip_address;
    }
  }
  return contact_points.str();
}

void test::SimulacronCluster::create_cluster(const std::vector<unsigned int>& data_center_nodes /*= DEFAULT_DATA_CENTER_NODES*/,
                                             bool with_vnodes /*= false */) {
  std::stringstream paramters;
  std::stringstream cluster_name;
  cluster_name << DEFAULT_CLUSTER_PREFIX << "_";

  // Add the data centers, Cassandra version, and token/vnodes parameters
  paramters << "data_centers="
    << test::Utils::implode<unsigned int>(data_center_nodes, ',')
    << "&cassandra_version=" << cassandra_version_;
  // Update the cluster configuration (set num_tokens)
  if (with_vnodes) {
    // Maximum number of tokens is 1536
    paramters << "&num_tokens=1536";
  } else {
    paramters << "&num_tokens=1";
  }

  // Add the DSE version (if applicable)
  if (Options::is_dse()) {
    paramters << "&dse_version=" << dse_version_;
    cluster_name << dse_version_;
  } else {
    cluster_name << cassandra_version_;
  }

  // Add the cluster name
  cluster_name << "_"
    << test::Utils::implode<unsigned int>(data_center_nodes, '-');
  if (with_vnodes) {
    cluster_name << "-vnodes";
  }
  paramters << "&name=" << cluster_name.str();

  // Create the cluster and get the cluster ID
  std::string endpoint = "cluster?" + paramters.str();
  std::string response = send_post(endpoint);
  rapidjson::Document document;
  document.Parse(response.c_str());
  current_cluster_id_ = Cluster(&document).id;
}

void test::SimulacronCluster::create_cluster(unsigned int data_center_one_nodes,
  unsigned int data_center_two_nodes /*= 0*/,
  bool with_vnodes /*=false */) {
  std::vector<unsigned int> data_center_nodes;
  if (data_center_one_nodes > 0) {
    data_center_nodes.push_back(data_center_one_nodes);
  }
  if (data_center_two_nodes > 0) {
    data_center_nodes.push_back(data_center_two_nodes);
  }
  create_cluster(data_center_nodes, with_vnodes);
}

void test::SimulacronCluster::remove_cluster() {
  send_delete("cluster");
}

std::string test::SimulacronCluster::get_ip_address(unsigned int node) {
  std::vector<Cluster::DataCenter::Node> current_nodes = nodes();
  if (node > 0 && node <= current_nodes.size()) {
    return current_nodes[node - 1].ip_address;
  }
  return "";
}

bool test::SimulacronCluster::is_node_down(unsigned int node) {
  // Attempt to connect to the binary port on the node
  unsigned int number_of_retries = 0;
  while (number_of_retries++ < SIMULACRON_CONNECTION_RETRIES) {
    if (!is_node_available(node)) {
      return true;
    } else {
      TEST_LOG("Connected to Node " << node
        << " in Cluster: Rechecking node down status ["
        << number_of_retries << "]");
      test::Utils::msleep(SIMULACRON_NAP);
    }
  }

  // Connection can still be established on node
  return false;
}

bool test::SimulacronCluster::is_node_up(unsigned int node) {
  // Attempt to connect to the binary port on the node
  unsigned int number_of_retries = 0;
  while (number_of_retries++ < SIMULACRON_CONNECTION_RETRIES) {
    if (is_node_available(node)) {
      return true;
    } else {
      TEST_LOG("Unable to Connect to Node " << node
        << " in Cluster: Rechecking node up status ["
        << number_of_retries << "]");
      test::Utils::msleep(SIMULACRON_NAP);
    }
  }

  // Connection cannot be established on node
  return false;
}

test::SimulacronCluster::Cluster test::SimulacronCluster::cluster() {
  // Generate the JSON document response
  std::stringstream endpoint;
  endpoint << "cluster/" << current_cluster_id_;
  std::string response = send_get(endpoint.str());
  rapidjson::Document document;
  document.Parse(response.c_str());

  // Create and return the cluster
  return Cluster(&document);
}

std::vector<test::SimulacronCluster::Cluster::DataCenter> test::SimulacronCluster::data_centers() {
  // Get the cluster object and retrieve the data centers
  Cluster current_cluster = cluster();
  return current_cluster.data_centers;
}

std::vector<test::SimulacronCluster::Cluster::DataCenter::Node>
test::SimulacronCluster::nodes() {
  // Get the cluster object and retrieve the nodes from the data center(s)
  Cluster current_cluster = cluster();
  std::vector<Cluster::DataCenter::Node> nodes;
  std::vector<Cluster::DataCenter> data_centers = current_cluster.data_centers;
  for (std::vector<Cluster::DataCenter>::iterator it =
    data_centers.begin(); it != data_centers.end(); ++it) {
    std::vector<Cluster::DataCenter::Node> dc_nodes = it->nodes;
    nodes.insert(nodes.end(), dc_nodes.begin(), dc_nodes.end());
  }
  return nodes;
}

unsigned int test::SimulacronCluster::active_connections(unsigned int node) {
  std::vector<Cluster::DataCenter::Node> current_nodes = nodes();
  if (node > 0 && node <= current_nodes.size()) {
    return current_nodes[node - 1].active_connections;
  }
  return 0;
}

unsigned int test::SimulacronCluster::active_connections() {
  return cluster().active_connections;
}

void test::SimulacronCluster::prime_query(prime::Request& request,
                                          unsigned int node /*= 0*/) {
  std::stringstream endpoint;
  endpoint << "prime/" << current_cluster_id_ << generate_node_endpoint(node);
  send_post(endpoint.str(), request.json());
}

void test::SimulacronCluster::remove_primed_queries(unsigned int node /*= 0*/) {
  std::stringstream endpoint;
  endpoint << "prime/" << current_cluster_id_ << generate_node_endpoint(node);
  send_delete(endpoint.str());
}

void test::SimulacronCluster::handle_exit(uv_process_t* process,
                                          int64_t error_code,
                                          int term_signal) {
  cass::ScopedMutex lock(&mutex_);
  TEST_LOG("Process " << process->pid << " Terminated: " << error_code);
  uv_close(reinterpret_cast<uv_handle_t*>(process), NULL);
}

void test::SimulacronCluster::handle_allocation(uv_handle_t* handle,
                                                size_t suggested_size,
                                                uv_buf_t* buffer) {
  cass::ScopedMutex lock(&mutex_);
  buffer->base = new char[OUTPUT_BUFFER_SIZE];
  buffer->len = OUTPUT_BUFFER_SIZE;
}

void test::SimulacronCluster::handle_thread_create(void* arg) {
  // Initialize the loop and process arguments
  uv_loop_t loop;
  if(uv_loop_init(&loop) != 0) {
    throw Exception("Unable to Create Simulacron Process: libuv loop " \
      "initialization failed");
  };
  uv_process_options_t options;
  memset(&options, 0, sizeof(uv_process_options_t));

  // Create the options for reading information from the spawn pipes
  uv_pipe_t standard_output;
  uv_pipe_t error_output;
  uv_pipe_init(&loop, &standard_output, 0);
  uv_pipe_init(&loop, &error_output, 0);
  uv_stdio_container_t stdio[3];
  options.stdio_count = 3;
  options.stdio = stdio;
  options.stdio[0].flags = UV_IGNORE;
  options.stdio[1].flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_WRITABLE_PIPE);
  options.stdio[1].data.stream = (uv_stream_t*) &standard_output;
  options.stdio[2].flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_WRITABLE_PIPE);
  options.stdio[2].data.stream = (uv_stream_t*) &error_output;

  // Generate the spawn command for use with uv_spawn
  const char* spawn_command[7];
  spawn_command[0] = "java";
  spawn_command[1] = "-jar";
  spawn_command[2] = SIMULACRON_SERVER_JAR;
  spawn_command[3] = "--loglevel";
  spawn_command[4] = SIMULACRON_LOG_LEVEL;
  spawn_command[5] = "--verbose";
  spawn_command[6] = NULL;

  // Create the options for the process
  options.args = const_cast<char**>(spawn_command);
  options.exit_cb = handle_exit;
  options.file = spawn_command[0];

  // Start the process
  uv_process_t process;
  int error_code = uv_spawn(&loop, &process, &options);
  if (error_code == 0) {
    TEST_LOG("Launched " << spawn_command[0] << " with ID "
      << process.pid);

    // Start the output thread loops
    uv_read_start(reinterpret_cast<uv_stream_t*>(&standard_output),
      handle_allocation, handle_read);
    uv_read_start(reinterpret_cast<uv_stream_t*>(&error_output),
      handle_allocation, handle_read);

    // Start the process loop
    is_running_ = true;
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
    is_running_ = false;
  } else {
    TEST_LOG_ERROR(uv_strerror(error_code));
  }
}

void test::SimulacronCluster::handle_read(uv_stream_t* stream,
  ssize_t buffer_length, const uv_buf_t* buffer) {
  cass::ScopedMutex lock(&mutex_);
  if (buffer_length > 0) {
    // Process the buffer and log it
    std::string message(buffer->base, buffer_length);
    TEST_LOG(Utils::trim(message));

    // Check to see if Simulacron is ready to accept connections
    if (Utils::contains(message, "Started HTTP server interface")) {
      is_ready_ = true;
    }
  } else if (buffer_length < 0) {
    uv_close(reinterpret_cast<uv_handle_t*>(stream), NULL);
  }

  // Clean up the memory allocated
  delete[] buffer->base;
}

void test::SimulacronCluster::send_delete(const std::string& endpoint) {
  Response response = send_request(Request::HTTP_METHOD_DELETE, endpoint);
  if (response.status_code != 202) {
    std::stringstream message;
    message << "DELETE Operation " << endpoint
      << " did not Complete Successfully: " << response.status_code;
    throw Exception(message.str());
  }
}

const std::string test::SimulacronCluster::send_get(
  const std::string& endpoint) {
  Response response = send_request(Request::HTTP_METHOD_GET, endpoint);
  if (response.status_code != 200) {
    std::stringstream message;
    message << "GET Operation " << endpoint
      << " did not Complete Successfully: " << response.status_code;
    throw Exception(message.str());
  }
  return response.message;
}

const std::string test::SimulacronCluster::send_post(
  const std::string& endpoint, const std::string& content /*= ""*/) {
  Response response = send_request(Request::HTTP_METHOD_POST, endpoint,
    content);
  if (response.status_code != 201) {
    std::stringstream message;
    message << "POST Operation " << endpoint
      << " did not Complete Successfully: " << response.status_code;
    throw Exception(message.str());
  }
  return response.message;
}

Response test::SimulacronCluster::send_request(Request::Method method,
  const std::string& endpoint,
  const std::string& content /*= ""*/) {
  // Create and send the request to the REST server
  Request request;
  request.method = method;
  request.address = SIMULACRON_LISTEN_ADDRESS;
  request.port = SIMULACRON_ADMIN_PORT;
  request.endpoint = endpoint;
  if (method == Request::HTTP_METHOD_POST && !content.empty()) {
    request.content = content;
  }
  return RestClient::send_request(request);
}

bool test::SimulacronCluster::is_node_available(unsigned int node) {
  // Determine if the node is valid
  std::vector<Cluster::DataCenter::Node> cluster_nodes = nodes();
  if (node > cluster_nodes.size()) {
    std::stringstream message;
    message << "Unable to Check Availability of Node: Node " << node
      << " is not a valid node";
    throw test::Exception(message.str());
  }

  // Determine if the node is available
  Cluster::DataCenter::Node cluster_node = cluster_nodes[node - 1];
  std::string ip_address = cluster_node.ip_address;
  unsigned short port = cluster_node.port;
  return is_node_available(ip_address, port);
}

bool test::SimulacronCluster::is_node_available(const std::string& ip_address,
  unsigned short port) {
  Socket socket;
  try {
    socket.establish_connection(ip_address, port);
    return true;
  } catch (...) {
    ; // No-op
  }

  // Unable to establish connection to node
  return false;
}

const std::string test::SimulacronCluster::generate_node_endpoint(unsigned int node) {
  std::stringstream endpoint;
  if (node > 0) {
    std::vector<Cluster::DataCenter::Node> current_nodes = nodes();
    if (node > current_nodes.size()) {
      std::stringstream message;
      message << "Insufficient Nodes in Cluster: Cluster contains "
        << current_nodes.size() << "; " << node << " is invalid";
      throw Exception(message.str());
    }
    endpoint << "/" << current_nodes[node - 1].data_center_id << "/"
      << current_nodes[node - 1].id;
  }
  return endpoint.str();
}
