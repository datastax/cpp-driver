/*
  Copyright (c) 2014-2015 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp> // Sleep functionality

#include <uv.h>

#include "cassandra.h"
#include "testing.hpp"
#include "test_utils.hpp"

#define DEFAULT_CASSANDRA_NODE_PORT 9042

struct LatencyAwarePolicyTest {
public:
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm_;

  LatencyAwarePolicyTest()
    : configuration_(cql::get_ccm_bridge_configuration())
    , cluster_(cass_cluster_new())
    , thread_()
#if UV_VERSION_MAJOR == 0
    , loop_(uv_loop_new()) {
# else
  {
    boost::debug::detect_memory_leaks(false);
    uv_loop_init(&loop_);
#endif
    // Initialize the cluster for latency aware
    cass_cluster_set_reconnect_wait_time(cluster_.get(), 1);
    cass_cluster_set_connect_timeout(cluster_.get(), 240 * test_utils::ONE_SECOND_IN_MICROS);
    cass_cluster_set_request_timeout(cluster_.get(), 240 * test_utils::ONE_SECOND_IN_MICROS);
    test_utils::initialize_contact_points(cluster_.get(), configuration_.ip_prefix(), 3, 0);
    cass_cluster_set_latency_aware_routing(cluster_.get(), cass_true);
    cass_cluster_set_latency_aware_routing_settings(cluster_.get(), 1e6, 1, 1, 1, 1);
    cass_cluster_set_protocol_version(cluster_.get(), 1); // Protocol for this test doesn't matter so simply support all C* versions

    // Create the cluster and connect
    ccm_ = cql::cql_ccm_bridge_t::create_and_start(configuration_, "test", 3, 0);
    session_ = test_utils::create_session(cluster_.get());
  }

  ~LatencyAwarePolicyTest() {
    stop_query_execution();
#if UV_VERSION_MAJOR == 0
    uv_loop_delete(loop_);
    loop - NULL;
#else
    uv_loop_close(&loop_);
#endif

    test_utils::CassFuturePtr close_future(cass_session_close(session_.get()));
    cass_future_wait(close_future.get());
  }

  /**
   * Start the query execution thread
   */
  void start_query_execution() {
    uv_thread_create(&thread_, start_thread, NULL);
  }

  /**
   * Stop the executing query thread
   */
  void stop_query_execution() {
    is_running_ = false;
    uv_thread_join(&thread_);
  }

  /**
   * Create latency on a node using CCM to pause/resume execution of the node
   *
   * @param node Node to create latency on
   * @param latency Latency to incur
   */
  void create_latency(int node, unsigned int latency) {
    // Add latency to the node
    ccm_->pause(node);
    boost::this_thread::sleep_for(boost::chrono::milliseconds(latency));
    ccm_->resume(node);
  }

  /**
   * Check the maximum latency incurred on a node and ensure that is it within
   * accepted threshold/tolerance
   *
   * @param node Node to check against
   * @param expected_latency Latency incurred
   * @param tolerance Acceptable percentage threshold/tolerance
   */
  void check_max_latency(int node, unsigned int expected_latency, double tolerance) {
      double latency = max_node_latency[node - 1];
      if (latency < expected_latency) {
        BOOST_CHECK_CLOSE(latency, expected_latency, tolerance);
      }
  }

private:
  static test_utils::CassSessionPtr session_;
  const cql::cql_ccm_bridge_configuration_t& configuration_;
  test_utils::CassClusterPtr cluster_;
  static bool is_running_;
#if UV_VERSION_MAJOR == 0
  static uv_loop_t* loop_;
#else
  static uv_loop_t loop_;
#endif
  uv_thread_t thread_;
  static double max_node_latency[3];

  /**
   * Start the executing query thread
   *
   * @param arg Not used
   */
  static void start_thread(void* arg) {
    execute_query();
#if UV_VERSION_MAJOR == 0
    uv_run(loop_, UV_RUN_DEFAULT)
#else
    uv_run(&loop_, UV_RUN_DEFAULT);
#endif
  }

  /**
   * Execute a query on the system table
   */
  static void execute_query() {
    is_running_ = true;
    while(is_running_) {
      std::string query = "SELECT * FROM system.local";
      test_utils::CassStatementPtr statement(cass_statement_new_n(query.data(), query.size(), 0));
      test_utils::CassFuturePtr future(cass_session_execute(session_.get(), statement.get()));
      CassError error_code = test_utils::wait_and_return_error(future.get(),
                                                               240 * test_utils::ONE_SECOND_IN_MICROS);

      // Ignore all timing errors
      if (error_code != CASS_OK &&
          error_code != CASS_ERROR_LIB_REQUEST_TIMED_OUT &&
          error_code != CASS_ERROR_SERVER_READ_TIMEOUT) {
        CassString message;
        cass_future_error_message(future.get(), &message.data, &message.length);
        BOOST_FAIL(std::string(message.data, message.length) << "' (" << cass_error_desc(error_code) << ")");
      }

      // Get the host latency
      std::string host_ip_address = cass::get_host_from_future(future.get());
      double host_latency = static_cast<double>(cass::get_host_latency_average(session_.get(),
                                                                               host_ip_address,
                                                                               DEFAULT_CASSANDRA_NODE_PORT)) / 1e6;

      // Update the max latency incurred
      std::stringstream node_value;
      node_value << host_ip_address[host_ip_address.length() - 1];
      int node = -1;
      if (!(node_value >> node).fail()) {
        if (max_node_latency[node - 1] < host_latency) {
          max_node_latency[node - 1] = host_latency;
        }
      }
    }
  }
};

// Define static instances
test_utils::CassSessionPtr LatencyAwarePolicyTest::session_;
bool LatencyAwarePolicyTest::is_running_ = true;
#if UV_VERSION_MAJOR == 0
  uv_loop_t* LatencyAwarePolicyTest::loop_ = NULL;
#else
  uv_loop_t LatencyAwarePolicyTest::loop_;
#endif
double LatencyAwarePolicyTest::max_node_latency[3] = { 0.0 };

BOOST_FIXTURE_TEST_SUITE(latency_aware_policy, LatencyAwarePolicyTest)

/**
 * Latency Aware Policy - Ensure Node Latency
 *
 * This test ensures that the nodes incur latency by using CCM pause and resume
 * to simulate network latency on a node
 *
 * @since 2.0.0
 * @jira_ticket CPP-150
 * @test_category load_balancing_policy:latency_aware
 */
BOOST_AUTO_TEST_CASE(ensure_latency) {
  // Start gathering latencies for nodes
  start_query_execution();
  boost::this_thread::sleep_for(boost::chrono::milliseconds(1000));

  /*
   * Create varying amounts of latency and ensure maximum latency using a threshold
   */
  create_latency(1, 500);
  create_latency(2, 500);
  create_latency(3, 500);
  check_max_latency(1, 500, 0.05);
  check_max_latency(2, 500, 0.05);
  check_max_latency(3, 500, 0.05);

  create_latency(1, 1000);
  create_latency(2, 1000);
  create_latency(3, 1000);
  check_max_latency(1, 1000, 0.025);
  check_max_latency(2, 1000, 0.025);
  check_max_latency(3, 1000, 0.025);

  create_latency(1, 2000);
  create_latency(2, 2000);
  create_latency(3, 2000);
  check_max_latency(1, 2000, 0.01);
  check_max_latency(2, 2000, 0.01);
  check_max_latency(3, 2000, 0.01);

  create_latency(1, 3000);
  create_latency(2, 3000);
  create_latency(3, 3000);
  check_max_latency(1, 3000, 0.01);
  check_max_latency(2, 3000, 0.01);
  check_max_latency(3, 3000, 0.01);
}

BOOST_AUTO_TEST_SUITE_END()

