/*
  Copyright (c) 2014-2016 DataStax

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

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp> // Sleep functionality

#include <uv.h>

#include "cassandra.h"
#include "testing.hpp"
#include "test_utils.hpp"

#define DEFAULT_CASSANDRA_NODE_PORT 9042
#define WARM_UP_QUERY_COUNT 25

struct LatencyAwarePolicyTest {
public:
  boost::shared_ptr<CCM::Bridge> ccm_;

  LatencyAwarePolicyTest()
    : ccm_(new CCM::Bridge("config.txt"))
    , cluster_(cass_cluster_new())
    , thread_() {
    uv_mutex_init(&lock_);
    uv_cond_init(&condition_);

    // Create the cluster
    if (ccm_->create_cluster(3)) {
      ccm_->start_cluster();
    }

    // Initialize the cluster for latency aware
    cass_cluster_set_reconnect_wait_time(cluster_.get(), 1);
    cass_cluster_set_connect_timeout(cluster_.get(), 240 * test_utils::ONE_SECOND_IN_MICROS);
    cass_cluster_set_request_timeout(cluster_.get(), 240 * test_utils::ONE_SECOND_IN_MICROS);
    test_utils::initialize_contact_points(cluster_.get(), ccm_->get_ip_prefix(), 3);
    cass_cluster_set_latency_aware_routing(cluster_.get(), cass_true);
    cass_cluster_set_latency_aware_routing_settings(cluster_.get(), 1e6, 1, 1, 1, 1);
    // Handle deprecated and removed protocol versions [CASSANDRA-10146]
    // https://issues.apache.org/jira/browse/CASSANDRA-10146
    int protocol_version = 1;
    if (test_utils::get_version() >= "3.0.0") {
      protocol_version = 3;
    }
    cass_cluster_set_protocol_version(cluster_.get(), protocol_version); // Protocol for this test doesn't matter so simply support all C* versions

    // Connect to the cluster
    session_ = test_utils::create_session(cluster_.get());
  }

  ~LatencyAwarePolicyTest() {
    stop_query_execution();
    uv_mutex_destroy(&lock_);
    uv_cond_destroy(&condition_);

    test_utils::CassFuturePtr close_future(cass_session_close(session_.get()));
    cass_future_wait(close_future.get());
  }

  /**
   * Start the query execution thread
   */
  void start_query_execution() {
    is_running_ = true;
    is_error_ = false;
    is_warming_up_ = true;
    memset(max_node_latency, 0, sizeof(max_node_latency));
    uv_thread_create(&thread_, start_thread, NULL);

    // Allow metrics to gather some initial data
    uv_mutex_lock(&lock_);
    while (is_warming_up_) {
      uv_cond_wait(&condition_, &lock_);
    }
    uv_mutex_unlock(&lock_);
  }

  /**
   * Stop the executing query thread
   */
  void stop_query_execution() {
    is_running_ = false;
    uv_thread_join(&thread_);
    if (is_error_) {
      BOOST_FAIL("Error occurred during query execution");
    }
  }

  /**
   * Create latency on a node using CCM to pause/resume execution of the node
   *
   * @param node Node to create latency on
   * @param latency Latency to incur
   */
  void create_latency(int node, unsigned int latency) {
    // Add latency to the node
    ccm_->pause_node(node);
    boost::this_thread::sleep_for(boost::chrono::milliseconds(latency));
    ccm_->resume_node(node);
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
      } else {
        // Ensure assertions are checked for testing framework
        BOOST_CHECK(latency >= expected_latency);
      }
  }

private:
  static test_utils::CassSessionPtr session_;
  test_utils::CassClusterPtr cluster_;
  static bool is_error_;
  static bool is_running_;
  static bool is_warming_up_;
  uv_thread_t thread_;
  static uv_cond_t condition_;
  static uv_mutex_t lock_;
  static double max_node_latency[3];

  /**
   * Start the executing query thread
   *
   * @param arg Not used
   */
  static void start_thread(void* arg) {
    execute_query();
  }

  /**
   * Execute a query on the system table
   */
  static void execute_query() {
    is_warming_up_ = true;
    int number_of_queries_executed = 0;

    do {
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

        // Indicate error occurred
        std::cerr << std::string(message.data, message.length) << "' (" << cass_error_desc(error_code) << ")" << std::endl;
        is_error_ = true;
        is_running_ = false;
      }

      if (!is_error_) {
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

        // Determine if warm up is complete
        uv_mutex_lock(&lock_);
        // Also signal if we're not running anymore
        if (++number_of_queries_executed == WARM_UP_QUERY_COUNT || !is_running_) {
          uv_cond_signal(&condition_);
          is_warming_up_ = false;
        }
        uv_mutex_unlock(&lock_);
      }
    } while (is_running_);
  }
};

// Define static instances
test_utils::CassSessionPtr LatencyAwarePolicyTest::session_;
bool LatencyAwarePolicyTest::is_error_ = false;
bool LatencyAwarePolicyTest::is_running_ = true;
bool LatencyAwarePolicyTest::is_warming_up_ = true;
uv_cond_t LatencyAwarePolicyTest::condition_;
uv_mutex_t LatencyAwarePolicyTest::lock_;
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
  /*
   * Create varying amounts of latency and ensure maximum latency using a threshold
   */
  start_query_execution();
  create_latency(1, 2000);
  create_latency(2, 2000);
  create_latency(3, 2000);
  stop_query_execution();
  check_max_latency(1, 2000, 0.01);
  check_max_latency(2, 2000, 0.01);
  check_max_latency(3, 2000, 0.01);

  start_query_execution();
  create_latency(1, 500);
  create_latency(2, 500);
  create_latency(3, 500);
  stop_query_execution();
  check_max_latency(1, 500, 0.05);
  check_max_latency(2, 500, 0.05);
  check_max_latency(3, 500, 0.05);

  start_query_execution();
  create_latency(1, 3000);
  create_latency(2, 3000);
  create_latency(3, 3000);
  stop_query_execution();
  check_max_latency(1, 3000, 0.01);
  check_max_latency(2, 3000, 0.01);
  check_max_latency(3, 3000, 0.01);

  start_query_execution();
  create_latency(1, 1000);
  create_latency(2, 1000);
  create_latency(3, 1000);
  stop_query_execution();
  check_max_latency(1, 1000, 0.025);
  check_max_latency(2, 1000, 0.025);
  check_max_latency(3, 1000, 0.025);
}

BOOST_AUTO_TEST_SUITE_END()
