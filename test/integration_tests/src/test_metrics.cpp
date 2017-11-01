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

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp> // Sleep functionality

#include "cassandra.h"
#include "constants.hpp"
#include "test_utils.hpp"

struct MetricsTest {
public:
  test_utils::CassClusterPtr cluster_;
  boost::shared_ptr<CCM::Bridge> ccm_;

  MetricsTest()
    : cluster_(cass_cluster_new())
    , ccm_(new CCM::Bridge("config.txt")) {}

  ~MetricsTest() {
    close_session();
  }

  /**
   * Create the session
   *
   * @param is_timeout True if for timeout tests; false otherwise
   */
  void create_session(bool is_timeout = false) {
    close_session();
    test_utils::CassSessionPtr session(cass_session_new());
    test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster_.get()));
    CassError error_code = test_utils::wait_and_return_error(connect_future.get());
    session_ = test_utils::create_session(cluster_.get(), &error_code);
    if (error_code != CASS_OK) {
      if (is_timeout) {
        if (error_code == CASS_ERROR_LIB_NO_HOSTS_AVAILABLE) {
          return;
        }
      }

      CassString message;
      cass_future_error_message(connect_future.get(), &message.data, &message.length);
      BOOST_FAIL(std::string(message.data, message.length) << "' (" << cass_error_desc(error_code) << ")");
    }
  }

  /**
   * Close the session
   */
  void close_session() {
    if (session_) {
      test_utils::CassFuturePtr close_future(cass_session_close(session_.get()));
      cass_future_wait(close_future.get());
    }
  }

  /**
   * Get the driver metrics
   *
   * @param metrics Metrics to assign from the active session
   */
  void get_metrics(CassMetrics *metrics) {
    cass_session_get_metrics(session_.get(), metrics);
  }

 /**
  * Execute a query on the system table
  *
  * @param is_async True if async query; false otherwise
  */
  void execute_query(bool is_async = false) {
    std::string query = "SELECT * FROM system.local";
    test_utils::CassStatementPtr statement(cass_statement_new_n(query.data(), query.size(), 0));
    test_utils::CassFuturePtr future(cass_session_execute(session_.get(), statement.get()));
    if (!is_async) {
      test_utils::wait_and_check_error(future.get());
    }
  }

private:
  test_utils::CassSessionPtr session_;
};

BOOST_FIXTURE_TEST_SUITE(metrics, MetricsTest)

/**
 * Driver Metrics - Connection statistics
 *
 * This test ensures that the driver is reporting the proper connection
 * statistics
 *
 * @since 2.0.0
 * @jira_ticket CPP-188
 * @test_category metrics
 */
BOOST_AUTO_TEST_CASE(connections) {
  //Create one connections per host
  cass_cluster_set_num_threads_io(cluster_.get(), 1);
  cass_cluster_set_core_connections_per_host(cluster_.get(), 1);
  cass_cluster_set_reconnect_wait_time(cluster_.get(), 10); // Low re-connect for node restart
  test_utils::initialize_contact_points(cluster_.get(), ccm_->get_ip_prefix(), 3);
  if (ccm_->create_cluster(3)) {
    ccm_->start_cluster();
  }
  create_session();
  boost::this_thread::sleep_for(boost::chrono::seconds(1));

  CassMetrics metrics;
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 3);
  ccm_->stop_node(1);
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 2);
  ccm_->stop_node(2);
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 1);
  ccm_->stop_node(3);
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 0);
  ccm_->start_node(1);
  boost::this_thread::sleep_for(boost::chrono::seconds(1));
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 1);
  ccm_->start_node(2);
  boost::this_thread::sleep_for(boost::chrono::seconds(1));
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 2);
  ccm_->start_node(3);
  boost::this_thread::sleep_for(boost::chrono::seconds(1));
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 3);
}

/**
 * Driver Metrics - Timeouts
 *
 * This test ensures that the driver is reporting the proper timeouts for
 * connection and requests
 *
 * @since 2.0.0
 * @jira_ticket CPP-188
 * @test_category metrics
 */
BOOST_AUTO_TEST_CASE(timeouts) {
  CassMetrics metrics;
  cass_cluster_set_core_connections_per_host(cluster_.get(), 2);
  test_utils::initialize_contact_points(cluster_.get(), ccm_->get_ip_prefix(), 2);

  /*
   * Check for connection timeouts
   */
  cass_cluster_set_connect_timeout(cluster_.get(), 1);
  if (ccm_->create_cluster(2)) {
    ccm_->start_cluster();
  }
  create_session(true);
  get_metrics(&metrics);
  BOOST_CHECK_GE(metrics.errors.connection_timeouts, 2);

  /*
   * Check for pending request timeouts
   */
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version <= 2 && version.minor_version < 1) || version.major_version < 2) {
    // Limit the connections to one
    cass_cluster_set_core_connections_per_host(cluster_.get(), 1);
    cass_cluster_set_max_connections_per_host(cluster_.get(), 1);
    // Lower connect timeout because this is what affects pending request timeout
    cass_cluster_set_connect_timeout(cluster_.get(), 100);
    if (ccm_->create_cluster(2)) {
      ccm_->start_cluster();
    }
    create_session(true);

    // Execute async queries to create pending request timeouts
    for (int n = 0; n < 1000; ++n) {
      execute_query(true);
    }

    // Ensure the pending request has occurred
    boost::chrono::steady_clock::time_point end =
    boost::chrono::steady_clock::now() + boost::chrono::seconds(10);
    do {
      boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
      get_metrics(&metrics);
    } while (boost::chrono::steady_clock::now() < end &&
             metrics.errors.pending_request_timeouts == 0);
    BOOST_CHECK_GT(metrics.errors.pending_request_timeouts, 0);
  } else {
    std::cout << "Skipping Pending Request Timeout for Cassandra v" << version.to_string() << std::endl;
  }

  /*
   * Check for request timeouts
   */
  cass_cluster_set_connect_timeout(cluster_.get(), 30 * test_utils::ONE_SECOND_IN_MILLISECONDS);
  cass_cluster_set_request_timeout(cluster_.get(), 1);
  if (ccm_->create_cluster()) {
    ccm_->start_cluster();
  }
  create_session(true);
  for (int n = 0; n < 100; ++n) {
    execute_query(true);
  }

  // Ensure the request timeout has occurred
  boost::chrono::steady_clock::time_point end =
    boost::chrono::steady_clock::now() + boost::chrono::seconds(10);
  do {
    boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
    get_metrics(&metrics);
  } while (boost::chrono::steady_clock::now() < end &&
           metrics.errors.request_timeouts == 0);
  BOOST_CHECK_GT(metrics.errors.request_timeouts, 0);
}

/**
 * Driver Metrics - Request Statistics
 *
 * This test ensures that the histogram data calculated by the driver is being
 * updated.
 *
 * NOTE: The data returned by the driver is not validated as this is performed
 *       in the unit tests.
 *
 * @since 2.0.0
 * @jira_ticket CPP-188
 * @test_category metrics
 */
BOOST_AUTO_TEST_CASE(request_statistics) {
  //Create one connections per host
  cass_cluster_set_num_threads_io(cluster_.get(), 1);
  cass_cluster_set_core_connections_per_host(cluster_.get(), 1);
  test_utils::initialize_contact_points(cluster_.get(), ccm_->get_ip_prefix(), 1);
  if (ccm_->create_cluster()) {
    ccm_->start_cluster();
  }
  create_session();

  CassMetrics metrics;
  boost::chrono::steady_clock::time_point end =
    boost::chrono::steady_clock::now() + boost::chrono::seconds(70);
  do {
    execute_query();
    get_metrics(&metrics);
  } while (boost::chrono::steady_clock::now() < end &&
    metrics.requests.one_minute_rate == 0.0);

  BOOST_CHECK_LT(metrics.requests.min, CASS_UINT64_MAX);
  BOOST_CHECK_GT(metrics.requests.max, 0);
  BOOST_CHECK_GT(metrics.requests.mean, 0);
  BOOST_CHECK_GT(metrics.requests.stddev, 0);
  BOOST_CHECK_GT(metrics.requests.median, 0);
  BOOST_CHECK_GT(metrics.requests.percentile_75th, 0);
  BOOST_CHECK_GT(metrics.requests.percentile_95th, 0);
  BOOST_CHECK_GT(metrics.requests.percentile_98th, 0);
  BOOST_CHECK_GT(metrics.requests.percentile_99th, 0);
  BOOST_CHECK_GT(metrics.requests.percentile_999th, 0);
  BOOST_CHECK_GT(metrics.requests.mean_rate, 0.0);
  BOOST_CHECK_GT(metrics.requests.one_minute_rate, 0.0);
  BOOST_CHECK_EQUAL(metrics.requests.five_minute_rate, metrics.requests.one_minute_rate);
  BOOST_CHECK_EQUAL(metrics.requests.fifteen_minute_rate, metrics.requests.one_minute_rate);
}

BOOST_AUTO_TEST_SUITE_END()
