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

#include <limits>

#include "cassandra.h"
#include "test_utils.hpp"

struct MetricsTest {
public:
  test_utils::CassClusterPtr cluster_;
  const cql::cql_ccm_bridge_configuration_t& configuration_;
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm_;

  MetricsTest()
    : cluster_(cass_cluster_new())
    , configuration_(cql::get_ccm_bridge_configuration()) {
    boost::debug::detect_memory_leaks(false);
  }

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
  test_utils::initialize_contact_points(cluster_.get(), configuration_.ip_prefix(), 3, 0);
  ccm_ = cql::cql_ccm_bridge_t::create_and_start(configuration_, "test", 3, 0);
  create_session();
  boost::this_thread::sleep_for(boost::chrono::seconds(1));

  CassMetrics metrics;
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 3);
  ccm_->stop(1);
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 2);
  ccm_->stop(2);
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 1);
  ccm_->stop(3);
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 0);
  ccm_->start(1);
  boost::this_thread::sleep_for(boost::chrono::seconds(1));
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 1);
  ccm_->start(2);
  boost::this_thread::sleep_for(boost::chrono::seconds(1));
  get_metrics(&metrics);
  BOOST_CHECK_EQUAL(metrics.stats.total_connections, 2);
  ccm_->start(3);
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
  test_utils::initialize_contact_points(cluster_.get(), configuration_.ip_prefix(), 2, 0);

  /*
   * Check for connection timeouts
   */
  cass_cluster_set_connect_timeout(cluster_.get(), 0);
  ccm_ = cql::cql_ccm_bridge_t::create_and_start(configuration_, "test", 2, 0);
  create_session(true);
  get_metrics(&metrics);
  BOOST_CHECK_GE(metrics.errors.connection_timeouts, 2);

  /*
   * Check for pending request timeouts
   */
  // Limit the connections to one
  cass_cluster_set_core_connections_per_host(cluster_.get(), 1);
  cass_cluster_set_max_connections_per_host(cluster_.get(), 1);
  // Lower connect timeout because this is what affects pending request timeout
  cass_cluster_set_connect_timeout(cluster_.get(), 100);
  // Make the number of pending requests really high to exceed the timeout
  cass_cluster_set_pending_requests_high_water_mark(cluster_.get(), 1000);
  ccm_ = cql::cql_ccm_bridge_t::create_and_start(configuration_, "test", 2, 0);
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

  /*
   * Check for request timeouts
   */
  cass_cluster_set_connect_timeout(cluster_.get(), 5 * test_utils::ONE_SECOND_IN_MICROS);
  cass_cluster_set_request_timeout(cluster_.get(), 0);
  ccm_ = cql::cql_ccm_bridge_t::create_and_start(configuration_, "test", 1, 0);
  create_session(true);
  get_metrics(&metrics);
  BOOST_CHECK_GE(metrics.errors.request_timeouts, 1);
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
  test_utils::initialize_contact_points(cluster_.get(), configuration_.ip_prefix(), 1, 0);
  ccm_ = cql::cql_ccm_bridge_t::create_and_start(configuration_, "test", 1, 0);
  create_session();

  CassMetrics metrics;
  boost::chrono::steady_clock::time_point end =
    boost::chrono::steady_clock::now() + boost::chrono::seconds(70);
  do {
    execute_query();
    get_metrics(&metrics);
  } while (boost::chrono::steady_clock::now() < end &&
    metrics.requests.one_minute_rate == 0.0);

  BOOST_CHECK_LT(metrics.requests.min, std::numeric_limits<uint64_t>::max());
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
