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

#include <algorithm>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>
#include <boost/atomic.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

struct LoggingTests {
  boost::shared_ptr<CCM::Bridge> ccm;

  LoggingTests()
    : ccm(new CCM::Bridge("config.txt")) {}
};

BOOST_FIXTURE_TEST_SUITE(logging, LoggingTests)

BOOST_AUTO_TEST_CASE(logging_callback)
{
  test_utils::CassLog::reset("Adding pool for host");

  if (ccm->create_cluster()) {
    ccm->start_cluster();
  }

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);
    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
  }

  BOOST_CHECK(test_utils::CassLog::message_count() > 0);
}

/**
 * Ensure logger error chatter is reduced during session connection
 *
 * This test will perform a connection against a single node cluster in which
 * the cluster is not currently active. This will force the driver to produce
 * an error log message and then after starting the cluster a new connection
 * attempt can be made to test the error log reduction to a warning by
 * terminating the connection after success.
 *
 * @jira_ticket CPP-337
 * @test_category logging
 * @since 2.4.0
 * @expected_result Logger error reduced to warning
 */
BOOST_AUTO_TEST_CASE(logging_connection_error_reduced)
{
  // Ensure the cluster is not running
  if (!ccm->create_cluster()) {
    ccm->stop_cluster();
  }

  {
    test_utils::CassLog::reset("Unable to establish a control connection to host");
    test_utils::CassLog::set_expected_log_level(CASS_LOG_ERROR);

    test_utils::CassClusterPtr cluster(cass_cluster_new());
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);
    test_utils::CassSessionPtr session(cass_session_new());
    test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster.get()));
    cass_future_error_code(connect_future.get());
    BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 1);

    test_utils::CassLog::reset("Lost control connection to host");
    test_utils::CassLog::set_expected_log_level(CASS_LOG_WARN);
    ccm->start_cluster();
    connect_future = test_utils::CassFuturePtr(cass_session_connect(session.get(), cluster.get()));
    cass_future_error_code(connect_future.get());
    ccm->stop_cluster();
    boost::this_thread::sleep_for(boost::chrono::seconds(5));
    BOOST_CHECK(test_utils::CassLog::message_count() > 0);
  }
  ccm->start_cluster();
}

/**
 * Ensure logger error chatter is reduced during reconnection attempts
 *
 * This test will perform a connection against a single node cluster where the
 * connection is performed asynchronously and the cluster is "paused" causing
 * a connection pool error. The cluster remains in a paused state for ten
 * seconds to ensure the reconnection of the pool is reduced to a warning log
 * level.
 *
 * @jira_ticket CPP-337
 * @test_category logging
 * @since 2.4.0
 * @expected_result Logger error reduced to warning
 */
BOOST_AUTO_TEST_CASE(logging_pool_error_reduced)
{
  test_utils::CassLog::reset("Connection pool was unable to connect to host");
  test_utils::CassLog::set_expected_log_level(CASS_LOG_ERROR);

  // Ensure the cluster is not running
  if (!ccm->create_cluster(2)) {
    ccm->stop_cluster();
  }

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    cass_cluster_set_connection_heartbeat_interval(cluster.get(), 1);
    cass_cluster_set_connection_idle_timeout(cluster.get(), 1);
    cass_cluster_set_request_timeout(cluster.get(), 1000);
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);
    test_utils::CassSessionPtr session(cass_session_new());
    ccm->start_cluster();
    ccm->pause_node(2);

    test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster.get()));
    BOOST_CHECK_EQUAL(cass_future_error_code(connect_future.get()), CASS_OK);
    BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 1);

    // Sleep to allow the connection pool failure on the paused node (WARN)
    test_utils::CassLog::reset("Connection pool was unable to reconnect to host");
    test_utils::CassLog::set_expected_log_level(CASS_LOG_WARN);
    boost::this_thread::sleep_for(boost::chrono::seconds(10));
    BOOST_CHECK(test_utils::CassLog::message_count() > 0);
  }
  ccm->resume_node(1);
}

BOOST_AUTO_TEST_SUITE_END()
