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

#include "cassandra.h"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>

struct HeartbestTest : public test_utils::MultipleNodesTest {
public:
  HeartbestTest()
      : MultipleNodesTest(2, 0) {}

  /**
   * Execute a select statement against the system tables for a specified amount
   * of time.
   *
   * NOTE: Results and errors are ignored
   *
   * @param duration Duration in seconds to execute queries
   * @param session Session instance
   */
  void execute_system_query(int duration, test_utils::CassSessionPtr session) {
    boost::posix_time::ptime start = boost::posix_time::second_clock::universal_time();
    while ((boost::posix_time::second_clock::universal_time() - start).total_seconds() < duration) {
      test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.local", 0));
      cass_statement_set_consistency(statement.get(), CASS_CONSISTENCY_ONE);
      test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
      cass_future_wait_timed(future.get(), test_utils::ONE_SECOND_IN_MICROS);
    }
  }

  /**
   * Get the number of connections established by the driver.
   *
   * @param session Session instance
   * @param Total number of connections
   */
  cass_uint64_t get_total_connections(test_utils::CassSessionPtr session) {
    CassMetrics metrics;
    cass_session_get_metrics(session.get(), &metrics);
    return metrics.stats.total_connections;
  }
};

BOOST_FIXTURE_TEST_SUITE(heartbeat, HeartbestTest)

/**
 * Heartbeat Interval
 *
 * This test ensures the heartbeat interval settings when connected to a
 * cluster
 *
 * @since 2.1.0
 * @jira_ticket CPP-152
 * @test_category connection:heartbeat
 */
BOOST_AUTO_TEST_CASE(interval) {
  // Heartbeat disabled
  cass_cluster_set_connection_heartbeat_interval(cluster, 0);
  test_utils::CassLog::reset("Heartbeat completed on host " + ccm->get_ip_prefix());
  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));
    execute_system_query(5, session);
  }
  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 0);

  // Heartbeat enabled
  cass_cluster_set_connection_heartbeat_interval(cluster, 1);
  test_utils::CassLog::reset("Heartbeat completed on host " + ccm->get_ip_prefix());
  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));
    execute_system_query(2, session);
  }
  BOOST_CHECK_GE(test_utils::CassLog::message_count(), 1);

  // Failed heartbeat
  cass_uint64_t start_total_connections = 0;
  cass_uint64_t end_total_connections = 0;
  cass_cluster_set_load_balance_round_robin(cluster);
  cass_cluster_set_connection_idle_timeout(cluster, 5);
  cass_cluster_set_connection_heartbeat_interval(cluster, 1);
  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));
    start_total_connections = get_total_connections(session);
    ccm->pause_node(2);
    int duration = 0;
    do {
      execute_system_query(1, session);
      ++duration;
      end_total_connections = get_total_connections(session);
    } while (end_total_connections > (start_total_connections / 2) && duration < 60);
    ccm->resume_node(2);
  }
  BOOST_CHECK_EQUAL(end_total_connections, start_total_connections / 2);
}

/**
 * Heartbeat Idle Timeout
 *
 * This test ensures the heartbeat idle timeout interval on a connection.
 *
 * @since 2.1.0
 * @jira_ticket CPP-152
 * @test_category connection:heartbeat
 */
BOOST_AUTO_TEST_CASE(idle_timeout) {
  cass_cluster_set_connection_idle_timeout(cluster, 5);
  cass_cluster_set_connection_heartbeat_interval(cluster, 1);
  test_utils::CassLog::reset("Failed to send a heartbeat within connection idle interval.");
  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));
    ccm->pause_node(2);
    execute_system_query(10, session);
    ccm->resume_node(2);
  }
  BOOST_CHECK_GE(test_utils::CassLog::message_count(), 1);
}

BOOST_AUTO_TEST_SUITE_END()
