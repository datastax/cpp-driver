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
#include "cluster.hpp"
#include "test_utils.hpp"

#include <uv.h>

#include <boost/scoped_ptr.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

struct TestPool : public test_utils::MultipleNodesTest {
  TestPool()
      : MultipleNodesTest(1, 0) {}

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
};

BOOST_AUTO_TEST_SUITE_WITH_DECOR(pool, *boost::unit_test::disabled())

BOOST_AUTO_TEST_CASE(connection_spawn) {
  TestPool tester;
  const std::string SPAWN_MSG =
      "Spawning new connection to host " + tester.ccm->get_ip_prefix() + "1";
  test_utils::CassLog::reset(SPAWN_MSG);

  test_utils::MultipleNodesTest inst(1, 0);
  cass_cluster_set_num_threads_io(tester.cluster, 1);
  cass_cluster_set_core_connections_per_host(tester.cluster, 1);
  cass_cluster_set_max_connections_per_host(tester.cluster, 2);
  cass_cluster_set_max_concurrent_requests_threshold(tester.cluster,
                                                     1); // start next connection soon

  // only one with no traffic
  { test_utils::CassSessionPtr session(test_utils::create_session(tester.cluster)); }
  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 1u);

  test_utils::CassLog::reset(SPAWN_MSG);
  // exactly two with traffic
  {
    test_utils::CassSessionPtr session(test_utils::create_session(tester.cluster));

    test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.local", 0));

    // run a few to get concurrent requests
    std::vector<test_utils::CassFuturePtr> futures;
    for (size_t i = 0; i < 10; ++i) {
      futures.push_back(
          test_utils::CassFuturePtr(cass_session_execute(session.get(), statement.get())));
    }
  }
  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 2u);
}

/**
 * Data for performing the connection interruption
 */
struct ConnectionInterruptionData {
  CCM::Bridge* ccm;
  int node;
  int duration;
  int delay;
};

/**
 * Create connection interruptions using CCM
 *
 * @param data Connection interruption data structure
 */
static void connection_interruptions(void* data) {
  boost::posix_time::ptime start = boost::posix_time::second_clock::universal_time();
  ConnectionInterruptionData* ci_data = static_cast<ConnectionInterruptionData*>(data);
  while ((boost::posix_time::second_clock::universal_time() - start).total_seconds() <
         ci_data->duration) {
    ci_data->ccm->pause_node(ci_data->node);
    boost::this_thread::sleep(boost::posix_time::seconds(ci_data->delay));
    ci_data->ccm->resume_node(ci_data->node);
  }
}

/**
 * Don't Recycle Pool On Connection Timeout
 *
 * This test ensures that a pool does not completely remove itself while
 * allowing partial connections to remain and reconnection attempts to use the
 * existing pool.
 *
 * @since 2.1.0
 * @test_category connection:connection_pool
 * @jira_ticket CPP-253 [https://datastax-oss.atlassian.net/browse/CPP-253]
 */
BOOST_AUTO_TEST_CASE(dont_recycle_pool_on_timeout) {
  // Limit backpressure test to lower version of C* (difficult to produce in later versions
  // deterministically)
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version <= 2 && version.minor_version < 1) || version.major_version < 2) {
    TestPool tester;

    // Add a second node
    tester.ccm->bootstrap_node();

    // Create the connection interruption data
    ConnectionInterruptionData ci_data = { tester.ccm.get(), 2, 0, 0 };

    std::string ip_prefix = tester.ccm->get_ip_prefix();
    test_utils::initialize_contact_points(tester.cluster, ip_prefix, 2);
    cass_cluster_set_connect_timeout(tester.cluster, 100);
    cass_cluster_set_num_threads_io(tester.cluster, 32);
    cass_cluster_set_core_connections_per_host(tester.cluster, 4);
    cass_cluster_set_load_balance_round_robin(tester.cluster);

    // Create session during "connection interruptions"
    test_utils::CassLog::reset("Host " + ip_prefix +
                               "2 already present attempting to initiate immediate connection");
    {
      uv_thread_t connection_interruptions_thread;
      ci_data.duration = 5;
      uv_thread_create(&connection_interruptions_thread, connection_interruptions, &ci_data);
      test_utils::CassSessionPtr session(test_utils::create_session(tester.cluster));
      uv_thread_join(&connection_interruptions_thread);
      tester.execute_system_query(60, session);
    }
    BOOST_CHECK_GE(test_utils::CassLog::message_count(), 1);

    // Handle partial reconnects
    cass_cluster_set_connect_timeout(tester.cluster, 5 * test_utils::ONE_SECOND_IN_MILLISECONDS);
    cass_cluster_set_connection_idle_timeout(tester.cluster, 1);
    cass_cluster_set_connection_heartbeat_interval(tester.cluster, 2);
    test_utils::CassLog::reset("already present attempting to initiate immediate connection");
    {
      // Create the session ignore all connection errors
      test_utils::CassSessionPtr session(cass_session_new());
      test_utils::CassFuturePtr future(cass_session_connect(session.get(), tester.cluster));
      cass_future_wait_timed(future.get(), test_utils::ONE_SECOND_IN_MICROS);
      uv_thread_t connection_interruptions_thread;
      ci_data.delay = 5;
      ci_data.duration = 45;
      uv_thread_create(&connection_interruptions_thread, connection_interruptions, &ci_data);
      tester.execute_system_query(60, session);
      uv_thread_join(&connection_interruptions_thread);
    }
    BOOST_CHECK_GE(test_utils::CassLog::message_count(), 1);

    // Destroy the current cluster (node added)
    tester.ccm->remove_cluster();
  } else {
    std::cout << "Difficult to Produce Don't Recycle Pool on Timeout for Cassandra v"
              << version.to_string()
              << ": Skipping pool/dont_recycle_pool_on_timeout (use C* 1.x - 2.0.x)" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
