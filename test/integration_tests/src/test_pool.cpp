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

#include "cassandra.h"
#include "test_utils.hpp"
#include "cluster.hpp"

#include <uv.h>

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/test/unit_test.hpp>

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

BOOST_FIXTURE_TEST_SUITE(pool, TestPool)

BOOST_AUTO_TEST_CASE(no_hosts_backpressure)
{
  cass_cluster_set_num_threads_io(cluster, 1);
  reinterpret_cast<cass::Cluster*>(cluster)->config().set_core_connections_per_host(0);// bypassing API param check

  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.local", 0));

    // reject should come immediately
    boost::chrono::steady_clock::time_point start = boost::chrono::steady_clock::now();
    test_utils::CassFuturePtr future_reject(cass_session_execute(session.get(), statement.get()));
    CassError code_reject = test_utils::wait_and_return_error(future_reject.get());
    boost::chrono::steady_clock::time_point reject_time = boost::chrono::steady_clock::now();
    boost::chrono::milliseconds reject_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(reject_time - start);

    BOOST_CHECK_LT(reject_ms.count(), 1);
    BOOST_CHECK_EQUAL(code_reject, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
  }

  {
    // one connection allowed
    cass_cluster_set_num_threads_io(cluster, 1);
    cass_cluster_set_core_connections_per_host(cluster, 1);
    cass_cluster_set_max_connections_per_host(cluster, 1);

    // becomes unwritable after two pending
    const size_t pending_low_wm = 1;
    const size_t pending_high_wm = 2;
    cass_cluster_set_pending_requests_low_water_mark(cluster, pending_low_wm);
    cass_cluster_set_pending_requests_high_water_mark(cluster, pending_high_wm);

    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.local", 0));

    // blow through streams until we get rejected
    const size_t max_streams = 128;
    size_t tries = 0;
    size_t max_tries = 2*max_streams; // v[12] stream has 128 ids
    std::vector<test_utils::CassFuturePtr> futures;
    for (; tries < max_tries; ++tries) {
      futures.push_back(test_utils::CassFuturePtr(cass_session_execute(session.get(), statement.get())));
      if (cass_future_wait_timed(futures.back().get(), 1)) {
        BOOST_REQUIRE_EQUAL(cass_future_error_code(futures.back().get()), CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
        break;
      }
    }

    BOOST_REQUIRE_GE(tries, max_streams + pending_high_wm + 1);
    BOOST_REQUIRE_LT(tries, max_tries);
    // wait for window to advance past low water mark
    test_utils::wait_and_check_error(futures[pending_low_wm + (tries - max_streams)].get());
    // now, should be writable again
    test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
    test_utils::wait_and_check_error(future.get());
  }
}

BOOST_AUTO_TEST_CASE(connection_spawn)
{
  const std::string SPAWN_MSG = "Spawning new connection to host " + ccm->get_ip_prefix() + "1:9042";
  test_utils::CassLog::reset(SPAWN_MSG);

  test_utils::MultipleNodesTest inst(1, 0);
  cass_cluster_set_num_threads_io(cluster, 1);
  cass_cluster_set_core_connections_per_host(cluster, 1);
  cass_cluster_set_max_connections_per_host(cluster, 2);
  cass_cluster_set_max_concurrent_requests_threshold(cluster, 1);// start next connection soon

  // only one with no traffic
  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));
  }
  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 1u);


  test_utils::CassLog::reset(SPAWN_MSG);
  // exactly two with traffic
  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.local", 0));

    // run a few to get concurrent requests
    std::vector<test_utils::CassFuturePtr> futures;
    for (size_t i = 0; i < 10; ++i) {
      futures.push_back(test_utils::CassFuturePtr(cass_session_execute(session.get(), statement.get())));
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
static void connection_interruptions(void *data) {
  boost::posix_time::ptime start = boost::posix_time::second_clock::universal_time();
  ConnectionInterruptionData* ci_data = static_cast<ConnectionInterruptionData*>(data);
  while ((boost::posix_time::second_clock::universal_time() - start).total_seconds() < ci_data->duration) {
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
  // Add a second node
  ccm->bootstrap_node();

  // Create the connection interruption data
  ConnectionInterruptionData ci_data = { ccm.get(), 2, 0, 0 };

  std::string ip_prefix = ccm->get_ip_prefix();
  test_utils::initialize_contact_points(cluster, ip_prefix, 2, 0);
  cass_cluster_set_connect_timeout(cluster, 1000);
  cass_cluster_set_num_threads_io(cluster, 32);
  cass_cluster_set_core_connections_per_host(cluster, 4);
  cass_cluster_set_load_balance_round_robin(cluster);

  // Create session during "connection interruptions"
  test_utils::CassLog::reset("Host " + ip_prefix + "2 already present attempting to initiate immediate connection");
  {
    uv_thread_t connection_interruptions_thread;
    ci_data.duration = 5;
    uv_thread_create(&connection_interruptions_thread, connection_interruptions, &ci_data);
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));
    uv_thread_join(&connection_interruptions_thread);
    execute_system_query(60, session);
  }
  BOOST_CHECK_GE(test_utils::CassLog::message_count(), 1);

  // Handle partial reconnects
  cass_cluster_set_connection_idle_timeout(cluster, 1);
  cass_cluster_set_connection_heartbeat_interval(cluster, 2);
  test_utils::CassLog::reset("already present attempting to initiate immediate connection");
  {
    // Create the session ignore all connection errors
    test_utils::CassSessionPtr session(cass_session_new());
    test_utils::CassFuturePtr future(cass_session_connect(session.get(), cluster));
    cass_future_wait_timed(future.get(), test_utils::ONE_SECOND_IN_MICROS);
    uv_thread_t connection_interruptions_thread;
    ci_data.delay = 5;
    ci_data.duration = 45;
    uv_thread_create(&connection_interruptions_thread, connection_interruptions, &ci_data);
    execute_system_query(60, session);
    uv_thread_join(&connection_interruptions_thread);
  }
  BOOST_CHECK_GE(test_utils::CassLog::message_count(), 1);

  // Destroy the current cluster (node added)
  ccm->remove_cluster();
}

BOOST_AUTO_TEST_SUITE_END()
