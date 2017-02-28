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

#include <algorithm>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>
#include <boost/atomic.hpp>
#include <boost/scoped_ptr.hpp>

#include <uv.h>

#include "cassandra.h"
#include "test_utils.hpp"

#define SESSION_STRESS_OPENED_LOG_MESSAGE "Session is connected"
#define SESSION_STRESS_CLOSED_LOG_MESSAGE "Session is disconnected"
#define SESSION_STRESS_NUMBER_OF_SESSIONS 16 //NOTE: Keep low due to CPP-194
#define SESSION_STRESS_NUMBER_OF_SHARED_SESSION_THREADS 8 //NOTE: Total threads will be (SESSION_STRESS_NUMBER_OF_SESSIONS / 4) * SESSION_STRESS_NUMBER_OF_SHARED_SESSION_THREADS
#define SESSION_STRESS_CHAOS_NUMBER_OF_ITERATIONS 256
#define SESSION_STRESS_NUMBER_OF_ITERATIONS 4 //NOTE: This effects sleep timer as well for async log messages
#define SESSION_STRESS_NUMBER_OF_ALLOWED_NO_HOST_AVAILABLE_OCCURRENCES 2

struct SessionTests {
public:
  boost::shared_ptr<CCM::Bridge> ccm;

  SessionTests()
    : ccm(new CCM::Bridge("config.txt")) {}
};

BOOST_FIXTURE_TEST_SUITE(sessions, SessionTests)

BOOST_AUTO_TEST_CASE(connect_invalid_name)
{
  test_utils::CassLog::reset("Unable to resolve address for node.domain-does-not-exist.dne");

  CassError code;

  // Note: This test might not work if your DNS provider forwards unresolved DNS requests
  // to a results page.

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    cass_cluster_set_contact_points(cluster.get(), "node.domain-does-not-exist.dne");

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get(), &code));
  }

  BOOST_CHECK(test_utils::CassLog::message_count() > 0);
  BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_CASE(connect_invalid_keyspace)
{
  test_utils::CassLog::reset("Received error response 'Keyspace 'invalid' does not exist");

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    if (ccm->create_cluster()) {
      ccm->start_cluster();
    }

    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);

    test_utils::CassSessionPtr session(cass_session_new());
    test_utils::CassFuturePtr connect_future(cass_session_connect_keyspace(session.get(), cluster.get(), "invalid"));
    CassError code = cass_future_error_code(connect_future.get());
    BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE);
  }

  BOOST_CHECK(test_utils::CassLog::message_count() > 0);
}

BOOST_AUTO_TEST_CASE(close_timeout_error)
{
  test_utils::CassLog::reset("Timed out during startup"); // JIRA CPP-127

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    if (ccm->create_cluster()) {
      ccm->start_cluster();
    }

    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);

    // Create new connections after 1 pending request
    cass_cluster_set_max_concurrent_requests_threshold(cluster.get(), 1);
    cass_cluster_set_max_connections_per_host(cluster.get(), 10);

    for (int i = 0; i < 100; ++i) {
      test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

      for (int j = 0; j < 10; ++j) {
        const char* query = "SELECT * FROM system.schema_keyspaces";
        test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
        cass_future_free(cass_session_execute(session.get(), statement.get()));
      }
    }
  }

  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 0ul);
}

/**
 * Connect when already connected
 *
 * @since 1.0.0-rc1
 * @test_category sessions
 */
BOOST_AUTO_TEST_CASE(connect_when_already_connected)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());
  if (ccm->create_cluster()) {
    ccm->start_cluster();
  }

  test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);

  test_utils::CassSessionPtr session(cass_session_new());
  test_utils::CassFuturePtr connect_future1(cass_session_connect(session.get(), cluster.get()));
  test_utils::CassFuturePtr connect_future2(cass_session_connect(session.get(), cluster.get()));

  test_utils::wait_and_check_error(connect_future1.get());

  CassError code = test_utils::wait_and_return_error(connect_future2.get());
  BOOST_CHECK(code == CASS_ERROR_LIB_UNABLE_TO_CONNECT);
}

/**
 * Close when already closed
 *
 * @since 1.0.0-rc1
 * @test_category sessions
 */
BOOST_AUTO_TEST_CASE(close_when_already_closed)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());
  if (ccm->create_cluster()) {
    ccm->start_cluster();
  }

  test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);

  test_utils::CassSessionPtr session(cass_session_new());
  test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster.get()));
  test_utils::wait_and_check_error(connect_future.get());

  test_utils::CassFuturePtr close_future1(cass_session_close(session.get()));
  test_utils::CassFuturePtr close_future2(cass_session_close(session.get()));

  test_utils::wait_and_check_error(close_future1.get());

  CassError code = test_utils::wait_and_return_error(close_future2.get());
  BOOST_CHECK(code == CASS_ERROR_LIB_UNABLE_TO_CLOSE);
}

/**
 * Close when not connected
 *
 * @since 1.0.0-rc1
 * @test_category sessions
 */
BOOST_AUTO_TEST_CASE(close_when_not_connected)
{
  test_utils::CassSessionPtr session(cass_session_new());
  test_utils::CassFuturePtr close_future(cass_session_close(session.get()));
  CassError code = test_utils::wait_and_return_error(close_future.get());
  BOOST_CHECK(code == CASS_ERROR_LIB_UNABLE_TO_CLOSE);
}

/**
 * Adding/Bootstrapping New Nodes Before Opening Session
 *
 * This test addresses a connection timeout when the Load Balancing Policy
 * has determined a host is to be ignored.  This issue tests the commit
 * https://github.com/datastax/cpp-driver/commit/0811afc5fc292aef9bd27af4297ab82395ef248e
 *
 * @since 1.0.0-rc1
 * @test_category sessions
 */
BOOST_AUTO_TEST_CASE(add_nodes_connect) {
  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
  {
    //Create a single node cluster with three nodes initialized as contact points
    std::cout << "Create single node cluster with all three nodes initialized as contact points" << std::endl;
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    if (ccm->create_cluster()) {
      ccm->start_cluster();
    }
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 3);

    //Add two nodes
    ccm->bootstrap_node();
    ccm->bootstrap_node();

    //Create session
    test_utils::create_session(cluster.get());
    ccm->remove_cluster();
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);


  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
  {
    //Create a single node cluster with two nodes initialized as contact points
    std::cout << "Create single node cluster with two of the three nodes initialized as contact points" << std::endl;
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    if (ccm->create_cluster()) {
      ccm->start_cluster();
    }
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 2);

    //Add two nodes
    ccm->bootstrap_node();
    ccm->bootstrap_node();

    //Create session
    test_utils::create_session(cluster.get());
    ccm->remove_cluster();
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);


  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
  {
    //Create a single node cluster with one node initialized as contact points
    std::cout << "Create single node cluster with one of the three nodes initialized as contact points" << std::endl;
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    if (ccm->create_cluster()) {
      ccm->start_cluster();
    }
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);

    //Add two nodes
    ccm->bootstrap_node();
    ccm->bootstrap_node();

    //Create session
    test_utils::create_session(cluster.get());
    ccm->remove_cluster();
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);

  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
  {
    //Create a two node cluster with three nodes initialized as contact points
    std::cout << "Create two node cluster with all three of the nodes initialized as contact points" << std::endl;
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    if (ccm->create_cluster(2)) {
      ccm->start_cluster();
    }
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 3);

    //Add one nodes
    ccm->bootstrap_node();

    //Create session
    test_utils::create_session(cluster.get());
    ccm->remove_cluster();
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);

  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
  {
    //Create a two node cluster with two nodes initialized as contact points
    std::cout << "Create two node cluster with two of the three nodes initialized as contact points" << std::endl;
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    if (ccm->create_cluster(2)) {
      ccm->start_cluster();
    }
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 2);

    //Add one nodes
    ccm->bootstrap_node();

    //Create session
    test_utils::create_session(cluster.get());
    ccm->remove_cluster();
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);
}

/**
 * Container for creating and storing sessions
 */
struct SessionContainer {
  /**
   * Mutex for sessions array
   */
  mutable uv_rwlock_t sessions_lock;

  /**
   * Sessions array
   */
  std::vector<test_utils::CassSessionPtr> sessions;

  /**
   * Cluster instance
   */
  const CassCluster* cluster;

  SessionContainer(const CassCluster* cluster)
    : cluster(cluster) {
    BOOST_REQUIRE(uv_rwlock_init(&sessions_lock) == 0);
  }

  ~SessionContainer() {
    uv_rwlock_destroy(&sessions_lock);
  }

  /**
   * Add a session to the container
   *
   * @param session Session to add
   */
  void add_session(const test_utils::CassSessionPtr& session) {
    uv_rwlock_wrlock(&sessions_lock);
    sessions.push_back(session);
    uv_rwlock_wrunlock(&sessions_lock);
  }

  /**
   * Get the number of sessions in the array
   *
   * @return Sessions in array
   */
  unsigned int count() const {
    uv_rwlock_rdlock(&sessions_lock);
    int size = sessions.size();
    uv_rwlock_rdunlock(&sessions_lock);
    return size;
  }
};

/**
* Open a session and add it to the list of sessions opened
*
* @param arg Session container to add session
*/
static void open_session(void* arg) {
  SessionContainer* sessions = static_cast<SessionContainer*>(arg);
  test_utils::CassSessionPtr session(cass_session_new());
  test_utils::CassFuturePtr session_future(cass_session_connect(session.get(), sessions->cluster));
  test_utils::wait_and_check_error(session_future.get(), 20 * test_utils::ONE_SECOND_IN_MICROS);
  sessions->add_session(session);
}

/**
 * Open a number of sessions concurrently or sequentially
 *
 * @param cluster Cluster to use in order to create sessions
 * @param sessions Session container for creating sessions
 * @param num_of_sessions Number of sessions to create concurrently
 * @param is_concurrently True if concurrent; false otherwise
 */
void open_sessions(SessionContainer* sessions, unsigned int num_of_sessions, bool is_concurrently = true) {
  //Create session threads
  std::vector<uv_thread_t> threads(num_of_sessions);
  for (unsigned int n = 0; n < num_of_sessions; ++n) {
    if (is_concurrently) {
      uv_thread_create(&threads[n], open_session, sessions);
    } else {
      open_session(sessions);
    }
  }

  //Ensure all threads have completed
  if (is_concurrently) {
    for (unsigned int n = 0; n < num_of_sessions; ++n) {
      uv_thread_join(&threads[n]);
    }
  }

  //TODO: Remove sleep and create a timed wait for log messages (no boost)
  for (unsigned int n = 0; n < (SESSION_STRESS_NUMBER_OF_ITERATIONS * 20); ++n) {
    if (static_cast<unsigned int>(test_utils::CassLog::message_count()) != num_of_sessions) {
      boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
    } else {
      break;
    }
  }
}

/**
 * Close a session
 *
 * @param session Session to close
 */
static void close_session(void* session) {
  test_utils::CassFuturePtr close_future(cass_session_close(static_cast<CassSession*>(session)));
  test_utils::wait_and_check_error(close_future.get(), 20 * test_utils::ONE_SECOND_IN_MICROS);
}

/**
 * Close a number of sessions concurrently or sequentially
 *
 * @param sessions Session container for closing sessions
 * @param num_of_sessions Number of sessions to close concurrently
 * @param is_concurrently True if concurrent; false otherwise
 */
void close_sessions(SessionContainer* sessions, unsigned int num_of_sessions, bool is_concurrently = true) {
  //Close session threads (LIFO)
  std::vector<uv_thread_t> threads(num_of_sessions);
  for (unsigned int n = 0; n < num_of_sessions; ++n) {
    if (is_concurrently) {
      uv_thread_create(&threads[n], close_session, sessions->sessions[sessions->count() - n - 1].get());
    } else {
      close_session(sessions->sessions[sessions->count() - n - 1].get());
    }
  }

  //Ensure all threads have completed
  if (is_concurrently) {
    for (unsigned int n = 0; n < num_of_sessions; ++n) {
      uv_thread_join(&threads[n]);
    }
  }

  sessions->sessions.clear();

  //TODO: Remove sleep and create a timed wait for log messages (no boost)
  for (unsigned int n = 0; n < (SESSION_STRESS_NUMBER_OF_ITERATIONS * 20); ++n) {
    if (static_cast<unsigned int>(test_utils::CassLog::message_count()) != num_of_sessions) {
      boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
    } else {
      break;
    }
  }
}

struct QuerySession {
  QuerySession(CassSession* session = NULL)
    : session(session)
    , error_code(CASS_OK) {}
  CassSession* session;
  CassError error_code;
};

/**
 * Run a query against the session
 *
 * @param arg An instance of QuerySession
 */
static void query_session(void* arg) {
  QuerySession* query = static_cast<QuerySession*>(arg);
  query->error_code = test_utils::execute_query_with_error(query->session,
                                                           test_utils::SELECT_VERSION,
                                                           NULL,
                                                           CASS_CONSISTENCY_ONE,
                                                           20 * test_utils::ONE_SECOND_IN_MICROS);
}

/**
 * Perform query operations using each session in multiple threads
 *
 * @param sessions Session container for closing sessions
 */
void query_sessions(const SessionContainer& sessions) {
  //Query session in multiple threads
  unsigned int thread_count = sessions.count() * SESSION_STRESS_NUMBER_OF_SHARED_SESSION_THREADS;

  std::vector<uv_thread_t> threads(thread_count);
  std::vector<QuerySession> queries;

  for (unsigned int iterations = 0; iterations < SESSION_STRESS_NUMBER_OF_SHARED_SESSION_THREADS; ++iterations) {
    for (unsigned int n = 0; n < sessions.count(); ++n) {
      int thread_index = (sessions.count() * iterations) + n;
      QuerySession query = QuerySession(sessions.sessions[n].get());
      queries.push_back(query);
      uv_thread_create(&threads[thread_index], query_session, &query);
    }
  }

  //Ensure all threads have completed
  for (unsigned int n = 0; n < thread_count; ++n) {
    uv_thread_join(&threads[n]);
  }

  int no_host_count = 0;
  for (unsigned int n = 0; n < thread_count; ++n) {
    //Timeouts are OK (especially on the minor chaos test)
    CassError error_code = queries[n].error_code;
    if (error_code != CASS_OK && error_code != CASS_ERROR_LIB_REQUEST_TIMED_OUT) {
      if (error_code == CASS_ERROR_LIB_NO_HOSTS_AVAILABLE) {
        ++no_host_count;
      } else {
        BOOST_FAIL("Error occurred during query '" << std::string(cass_error_desc(error_code)) << "' [" << error_code << "]");
      }
    }
  }

  // Ensure that some hosts were available (chaos)
  if (no_host_count > SESSION_STRESS_NUMBER_OF_ALLOWED_NO_HOST_AVAILABLE_OCCURRENCES) {
    BOOST_FAIL("Unacceptable Limit of CASS_ERROR_LIB_NO_HOSTS_AVAILABLE Occurred: " << no_host_count << " > " << SESSION_STRESS_NUMBER_OF_ALLOWED_NO_HOST_AVAILABLE_OCCURRENCES);
  }
}

/**
 * Create some minor chaos using CCM
 *
 * @param ccm CCM bridge pointer
 */
static void minor_chaos(void *ccm) {
  CCM::Bridge* ccm_ptr = static_cast<CCM::Bridge*>(ccm);
  ccm_ptr->kill_node(1);
  ccm_ptr->decommission_node(2);
  ccm_ptr->start_node(1);
  ccm_ptr->disable_node_gossip(3);
}

/**
 * Session Stress Test [Opening and Closing Session]
 *
 * This test opens and closes {@code CassSession} in a multithreaded environment
 * to ensure stress on the driver does not result in deadlock or memory issues.
 *
 * <ul>
 *   <li>Open and close multiple session SESSION_STRESS_NUMBER_OF_ITERATIONS times</li>
 *   <ol>
 *     <li>Open SESSION_STRESS_NUMBER_OF_SESSIONS {@code Session} concurrently</li>
 *     <li>Verify that SESSION_STRESS_NUMBER_OF_SESSIONS sessions are reported as open by the {@code Session}</li>
 *     <li>Close SESSION_STRESS_NUMBER_OF_SESSIONS {@code Session} concurrently</li>
 *     <li>Verify that SESSION_STRESS_NUMBER_OF_SESSIONS sessions are reported as close by the {@code Session}</li>
 *   </ol>
 *   <li>Open multiple sessions [SESSION_STRESS_NUMBER_OF_SESSIONS / 4] and perform query operations sharing sessions on multiple threads</li>
 *   <li>Open multiple sessions [SESSION_STRESS_NUMBER_OF_SESSIONS / 4] and perform query operations sharing sessions on multiple threads with minor chaos</li>
 *   <ol>
 *     <li>Kill one node</li>
 *     <li>Disable gossip on another node</li>
 *     <li>Restart killed node</li>
 *     <li>Decommission third node</li>
 *   </ol>
 * </ul>
 *
 * @since 1.0.0-rc1
 * @test_category sessions:stress
 * @jira_ticket CPP-157 [https://datastax-oss.atlassian.net/browse/CPP-157]
 */
BOOST_AUTO_TEST_CASE(stress)
{
  //Initialize the logger with the opened log message
  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);

  {
    //Create a single node cluster
    test_utils::CassLog::set_output_log_level(CASS_LOG_DISABLED); //Quiet the logger output
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    if (ccm->create_cluster()) {
      ccm->start_cluster();
    }
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 1);

    //Open and close sessions sequentially
    SessionContainer sessions(cluster.get());
    std::cout << "Sequential" << std::endl;
    for (unsigned int iterations = 0; iterations < SESSION_STRESS_NUMBER_OF_ITERATIONS; ++iterations) {
      open_sessions(&sessions, SESSION_STRESS_NUMBER_OF_SESSIONS, false);
      BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), SESSION_STRESS_NUMBER_OF_SESSIONS);
      BOOST_CHECK_EQUAL(sessions.count(), SESSION_STRESS_NUMBER_OF_SESSIONS);

      test_utils::CassLog::reset(SESSION_STRESS_CLOSED_LOG_MESSAGE);
      close_sessions(&sessions, SESSION_STRESS_NUMBER_OF_SESSIONS, false);
      BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), SESSION_STRESS_NUMBER_OF_SESSIONS);
      BOOST_CHECK_EQUAL(sessions.count(), 0);
    }

    //Open and close sessions concurrently in sequence
    std::cout << "Concurrently in Sequence" << std::endl;
    for (unsigned int iterations = 0; iterations < SESSION_STRESS_NUMBER_OF_ITERATIONS; ++iterations) {
      test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
      open_sessions(&sessions, SESSION_STRESS_NUMBER_OF_SESSIONS);
      BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), SESSION_STRESS_NUMBER_OF_SESSIONS);
      BOOST_CHECK_EQUAL(sessions.count(), SESSION_STRESS_NUMBER_OF_SESSIONS);

      test_utils::CassLog::reset(SESSION_STRESS_CLOSED_LOG_MESSAGE);
      close_sessions(&sessions, SESSION_STRESS_NUMBER_OF_SESSIONS);
      BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), SESSION_STRESS_NUMBER_OF_SESSIONS);
      BOOST_CHECK_EQUAL(sessions.count(), 0);
    }

    //Perform query operations between threads using sessions (1/4 sessions)
    std::cout << "Query sessions across multiple threads" << std::endl;
    unsigned int quarter_sessions = SESSION_STRESS_NUMBER_OF_SESSIONS / 4;
    for (unsigned int n = 0; n < quarter_sessions; ++n) {
      test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
      open_sessions(&sessions, quarter_sessions, false);
      BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), quarter_sessions);
      BOOST_CHECK_EQUAL(sessions.count(), quarter_sessions);

      //Query sessions over multiple threads
      query_sessions(sessions);

      test_utils::CassLog::reset(SESSION_STRESS_CLOSED_LOG_MESSAGE);
      close_sessions(&sessions, quarter_sessions, false);
      BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), quarter_sessions);
      BOOST_CHECK_EQUAL(sessions.count(), 0);
    }

    //Perform query operations between threads using sessions; with chaos
    if (ccm->create_cluster(3)) {
      ccm->start_cluster();
    }
    test_utils::initialize_contact_points(cluster.get(), ccm->get_ip_prefix(), 3);

    //Create sessions
    test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
    cass_cluster_set_num_threads_io(cluster.get(), 2);
    open_sessions(&sessions, (SESSION_STRESS_NUMBER_OF_SESSIONS / 4), false);
    BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), (SESSION_STRESS_NUMBER_OF_SESSIONS / 4));
    BOOST_CHECK_EQUAL(sessions.count(), (SESSION_STRESS_NUMBER_OF_SESSIONS / 4));

    //Query sessions over multiple threads
    uv_thread_t chaos_thread;
    uv_thread_create(&chaos_thread, minor_chaos, ccm.get());
    //Do many of these so minor chaos can complete
    for (unsigned int queries = 0; queries < SESSION_STRESS_CHAOS_NUMBER_OF_ITERATIONS; ++queries) {
      query_sessions(sessions);
    }
    uv_thread_join(&chaos_thread);

    //Close sessions
    test_utils::CassLog::reset(SESSION_STRESS_CLOSED_LOG_MESSAGE);
    close_sessions(&sessions, (SESSION_STRESS_NUMBER_OF_SESSIONS / 4), false);
    BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), (SESSION_STRESS_NUMBER_OF_SESSIONS / 4));
    BOOST_CHECK_EQUAL(sessions.count(), 0);
    ccm->remove_cluster();
  }
}

BOOST_AUTO_TEST_SUITE_END()
