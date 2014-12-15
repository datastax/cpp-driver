/*
  Copyright (c) 2014 DataStax

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

#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

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

#include "cassandra.h"
#include "test_utils.hpp"
#include "cql_ccm_bridge.hpp"

#define SESSION_STRESS_OPENED_LOG_MESSAGE "Session is connected"

struct SessionTests {
  SessionTests() {}
};

BOOST_FIXTURE_TEST_SUITE(sessions, SessionTests)

BOOST_AUTO_TEST_CASE(connect_invalid_name)
{
  test_utils::CassLog::reset("Unable to resolve host node.domain-does-not-exist.dne:9042");

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
  test_utils::CassLog::reset("Error response: 'Keyspace 'invalid' does not exist");

  CassError code;

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 1);

    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

    test_utils::CassSessionPtr session(cass_session_new());
    test_utils::CassFuturePtr connect_future(cass_session_connect_keyspace(session.get(), cluster.get(), "invalid"));

    CassString query = cass_string_init("SELECT * FROM table");
    test_utils::CassStatementPtr statement(cass_statement_new(query, 0));

    test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));

    code = cass_future_error_code(future.get());
  }

  BOOST_CHECK(test_utils::CassLog::message_count() > 0);
  BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_CASE(close_timeout_error)
{
  test_utils::CassLog::reset("Timed out during startup"); // JIRA CPP-127

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 1);

    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

    // Create new connections after 1 pending request
    cass_cluster_set_max_concurrent_requests_threshold(cluster.get(), 1);
    cass_cluster_set_max_connections_per_host(cluster.get(), 10);

    for (int i = 0; i < 100; ++i) {
      test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

      for (int j = 0; j < 10; ++j) {
        CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
        test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
        cass_future_free(cass_session_execute(session.get(), statement.get()));
      }
    }
  }

  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 0ul);
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
    BOOST_TEST_MESSAGE("Create single node cluster with all three nodes initialized as contact points");
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    const cql::cql_ccm_bridge_configuration_t& configuration = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(configuration, "test", 1);
    test_utils::initialize_contact_points(cluster.get(), configuration.ip_prefix(), 3, 0);
 
    //Add two nodes
    BOOST_TEST_MESSAGE("Adding two nodes");
    ccm->bootstrap(2);
    ccm->bootstrap(3);
 
    //Create session
    test_utils::create_session(cluster.get());
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);
 
 
  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
  {
    //Create a single node cluster with two nodes initialized as contact points
    BOOST_TEST_MESSAGE("Create single node cluster with two of the three nodes initialized as contact points");
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    const cql::cql_ccm_bridge_configuration_t& configuration = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(configuration, "test", 1);
    test_utils::initialize_contact_points(cluster.get(), configuration.ip_prefix(), 2, 0);
 
    //Add two nodes
    BOOST_TEST_MESSAGE("Adding two nodes");
    ccm->bootstrap(2);
    ccm->bootstrap(3);
 
    //Create session
    test_utils::create_session(cluster.get());
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);
 
 
  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
  {
    //Create a single node cluster with one node initialized as contact points
    BOOST_TEST_MESSAGE("Create single node cluster with one of the three nodes initialized as contact points");
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    const cql::cql_ccm_bridge_configuration_t& configuration = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(configuration, "test", 1);
    test_utils::initialize_contact_points(cluster.get(), configuration.ip_prefix(), 1, 0);
 
    //Add two nodes
    BOOST_TEST_MESSAGE("Adding two nodes");
    ccm->bootstrap(2);
    ccm->bootstrap(3);
 
    //Create session
    test_utils::create_session(cluster.get());
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);
 
  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
  {
    //Create a two node cluster with three nodes initialized as contact points
    BOOST_TEST_MESSAGE("Create two node cluster with all three of the nodes initialized as contact points");
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    const cql::cql_ccm_bridge_configuration_t& configuration = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(configuration, "test", 2);
    test_utils::initialize_contact_points(cluster.get(), configuration.ip_prefix(), 3, 0);
 
    //Add one nodes
    BOOST_TEST_MESSAGE("Adding one node");
    ccm->bootstrap(3);
 
    //Create session
    test_utils::create_session(cluster.get());
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);
 
  test_utils::CassLog::reset(SESSION_STRESS_OPENED_LOG_MESSAGE);
  {
    //Create a two node cluster with two nodes initialized as contact points
    BOOST_TEST_MESSAGE("Create two node cluster with two of the three nodes initialized as contact points");
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    const cql::cql_ccm_bridge_configuration_t& configuration = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(configuration, "test", 2);
    test_utils::initialize_contact_points(cluster.get(), configuration.ip_prefix(), 2, 0);
 
    //Add one nodes
    BOOST_TEST_MESSAGE("Adding one node");
    ccm->bootstrap(3);
 
    //Create session
    test_utils::create_session(cluster.get());
  }
  BOOST_CHECK(test_utils::CassLog::message_count() == 1);
}

BOOST_AUTO_TEST_SUITE_END()
