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

#include "testing.hpp"

#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>
#include <boost/lexical_cast.hpp>

#include <set>
#include <sstream>
#include <stdarg.h>

struct ControlConnectionTests {
  ControlConnectionTests() {}

  void check_for_live_hosts(test_utils::CassSessionPtr session,
                            const std::set<std::string>& should_be_present) {
    std::set<std::string> hosts;

    for (size_t i = 0; i < should_be_present.size() + 1; ++i) {
      const char* query = "SELECT * FROM system.schema_keyspaces";
      test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
      test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
      if (cass_future_error_code(future.get()) ==  CASS_OK) {
        hosts.insert(cass::get_host_from_future(future.get()));
      } else {
        CassString message;
        cass_future_error_message(future.get(), &message.data, &message.length);
        BOOST_MESSAGE("Failed to query host: " << std::string(message.data, message.length));
      }
    }

    BOOST_CHECK(hosts.size() == should_be_present.size());
    for (std::set<std::string>::const_iterator it = should_be_present.begin();
         it != should_be_present.end(); ++it) {
      BOOST_CHECK(hosts.count(*it) > 0);
    }
  }

  std::set<std::string> build_single_ip(const std::string& ip_prefix, int n) {
    std::set<std::string> range;
    range.insert(ip_prefix + boost::lexical_cast<std::string>(n));
    return range;
  }

  std::set<std::string> build_ip_range(const std::string& ip_prefix, int start, int end) {
    std::set<std::string> range;
    for (int i = start; i <= end; ++i) {
      range.insert(ip_prefix + boost::lexical_cast<std::string>(i));
    }
    return range;
  }
};

BOOST_FIXTURE_TEST_SUITE(control_connection, ControlConnectionTests)

BOOST_AUTO_TEST_CASE(connect_invalid_ip)
{
  test_utils::CassLog::reset("Host 1.1.1.1 had the following error on startup: 'Connection timeout'");

  test_utils::CassClusterPtr cluster(cass_cluster_new());
  cass_cluster_set_contact_points(cluster.get(), "1.1.1.1");
  {
    CassError code;
    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get(), &code));
    BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
  }

  BOOST_CHECK(test_utils::CassLog::message_count() > 0);
}

BOOST_AUTO_TEST_CASE(connect_invalid_port)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 1);

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  cass_cluster_set_port(cluster.get(), 9999); // Invalid port

  CassError code;
  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get(), &code));
  BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_CASE(reconnection)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 2);

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  // Stop the node of the current control connection
  ccm->stop(1);

  // Add a new node to make sure the node gets added on the new control connection to node 2
  ccm->add_node(3);
   // Allow this node to come up without node1
  ccm->start(3, "-Dcassandra.consistent.rangemovement=false");
  boost::this_thread::sleep_for(boost::chrono::seconds(10));

  // Stop the other node
  ccm->stop(2);

  check_for_live_hosts(session, build_single_ip(conf.ip_prefix(), 3));
}

BOOST_AUTO_TEST_CASE(topology_change)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 1);

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  // Adding a new node will trigger a "NEW_NODE" event
  ccm->bootstrap(2);
  boost::this_thread::sleep_for(boost::chrono::seconds(10));

  std::set<std::string> should_be_present = build_ip_range(conf.ip_prefix(), 1, 2);
  check_for_live_hosts(session, should_be_present);

  // Decommissioning a node will trigger a "REMOVED_NODE" event
  ccm->decommission(2);

  should_be_present.erase(conf.ip_prefix() + "2");
  check_for_live_hosts(session, should_be_present);
}

BOOST_AUTO_TEST_CASE(status_change)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 2);

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  std::set<std::string> should_be_present = build_ip_range(conf.ip_prefix(), 1, 2);
  check_for_live_hosts(session, should_be_present);

  // Stopping a node will trigger a "DOWN" event
  ccm->stop(2);

  should_be_present.erase(conf.ip_prefix() + "2");
  check_for_live_hosts(session, should_be_present);

  // Starting a node will trigger an "UP" event
  ccm->start(2);
  boost::this_thread::sleep_for(boost::chrono::seconds(10));

  should_be_present.insert(conf.ip_prefix() + "2");
  check_for_live_hosts(session, should_be_present);
}

BOOST_AUTO_TEST_CASE(node_discovery)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 3);

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  // Only add a single IP
  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  // Allow the nodes to be discovered
  boost::this_thread::sleep_for(boost::chrono::seconds(20)); //TODO: Remove sleep and implement a pre-check

  check_for_live_hosts(session, build_ip_range(conf.ip_prefix(), 1, 3));
}

BOOST_AUTO_TEST_CASE(node_discovery_invalid_ips)
{
  test_utils::CassLog::reset("Unable to reach contact point 192.0.2.");

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());


    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 3);

    // Ensure RR policy
    cass_cluster_set_load_balance_round_robin(cluster.get());;

    // Add invalid IPs first (http://tools.ietf.org/html/rfc5737)
    cass_cluster_set_contact_points(cluster.get(), "192.0.2.0,192.0.2.1,192.0.2.3");

    // Only add a single valid IP
    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

    // Make sure the timout is very high for the initial invalid IPs
    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get(), NULL, 60 * test_utils::ONE_SECOND_IN_MICROS));

    // Allow the nodes to be discovered
    boost::this_thread::sleep_for(boost::chrono::seconds(20)); //TODO: Remove sleep and implement a pre-check

    check_for_live_hosts(session, build_ip_range(conf.ip_prefix(), 1, 3));
  }

  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 3ul);
}

BOOST_AUTO_TEST_CASE(node_discovery_no_local_rows)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 3);

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  // Only add a single valid IP
  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
    test_utils::execute_query(session.get(), "DELETE FROM system.local WHERE key = 'local'");
  }

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  // Allow the nodes to be discovered
  boost::this_thread::sleep_for(boost::chrono::seconds(10)); //TODO: Remove sleep and implement a pre-check

  check_for_live_hosts(session, build_ip_range(conf.ip_prefix(), 1, 3));
}

BOOST_AUTO_TEST_CASE(node_discovery_no_rpc_addresss)
{
  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  test_utils::CassLog::reset("No rpc_address for host " + conf.ip_prefix() + "3 in system.peers on " + conf.ip_prefix() + "1. Ignoring this entry.");

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 3);

    // Ensure RR policy
    cass_cluster_set_load_balance_round_robin(cluster.get());;

    // Only add a single valid IP
    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

    // Make the 'rpc_address' null on all applicable hosts (1 and 2)
    {
      test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

      for (int i = 0; i < 3; ++i) {
        std::ostringstream ss;
        ss << "UPDATE system.peers SET rpc_address = null WHERE peer = '" << conf.ip_prefix() << "3'";
        test_utils::execute_query(session.get(), ss.str());
      }
    }

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

    // This should only contain 2 address because one peer is ignored
    check_for_live_hosts(session, build_ip_range(conf.ip_prefix(), 1, 2));
  }

  BOOST_CHECK(test_utils::CassLog::message_count() > 0);
}

BOOST_AUTO_TEST_CASE(full_outage)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const char* query = "SELECT * FROM system.local";

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 1);

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);
  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
  test_utils::execute_query(session.get(), query);

  ccm->stop();
  BOOST_CHECK(test_utils::execute_query_with_error(session.get(), query) == CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);

  ccm->start();

  boost::this_thread::sleep_for(boost::chrono::seconds(10));

  test_utils::execute_query(session.get(), query);
}

/**
 * Node Decommission
 *
 * This test ensures the driver will not attempt reconnects after a node has
 * been decommissioned from a cluster
 *
 * @since 1.0.1
 * @jira_ticket CPP-210
 * @test_category control_connection
 */
BOOST_AUTO_TEST_CASE(node_decommission)
{
  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  test_utils::CassLog::reset("Adding pool for host " + conf.ip_prefix());

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 2);

    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 2, 0);
    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

    // Wait for all hosts to be added to the pool; timeout after 10 seconds
    boost::chrono::steady_clock::time_point end = boost::chrono::steady_clock::now() + boost::chrono::milliseconds(10000);
    while (test_utils::CassLog::message_count() != 2 && boost::chrono::steady_clock::now() < end) {
      boost::this_thread::sleep_for(boost::chrono::seconds(1));
    }
    BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 2ul);

    test_utils::CassLog::reset("Spawning new connection to host " + conf.ip_prefix() + "1");
    ccm->decommission(1);
    BOOST_TEST_MESSAGE("Node Decommissioned [" << conf.ip_prefix() << "1]: Sleeping for 30 seconds");
    boost::this_thread::sleep_for(boost::chrono::seconds(30));
  }
  
  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 0ul);
}

BOOST_AUTO_TEST_SUITE_END()
