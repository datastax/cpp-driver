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

#include "testing.hpp"

#include "test_utils.hpp"

#include <boost/test/debug.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>
#include <boost/lexical_cast.hpp>

#include <set>
#include <sstream>
#include <stdarg.h>

struct ControlConnectionTests {
public:
  boost::shared_ptr<CCM::Bridge> ccm;
  std::string ip_prefix;
  CCM::CassVersion version;

  ControlConnectionTests()
    : ccm(new CCM::Bridge("config.txt"))
    , ip_prefix(ccm->get_ip_prefix())
    , version(test_utils::get_version()) {}

  std::string get_executing_host(test_utils::CassSessionPtr session) {
    std::stringstream query;
    query << "SELECT * FROM " << (version >= "3.0.0" ? "system_schema.keyspaces" : "system.schema_keyspaces");
    test_utils::CassStatementPtr statement(cass_statement_new(query.str().c_str(), 0));
    test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
    if (cass_future_error_code(future.get()) ==  CASS_OK) {
      return cass::get_host_from_future(future.get());
    } else {
      CassString message;
      cass_future_error_message(future.get(), &message.data, &message.length);
      std::cerr << "Failed to query host: " << std::string(message.data, message.length) << std::endl;
    }

    return "";
  }

  void  check_for_live_hosts(test_utils::CassSessionPtr session,
                            const std::set<std::string>& should_be_present) {
    std::set<std::string> hosts;

    std::stringstream query;
    query << "SELECT * FROM " << (version >= "3.0.0" ? "system_schema.keyspaces" : "system.schema_keyspaces");
    for (size_t i = 0; i < should_be_present.size() + 2; ++i) {
      std::string host = get_executing_host(session);
      if (!host.empty()) {
        hosts.insert(host);
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
  test_utils::CassLog::reset("Unable to establish a control connection to host "
    "1.1.1.1 because of the following error: Connection timeout");

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

  if (ccm->create_cluster()) {
    ccm->start_cluster();
  }

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  cass_cluster_set_port(cluster.get(), 9999); // Invalid port

  CassError code;
  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get(), &code));
  BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_CASE(reconnection)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(2)) {
    // Ensure the cluster data is cleared to eliminate bootstrapping errors
    ccm->kill_cluster();
    ccm->clear_cluster_data();
  }
  ccm->start_cluster();

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  // Stop the node of the current control connection
  ccm->stop_node(1);

  // Add a new node to make sure the node gets added on the new control connection to node 2
  int node = ccm->bootstrap_node("\"-Dcassandra.consistent.rangemovement=false -Dcassandra.ring_delay_ms=1000\""); // Allow this node to come up without node1
  test_utils::wait_for_node_connection(ip_prefix, node);

  // Stop the other node
  ccm->stop_node(2);

  check_for_live_hosts(session, build_single_ip(ip_prefix, 3));

  // Destroy the current cluster (node was added)
  ccm->remove_cluster();
}

BOOST_AUTO_TEST_CASE(topology_change)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster()) {
    ccm->start_cluster();
  }

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  // Adding a new node will trigger a "NEW_NODE" event
  int node = ccm->bootstrap_node();
  test_utils::wait_for_node_connection(ip_prefix, node);

  std::set<std::string> should_be_present = build_ip_range(ip_prefix, 1, 2);
  check_for_live_hosts(session, should_be_present);

  // Decommissioning a node will trigger a "REMOVED_NODE" event
  ccm->force_decommission_node(2);

  should_be_present.erase(ip_prefix + "2");
  check_for_live_hosts(session, should_be_present);

  // Destroy the current cluster (decommissioned node)
  ccm->remove_cluster();
}

BOOST_AUTO_TEST_CASE(status_change)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(2)) {
    ccm->start_cluster();
  }

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  std::set<std::string> should_be_present = build_ip_range(ip_prefix, 1, 2);
  check_for_live_hosts(session, should_be_present);

  // Stopping a node will trigger a "DOWN" event
  ccm->stop_node(2);

  should_be_present.erase(ip_prefix + "2");
  check_for_live_hosts(session, should_be_present);

  // Starting a node will trigger an "UP" event
  int node = ccm->start_node(2);
  test_utils::wait_for_node_connection(ip_prefix, node);

  should_be_present.insert(ip_prefix + "2");
  check_for_live_hosts(session, should_be_present);
}

BOOST_AUTO_TEST_CASE(node_discovery)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(3)) {
    ccm->start_cluster();
  }

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  // Only add a single IP
  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  // Allow the nodes to be discovered
  boost::this_thread::sleep_for(boost::chrono::seconds(20)); //TODO: Remove sleep and implement a pre-check

  check_for_live_hosts(session, build_ip_range(ip_prefix, 1, 3));
}

BOOST_AUTO_TEST_CASE(node_discovery_invalid_ips)
{
  test_utils::CassLog::reset("Unable to reach contact point 192.0.2.");

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    if (ccm->create_cluster(3)) {
      ccm->start_cluster();
    }

    // Ensure RR policy
    cass_cluster_set_load_balance_round_robin(cluster.get());;

    // Add invalid IPs first (http://tools.ietf.org/html/rfc5737)
    cass_cluster_set_contact_points(cluster.get(), "192.0.2.0,192.0.2.1,192.0.2.3");

    // Only add a single valid IP
    test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

    // Make sure the timeout is very high for the initial invalid IPs
    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get(), NULL, 60 * test_utils::ONE_SECOND_IN_MICROS));

    // Allow the nodes to be discovered
    boost::this_thread::sleep_for(boost::chrono::seconds(20)); //TODO: Remove sleep and implement a pre-check

    check_for_live_hosts(session, build_ip_range(ip_prefix, 1, 3));
  }

  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 3ul);
}

BOOST_AUTO_TEST_CASE(node_discovery_no_local_rows)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(3)) {
    ccm->start_cluster();
  }

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  // Only add a single valid IP
  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
    test_utils::execute_query(session.get(), "DELETE FROM system.local WHERE key = 'local'");
  }

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  // Allow the nodes to be discovered
  boost::this_thread::sleep_for(boost::chrono::seconds(20)); //TODO: Remove sleep and implement a pre-check

  check_for_live_hosts(session, build_ip_range(ip_prefix, 1, 3));
}

BOOST_AUTO_TEST_CASE(node_discovery_no_rpc_addresss)
{
  test_utils::CassLog::reset("No rpc_address for host " + ip_prefix + "3 in system.peers on " + ip_prefix + "1. Ignoring this entry.");

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    if (ccm->create_cluster(3)) {
      ccm->start_cluster();
    }

    // Ensure RR policy
    cass_cluster_set_load_balance_round_robin(cluster.get());;

    // Only add a single valid IP
    test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

    // Make the 'rpc_address' null on all applicable hosts (1 and 2)
    {
      test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

      for (int i = 0; i < 3; ++i) {
        std::ostringstream ss;
        ss << "UPDATE system.peers SET rpc_address = null WHERE peer = '" << ip_prefix << "3'";
        test_utils::execute_query(session.get(), ss.str());
      }
    }

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

    // This should only contain 2 address because one peer is ignored
    check_for_live_hosts(session, build_ip_range(ip_prefix, 1, 2));
  }

  BOOST_CHECK(test_utils::CassLog::message_count() > 0);
}

BOOST_AUTO_TEST_CASE(full_outage)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const char* query = "SELECT * FROM system.local";

  if (ccm->create_cluster()) {
    ccm->start_cluster();
  }

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);
  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
  test_utils::execute_query(session.get(), query);

  ccm->stop_cluster();
  BOOST_CHECK(test_utils::execute_query_with_error(session.get(), query) == CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);

  ccm->start_cluster();
  test_utils::wait_for_node_connection(ip_prefix, 1);

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
  test_utils::CassLog::reset("Adding pool for host " + ip_prefix);

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    if (ccm->create_cluster(2)) {
      ccm->start_cluster();
    }

    test_utils::initialize_contact_points(cluster.get(), ip_prefix, 2);
    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

    // Wait for all hosts to be added to the pool; timeout after 10 seconds
    boost::chrono::steady_clock::time_point end = boost::chrono::steady_clock::now() + boost::chrono::milliseconds(10000);
    while (test_utils::CassLog::message_count() != 2 && boost::chrono::steady_clock::now() < end) {
      boost::this_thread::sleep_for(boost::chrono::seconds(1));
    }
    BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 2ul);

    test_utils::CassLog::reset("Spawning new connection to host " + ip_prefix + "1");
    ccm->force_decommission_node(1);
    std::cout << "Node Decommissioned [" << ip_prefix << "1]: Sleeping for 30 seconds" << std::endl;
    boost::this_thread::sleep_for(boost::chrono::seconds(30));
  }

  BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 0ul);

  // Destroy the current cluster (decommissioned node)
  ccm->remove_cluster();
}

/**
 * Randomized contact points
 *
 * This test ensures the driver will randomize the contact points when executing
 * a query plan
 *
 * @since 2.4.3
 * @jira_ticket CPP-193
 * @test_category control_connection
 */
BOOST_AUTO_TEST_CASE(randomized_contact_points)
{
  std::string starting_host = ip_prefix + "1";
  size_t retries = 0;
  test_utils::CassSessionPtr session;

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    if (ccm->create_cluster(4)) {
      ccm->start_cluster();
    }

    test_utils::initialize_contact_points(cluster.get(), ip_prefix, 4);
    cass_cluster_set_use_randomized_contact_points(cluster.get(), cass_true);

    // Make sure the first host executing a statement is not .1
    do {
      test_utils::CassLog::reset("Adding pool for host " + ip_prefix);
      session = test_utils::CassSessionPtr(test_utils::create_session(cluster.get()));

      // Wait for all hosts to be added to the pool; timeout after 10 seconds
      boost::chrono::steady_clock::time_point end = boost::chrono::steady_clock::now() + boost::chrono::milliseconds(10000);
      while (test_utils::CassLog::message_count() != 4ul && boost::chrono::steady_clock::now() < end) {
        boost::this_thread::sleep_for(boost::chrono::seconds(1));
      }
      BOOST_CHECK_EQUAL(test_utils::CassLog::message_count(), 4ul);

      starting_host = get_executing_host(session);
    } while (starting_host == ip_prefix + "1" && retries++ < 10);
  }

  BOOST_CHECK_NE(ip_prefix + "1", starting_host);
  BOOST_CHECK_LT(retries, 10);

  // Ensure the remaining hosts are executed (round robin))
  {
    int node = starting_host.at(starting_host.length() - 1) - '0';
    for (int i = 0; i < 3; ++i) {
      node = (node + 1 > 4) ? 1 : node + 1;
      std::string expected_host = ip_prefix + boost::lexical_cast<std::string>(node);
      std::string host = get_executing_host(session);
      BOOST_CHECK_EQUAL(expected_host, host);
    }
  }

  // Ensure the next host is the starting host
  BOOST_CHECK_EQUAL(starting_host, get_executing_host(session));
}

/**
 * Control connection is properly closed when invalid DC is used in LBP.
 *
 * This test will create a session using a DC aware LBP where the assigned DC is
 * invalid (e.g. does not exist) and ensure the driver does not "hang" and
 * terminates the control connection.
 *
 * @since 2.6.0
 * @test_category control_connection
 * @jira_ticket CPP-398
 * @expected_result Driver will not hang and session/control connection will
 *                  terminate; CASS_ERROR_LIB_NO_HOSTS_AVAILABLE
 */
BOOST_AUTO_TEST_CASE(invalid_dc)
{
  // Create the CCM cluster if it does not already exist (2 DCs)
  if (ccm->create_cluster(1, 1)) {
    ccm->start_cluster();
  }

  {
    // Create the cluster instance and LBP with invalid local DC
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);
    cass_cluster_set_load_balance_dc_aware(cluster.get(), "invalid_dc", 0, cass_false);

    // Establish the connection using invalid DC (ensure no hosts available)
    test_utils::CassSessionPtr session(cass_session_new());
    test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster.get()));
    CassError error_code = cass_future_error_code(connect_future.get());
    BOOST_CHECK_EQUAL(error_code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);

    // Verify the future error message
    CassString message;
    cass_future_error_message(connect_future.get(), &message.data, &message.length);
    BOOST_CHECK_EQUAL("No hosts available for connection using the current load balancing policy", message.data);
  }
}

/**
 * Control connection is properly closed with error using multiple  IO threads.
 *
 * This test will create a session using more than one IO thread multiple times
 * ensuring each time that the driver properly terminates when using an invalid
 * keyspace name during the session connection. The invalid keyspace is being
 * used to tease out a previous bug that was fixed in v2.3.0; however was not
 * tested using multiple IO threads which caused the driver to "hang" which was
 * the direct result of the control connection not closing properly.
 *
 * @since 2.6.0
 * @test_category control_connection
 * @jira_ticket CPP-384
 * @expected_result Driver will not hang and session/control connection will
 *                  terminate with error
 */
BOOST_AUTO_TEST_CASE(terminated_using_multiple_io_threads_with_error)
{
  // Create the CCM cluster if it does not already exist
  if (ccm->create_cluster()) {
    ccm->start_cluster();
  }

  {
    // Create multiple session connection while incrementing the IO threads used
    for (int i = 2; i <= 16; ++i) {
      // Create the cluster instance (with IO threads))
      test_utils::CassClusterPtr cluster(cass_cluster_new());
      test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);
      cass_cluster_set_num_threads_io(cluster.get(), i);

      // Create the session and add the log messages to check for
      test_utils::CassSessionPtr session(cass_session_new());
      std::stringstream message;
      message << "Creating " << i << " IO worker threads";
      test_utils::CassLog::reset(message.str());
      message.str("");
      message << "Connection ready on host " << ip_prefix << "1";
      test_utils::CassLog::add(message.str());
      test_utils::CassLog::add("Session is disconnected");

      // Establish the connection using invalid keyspace
      test_utils::CassFuturePtr connect_future(cass_session_connect_keyspace(session.get(), cluster.get(), "invalid"));
      CassError code = cass_future_error_code(connect_future.get());
      BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE);
      BOOST_CHECK_EQUAL(3ul, test_utils::CassLog::message_count());
    }
  }
}

BOOST_AUTO_TEST_SUITE_END()
