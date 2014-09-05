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

#include "testing.hpp"

#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>

#include <set>
#include <sstream>

struct ControlConnectionTests {
  ControlConnectionTests() {}
};

BOOST_FIXTURE_TEST_SUITE(control_connection, ControlConnectionTests)

BOOST_AUTO_TEST_CASE(test_connect_invalid_ip)
{
  boost::scoped_ptr<test_utils::LogData> log_data(new test_utils::LogData("Connection: Host 1.1.1.1 had the following error on startup: 'Connection timeout'"));

  test_utils::CassClusterPtr cluster(cass_cluster_new());
  cass_cluster_set_contact_points(cluster.get(), "1.1.1.1");
  cass_cluster_set_log_callback(cluster.get(), test_utils::count_message_log_callback, log_data.get());
  {
    test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster.get()));
    CassError code = test_utils::wait_and_return_error(session_future.get());
    BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
  }
  BOOST_CHECK(log_data->message_count > 0);
}

BOOST_AUTO_TEST_CASE(test_connect_invalid_port)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create(conf, "test", 1, 0);

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  cass_cluster_set_port(cluster.get(), 9999); // Invalid port

  test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster.get()));
  CassError code = test_utils::wait_and_return_error(session_future.get());

  BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_CASE(test_reconnection)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create(conf, "test", 2, 0);

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  // Stop the node of the current control connection
  ccm->stop(1);

  // Add a new node to make sure the node gets added on the new control connection to node 2
  ccm->bootstrap(3);
  boost::this_thread::sleep_for(boost::chrono::seconds(10));

  // Stop the other node
  ccm->stop(2);

  std::set<std::string> hosts;
  for (int i = 0; i < 2; ++i) {
    CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
    test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
    test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
    BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);
    hosts.insert(cass::get_host_from_future(future.get()));
  }

  BOOST_CHECK(hosts.size() == 1);
  BOOST_CHECK(hosts.count("127.0.0.3") > 0);
}

BOOST_AUTO_TEST_CASE(test_node_discovery)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create(conf, "test", 3, 0);

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  // Only add a single IP
  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  std::set<std::string> hosts;
  for (int i = 0; i < 3; ++i) {
    CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
    test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
    test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
    hosts.insert(cass::get_host_from_future(future.get()));
  }

  BOOST_CHECK(hosts.size() == 3);
}

BOOST_AUTO_TEST_CASE(test_node_discovery_invalid_ips)
{
  boost::scoped_ptr<test_utils::LogData> log_data(new test_utils::LogData("Unable to reach contact point 192.0.2."));

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());


    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create(conf, "test", 3, 0);

    cass_cluster_set_log_callback(cluster.get(), test_utils::count_message_log_callback, log_data.get());

    // Ensure RR policy
    cass_cluster_set_load_balance_round_robin(cluster.get());;

    // Add invalid IPs first (http://tools.ietf.org/html/rfc5737)
    cass_cluster_set_contact_points(cluster.get(), "192.0.2.0,192.0.2.1,192.0.2.3");

    // Only add a single valid IP
    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    std::set<std::string> hosts;
    for (int i = 0; i < 4; ++i) {
      CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
      test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
      test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
      hosts.insert(cass::get_host_from_future(future.get()));
    }

    BOOST_CHECK(hosts.size() == 3);
  }

  BOOST_CHECK_EQUAL(log_data->message_count, 3);
}

BOOST_AUTO_TEST_CASE(test_node_discovery_no_local_rows)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create(conf, "test", 3, 0);

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  // Only add a single valid IP
  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));
    test_utils::execute_query(session.get(), "DELETE FROM system.local WHERE key = 'local'");
  }

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  std::set<std::string> hosts;
  for (int i = 0; i < 3; ++i) {
    CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
    test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
    test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
    hosts.insert(cass::get_host_from_future(future.get()));
  }

  BOOST_CHECK(hosts.size() == 3);
}

BOOST_AUTO_TEST_CASE(test_node_discovery_no_rpc_addresss)
{
  boost::scoped_ptr<test_utils::LogData> log_data(new test_utils::LogData("No rpc_address for host 127.0.0.2 in system.peers on 127.0.0.1. Ignoring this entry."));

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create(conf, "test", 3, 0);

    cass_cluster_set_log_callback(cluster.get(), test_utils::count_message_log_callback, log_data.get());

    // Ensure RR policy
    cass_cluster_set_load_balance_round_robin(cluster.get());;

    // Only add a single valid IP
    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

    {
      test_utils::CassSessionPtr session(test_utils::create_session(cluster));
      std::ostringstream ss;
      ss << "UPDATE system.peers SET rpc_address = null WHERE peer = '" << conf.ip_prefix() << "2'";
      test_utils::execute_query(session.get(), ss.str());
    }

    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    std::set<std::string> hosts;
    for (int i = 0; i < 3; ++i) {
      CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
      test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
      test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
      hosts.insert(cass::get_host_from_future(future.get()));
    }

    // This should only contain 2 address because one pee is ignored
    BOOST_CHECK(hosts.size() == 2);
  }

  BOOST_CHECK(log_data->message_count > 0);
}


BOOST_AUTO_TEST_SUITE_END()
