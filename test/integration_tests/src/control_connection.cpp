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

#include "address.hpp"
#include "cassandra.h"
#include "types.hpp"

#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>

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

BOOST_AUTO_TEST_CASE(test_node_discovery)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create(conf, "test", 3, 0);

  // Ensure RR policy
  cass_cluster_set_load_balance_round_robin(cluster.get());;

  // Only add a single IP
  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster.get()));
  test_utils::wait_and_check_error(session_future.get());
  test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));

  cass::AddressSet addresses;
  for (int i = 0; i < 3; ++i) {
    CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
    test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
    test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
    addresses.insert(static_cast<cass::ResponseFuture*>(future->from())->get_host_address());
  }

  BOOST_CHECK(addresses.size() == 3);
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

    test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster.get()));
    test_utils::wait_and_check_error(session_future.get());
    test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));

    cass::AddressSet addresses;
    for (int i = 0; i < 4; ++i) {
      CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
      test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
      test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
      addresses.insert(static_cast<cass::ResponseFuture*>(future->from())->get_host_address());
    }

    BOOST_CHECK(addresses.size() == 3);
  }

  BOOST_CHECK_EQUAL(log_data->message_count, 3);
}

BOOST_AUTO_TEST_SUITE_END()
