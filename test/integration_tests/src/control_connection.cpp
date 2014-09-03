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

#include "cassandra.h"
#include "test_utils.hpp"
#include "cql_ccm_bridge.hpp"

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

BOOST_AUTO_TEST_SUITE_END()
