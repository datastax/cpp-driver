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

struct SessionTests {
  SessionTests() {}
};

BOOST_FIXTURE_TEST_SUITE(sessions, SessionTests)

BOOST_AUTO_TEST_CASE(test_connect_invalid_name)
{
  boost::scoped_ptr<test_utils::LogData> log_data(new test_utils::LogData("Unable to resolve host node.domain-does-not-exist.dne:9042"));

  CassError code;

  // Note: This test might not work if your DNS provider forwards unresolved DNS requests
  // to a results page.

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());
    cass_cluster_set_contact_points(cluster.get(), "node.domain-does-not-exist.dne");

    cass_cluster_set_log_callback(cluster.get(), test_utils::count_message_log_callback, log_data.get());

    test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster.get()));
    code = cass_future_error_code(session_future.get());

    test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));
  }

  BOOST_CHECK(log_data->message_count > 0);
  BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_CASE(test_connect_invalid_keyspace)
{
  boost::scoped_ptr<test_utils::LogData> log_data(new test_utils::LogData("Error response: 'Keyspace 'invalid' does not exist"));

  CassError code;

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 1);

    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

    cass_cluster_set_log_callback(cluster.get(), test_utils::count_message_log_callback, log_data.get());

    test_utils::CassFuturePtr session_future(cass_cluster_connect_keyspace(cluster.get(), "invalid"));
    test_utils::wait_and_check_error(session_future.get());
    test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));

    CassString query = cass_string_init("SELECT * FROM table");
    test_utils::CassStatementPtr statement(cass_statement_new(query, 0));

    test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));

    code = cass_future_error_code(future.get());
  }

  BOOST_CHECK(log_data->message_count > 0);
  BOOST_CHECK_EQUAL(code, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_CASE(test_close_timeout_error)
{
  boost::scoped_ptr<test_utils::LogData> log_data(new test_utils::LogData("Timed out during startup")); // JIRA CPP-127

  {
    test_utils::CassClusterPtr cluster(cass_cluster_new());

    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 1);

    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

    cass_cluster_set_log_callback(cluster.get(), test_utils::count_message_log_callback, log_data.get());

    // Create new connections after 1 pending request
    cass_cluster_set_max_concurrent_requests_threshold(cluster.get(), 1);
    cass_cluster_set_max_connections_per_host(cluster.get(), 10);

    for (int i = 0; i < 100; ++i) {
      test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster.get()));
      test_utils::wait_and_check_error(session_future.get());
      test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));

      for (int j = 0; j < 10; ++j) {
        CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
        test_utils::CassStatementPtr statement(cass_statement_new(query, 0));
        cass_future_free(cass_session_execute(session.get(), statement.get()));
      }
    }
  }

  BOOST_CHECK_EQUAL(log_data->message_count, 0ul);
}

BOOST_AUTO_TEST_SUITE_END()
