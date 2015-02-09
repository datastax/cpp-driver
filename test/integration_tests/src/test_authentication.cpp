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

#include "cassandra.h"
#include "test_utils.hpp"
#include "cql_ccm_bridge.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>

struct AthenticationTests {
  AthenticationTests()
    : cluster(cass_cluster_new())
    , conf(cql::get_ccm_bridge_configuration())
    , ccm(cql::cql_ccm_bridge_t::create(conf, "test")) {
    ccm->populate(1);
    ccm->update_config("authenticator", "PasswordAuthenticator");
    ccm->start(1, "-Dcassandra.superuser_setup_delay_ms=0");
    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

    // Sometimes the superuser will still not be setup
    boost::this_thread::sleep_for(boost::chrono::seconds(1));
  }

  void auth(int protocol_version) {
    cass_cluster_set_protocol_version(cluster.get(), protocol_version);
    cass_cluster_set_credentials(cluster.get(), "cassandra", "cassandra");

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

    test_utils::CassResultPtr result;
    test_utils::execute_query(session.get(), "SELECT * FROM system.schema_keyspaces", &result);

    BOOST_CHECK(cass_result_row_count(result.get()) > 0);
  }

  void invalid_credentials(int protocol_version, const char* username, 
                           const char* password, const char* expected_error,
                           CassError expected_code) {
    test_utils::CassLog::reset(expected_error);

    cass_cluster_set_protocol_version(cluster.get(), protocol_version);
    cass_cluster_set_credentials(cluster.get(), username, password);
    {
      CassError code;
      test_utils::CassSessionPtr temp_session(test_utils::create_session(cluster.get(), &code));
      BOOST_CHECK_EQUAL(expected_code, code);
    }
    BOOST_CHECK(test_utils::CassLog::message_count() > 0);
  }

  test_utils::CassClusterPtr cluster;
  const cql::cql_ccm_bridge_configuration_t& conf;
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm;
};

BOOST_FIXTURE_TEST_SUITE(authentication, AthenticationTests)

BOOST_AUTO_TEST_CASE(protocol_versions)
{
  auth(1);
  auth(2);
}

BOOST_AUTO_TEST_CASE(empty_credentials)
{
  // This is a case that could be guarded in the API entry point, or errored in connection. However,
  // auth is subject to major changes and this is just a simple form.
  // This test serves to characterize what is there presently.
  const char* expected_error
      = "java.lang.AssertionError: org.apache.cassandra.exceptions.InvalidRequestException: Key may not be empty";
  invalid_credentials(1, "", "", expected_error, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
  invalid_credentials(2, "", "", expected_error, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_CASE(bad_credentials)
{
  const char* expected_error
      = "'Username and/or password are incorrect'";
  invalid_credentials(1, "invalid", "invalid", expected_error, CASS_ERROR_SERVER_BAD_CREDENTIALS);
  invalid_credentials(2, "invalid", "invalid", expected_error, CASS_ERROR_SERVER_BAD_CREDENTIALS);
}

BOOST_AUTO_TEST_SUITE_END()
