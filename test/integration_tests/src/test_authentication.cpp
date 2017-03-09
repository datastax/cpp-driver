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

#include "cassandra.h"
#include "test_utils.hpp"

#include <sstream>

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
    , ccm(new CCM::Bridge("config.txt"))
    , version(test_utils::get_version()) {
    ccm->create_cluster();
    ccm->kill_cluster();
    ccm->update_cluster_configuration("authenticator", "PasswordAuthenticator");
    ccm->start_cluster("-Dcassandra.superuser_setup_delay_ms=0");
    test_utils::initialize_contact_points(cluster.get(), CCM::Bridge::get_ip_prefix("config.txt"), 1);
  }

  ~AthenticationTests() {
    //TODO: Add name generation for simple auth tests
    ccm->remove_cluster();
  }

  void auth(int protocol_version) {
    cass_cluster_set_protocol_version(cluster.get(), protocol_version);
    cass_cluster_set_credentials(cluster.get(), "cassandra", "cassandra");

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

    test_utils::CassResultPtr result;
    std::stringstream query;
    query << "SELECT * FROM " << (version >= "3.0.0" ? "system_schema.keyspaces" : "system.schema_keyspaces");
    test_utils::execute_query(session.get(), query.str().c_str(), &result);

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
  boost::shared_ptr<CCM::Bridge> ccm;
  CCM::CassVersion version;
};

BOOST_FIXTURE_TEST_SUITE(authentication, AthenticationTests)

BOOST_AUTO_TEST_CASE(protocol_versions)
{
  // Handle deprecated and removed protocol versions [CASSANDRA-10146]
  // https://issues.apache.org/jira/browse/CASSANDRA-10146
  if (version < "2.2.0") {
    auth(1);
    auth(2);
  }
  auth(3);
  auth(4);
}

BOOST_AUTO_TEST_CASE(empty_credentials)
{
  // This is a case that could be guarded in the API entry point, or errored in connection. However,
  // auth is subject to major changes and this is just a simple form.
  // This test serves to characterize what is there presently.
  const char* expected_error
      = "Key may not be empty";
  // Handle deprecated and removed protocol versions [CASSANDRA-10146]
  // https://issues.apache.org/jira/browse/CASSANDRA-10146
  if (version < "2.2.0") {
    invalid_credentials(1, "", "", expected_error, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
    invalid_credentials(2, "", "", expected_error, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
  }
  invalid_credentials(3, "", "", expected_error, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
  invalid_credentials(4, "", "", expected_error, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

BOOST_AUTO_TEST_CASE(bad_credentials)
{
  std::string expected_error = "Username and/or password are incorrect";
  if (version >= "3.10") {
    expected_error = "Provided username invalid and/or password are incorrect";
  }
  // Handle deprecated and removed protocol versions [CASSANDRA-10146]
  // https://issues.apache.org/jira/browse/CASSANDRA-10146
  if (version < "2.2.0") {
    invalid_credentials(1, "invalid", "invalid", expected_error.c_str(), CASS_ERROR_SERVER_BAD_CREDENTIALS);
    invalid_credentials(2, "invalid", "invalid", expected_error.c_str(), CASS_ERROR_SERVER_BAD_CREDENTIALS);
  }
  invalid_credentials(3, "invalid", "invalid", expected_error.c_str(), CASS_ERROR_SERVER_BAD_CREDENTIALS);
  invalid_credentials(4, "invalid", "invalid", expected_error.c_str(), CASS_ERROR_SERVER_BAD_CREDENTIALS);
}

BOOST_AUTO_TEST_SUITE_END()
