#define BOOST_TEST_DYN_LINK
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

    test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster.get()));
    test_utils::wait_and_check_error(session_future.get());
    test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));

    test_utils::CassResultPtr result;
    test_utils::execute_query(session.get(), "SELECT * FROM system.schema_keyspaces", &result);

    BOOST_CHECK(cass_result_row_count(result.get()) > 0);
  }

  void invalid_credentials(int protocol_version, const char* username, const char* password, const char* expected_error) {
    boost::scoped_ptr<test_utils::LogData> log_data(new test_utils::LogData(expected_error));

    {
      cass_cluster_set_log_callback(cluster.get(), test_utils::count_message_log_callback, log_data.get());
      cass_cluster_set_protocol_version(cluster.get(), protocol_version);
      cass_cluster_set_credentials(cluster.get(), username, password);

      test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster.get()));

      test_utils::wait_and_check_error(session_future.get());
      test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));

      CassError code = test_utils::execute_query_with_error(session.get(), "SELECT * FROM system.schema_keyspaces");
      BOOST_CHECK_EQUAL(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, code);
    }

    BOOST_CHECK(log_data->message_count > 0);
  }

  test_utils::CassClusterPtr cluster;
  const cql::cql_ccm_bridge_configuration_t& conf;
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm;
};

BOOST_FIXTURE_TEST_SUITE(authentication, AthenticationTests)

BOOST_AUTO_TEST_CASE(test_auth)
{
  auth(1);
  auth(2);
}

BOOST_AUTO_TEST_CASE(test_empty_credentials)
{
  const char* expected_error
      = "java.lang.AssertionError: org.apache.cassandra.exceptions.InvalidRequestException: Key may not be empty";
  invalid_credentials(1, "", "", expected_error);
  invalid_credentials(2, "", "", expected_error);
}

BOOST_AUTO_TEST_CASE(test_invalid_credentials)
{
  const char* expected_error
      = "Error response during startup: 'Username and/or password are incorrect";
  invalid_credentials(1, "invalid", "invalid", expected_error);
  invalid_credentials(2, "invalid", "invalid", expected_error);
}

BOOST_AUTO_TEST_SUITE_END()
