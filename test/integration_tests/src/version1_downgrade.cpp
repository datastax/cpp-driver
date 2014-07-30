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

#include "cassandra.h"
#include "test_utils.hpp"
#include "cql_ccm_bridge.hpp"

struct Version1DowngradeTests {
    Version1DowngradeTests() {}
};

BOOST_FIXTURE_TEST_SUITE(version1_downgrade, Version1DowngradeTests)

BOOST_AUTO_TEST_CASE(test_query_after_downgrade)
{
  boost::shared_ptr<test_utils::LogData> log_data(
        new test_utils::LogData("Protocol version 2 unsupported. Trying protocol version 1.."));

  cass_size_t row_count;

  {
    test_utils::CassClusterPtr cluster = test_utils::make_shared(cass_cluster_new());

    const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration("config_v1.txt");

    boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create(conf, "test", 3, 0);

    test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 3, 0);

    cass_cluster_set_protocol_version(cluster.get(), 2);

    cass_cluster_set_log_callback(cluster.get(), test_utils::count_message_log_callback, log_data.get());

    test_utils::CassFuturePtr session_future = test_utils::make_shared(cass_cluster_connect(cluster.get()));
    test_utils::wait_and_check_error(session_future.get());
    test_utils::CassSessionPtr session = test_utils::make_shared(cass_future_get_session(session_future.get()));

    test_utils::CassResultPtr result;
    test_utils::execute_query(session.get(), "SELECT * FROM system.schema_keyspaces", &result);
  
    row_count = cass_result_row_count(result.get());
  }

  BOOST_CHECK(row_count > 0);
  BOOST_CHECK(log_data->message_count > 0);
}

BOOST_AUTO_TEST_SUITE_END()
