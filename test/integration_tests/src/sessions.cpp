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

#include "cassandra.h"
#include "test_utils.hpp"

void check_error_log_callback(void* data,
                              cass_uint64_t time,
                              CassLogLevel severity,
                              CassString message) {
  std::string str(message.data, message.length);
  BOOST_REQUIRE(str.find("Timed out during startup") == std::string::npos); // JIRA CPP-127
}

struct SessionTests : test_utils::MultipleNodesTest {
    SessionTests() : MultipleNodesTest(1, 0) {
      cass_cluster_set_log_callback(cluster, NULL, check_error_log_callback);
    }
};

BOOST_FIXTURE_TEST_SUITE(sessions, SessionTests)

BOOST_AUTO_TEST_CASE(test_session_close)
{
  // Create new connections after 1 pending request
  cass_cluster_set_max_simultaneous_requests_threshold(cluster, 1);
  cass_cluster_set_max_connections_per_host(cluster, 10);

  for (int i = 0; i < 100; ++i) {
    test_utils::CassFuturePtr session_future = test_utils::make_shared(cass_cluster_connect(cluster));
    test_utils::wait_and_check_error(session_future.get());
    test_utils::CassSessionPtr session = test_utils::make_shared(cass_future_get_session(session_future.get()));

    for (int j = 0; j < 10; ++j) {
      CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces");
      test_utils::CassStatementPtr statement = test_utils::make_shared(cass_statement_new(query, 0));
      cass_future_free(cass_session_execute(session.get(), statement.get()));
    }
  }
}

BOOST_AUTO_TEST_SUITE_END()
