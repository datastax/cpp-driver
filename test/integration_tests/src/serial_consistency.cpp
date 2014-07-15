#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
# define BOOST_TEST_MODULE cassandra
#endif

#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"
#include "policy_tools.hpp"

#include "cassandra.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp>
#include <boost/format.hpp>

struct SerialConsistencyTests : public test_utils::SingleSessionTest {
  SerialConsistencyTests() : test_utils::SingleSessionTest(1, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
    test_utils::execute_query(session, "CREATE TABLE test (key text PRIMARY KEY, value int);");
  }
};

BOOST_FIXTURE_TEST_SUITE(serial_consistency, SerialConsistencyTests)

BOOST_AUTO_TEST_CASE(test_invalid)
{
  CassString insert_query = cass_string_init("INSERT INTO test (key, value) VALUES (?, ?) IF NOT EXISTS;");

  test_utils::CassStatementPtr statement = test_utils::make_shared(cass_statement_new(insert_query, 2));
  cass_statement_bind_string(statement.get(), 0, cass_string_init("abc"));
  cass_statement_bind_int32(statement.get(), 1, 99);

  cass_statement_set_serial_consistency(statement.get(), CASS_CONSISTENCY_ONE); // Invalid
  test_utils::CassFuturePtr future = test_utils::make_shared(cass_session_execute(session, statement.get()));

  CassError code = cass_future_error_code(future.get());
  BOOST_REQUIRE(code == CASS_ERROR_SERVER_INVALID_QUERY);

  CassString message = cass_future_error_message(future.get());
  BOOST_REQUIRE(strncmp(message.data, "Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL", message.length) == 0);
}

BOOST_AUTO_TEST_SUITE_END()
