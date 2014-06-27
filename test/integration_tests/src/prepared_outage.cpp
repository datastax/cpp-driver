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

#include "cql_ccm_bridge.hpp"
#include "cassandra.h"
#include "test_utils.hpp"

struct PreparedOutageTests : test_utils::SingleSessionTest {
    PreparedOutageTests() : SingleSessionTest(2, 0) {
      test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                                   % test_utils::SIMPLE_KEYSPACE % "1"));
      test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
    }
};

BOOST_FIXTURE_TEST_SUITE(prepared_outage, PreparedOutageTests)

BOOST_AUTO_TEST_CASE(test_reprepared_on_new_node)
{
  std::string table_name = "test";

  test_utils::execute_query(session, str(boost::format("CREATE TABLE %s (key text PRIMARY KEY, value int);") % table_name));

  std::string insert_query = "INSERT INTO %s (key, value) VALUES ('%s', %d);";
  test_utils::execute_query(session, str(boost::format(insert_query) % table_name % "123" % 17));
  test_utils::execute_query(session, str(boost::format(insert_query) % table_name % "456" % 18));


  std::string select_query = str(boost::format("SELECT * FROM %s WHERE key = ?;") % table_name);
  test_utils::CassFuturePtr prepared_future = test_utils::make_shared(cass_session_prepare(session,
                                                                                           cass_string_init2(select_query.data(), select_query.size())));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared = test_utils::make_shared(cass_future_get_prepared(prepared_future.get()));

  {
    test_utils::CassStatementPtr statement = test_utils::make_shared(cass_prepared_bind(prepared.get(), 1));
    BOOST_REQUIRE(cass_statement_bind_string(statement.get(), 0, cass_string_init("123")) == CASS_OK);
    test_utils::CassFuturePtr future = test_utils::make_shared(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result = test_utils::make_shared(cass_future_get_result(future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

    const CassRow* row = cass_result_first_row(result.get());
    cass_int32_t result_value;
    BOOST_REQUIRE(cass_value_get_int32(cass_row_get_column(row, 1), &result_value) == CASS_OK);
    BOOST_REQUIRE(result_value == 17);
  }

  ccm->stop(1);
  ccm->start(1);
  ccm->stop(2);

  boost::this_thread::sleep_for(boost::chrono::seconds(10));

  for(int i = 0; i < 10; ++i){
    test_utils::CassStatementPtr statement = test_utils::make_shared(cass_prepared_bind(prepared.get(), 1));
    BOOST_REQUIRE(cass_statement_bind_string(statement.get(), 0, cass_string_init("456")) == CASS_OK);
    test_utils::CassFuturePtr future = test_utils::make_shared(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result = test_utils::make_shared(cass_future_get_result(future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

    const CassRow* row = cass_result_first_row(result.get());
    cass_int32_t result_value;
    BOOST_REQUIRE(cass_value_get_int32(cass_row_get_column(row, 1), &result_value) == CASS_OK);
    BOOST_REQUIRE(result_value == 18);
  }
}

BOOST_AUTO_TEST_SUITE_END()
