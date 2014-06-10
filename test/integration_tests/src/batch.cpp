#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cassandra.h"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

struct BatchTests : test_utils::SingleSessionTest {
    static const char* SIMPLE_TABLE_NAME;

    BatchTests() : SingleSessionTest(3,0) {
      test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                             % test_utils::SIMPLE_KEYSPACE % "1"));
      test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
      test_utils::execute_query(session, str(boost::format("CREATE TABLE %s (tweet_id int PRIMARY KEY, test_val text);") % SIMPLE_TABLE_NAME));
    }
};

const char* BatchTests::SIMPLE_TABLE_NAME = "simple_batch_testing_table";

BOOST_FIXTURE_TEST_SUITE(batch, BatchTests)


void validate_results(CassSession* session, int num_rows) {
  std::string select_query = str(boost::format("SELECT * FROM %s WHERE tweet_id = ?;") % BatchTests::SIMPLE_TABLE_NAME);

  for(int y = 0; y < num_rows; y++)
  {
    test_utils::CassStatementPtr select_statement(cass_statement_new(cass_string_init(select_query.c_str()), 1, CASS_CONSISTENCY_ONE));
    BOOST_REQUIRE(cass_statement_bind_int32(select_statement.get(), 0, y) == CASS_OK);
    test_utils::CassFuturePtr select_future(cass_session_execute(session, select_statement.get()));
    test_utils::wait_and_check_error(select_future.get());
    test_utils::CassResultPtr result(cass_future_get_result(select_future.get()));
    const CassValue* column = cass_row_get_column(cass_result_first_row(result.get()), 1);

    CassString result_value;
    BOOST_REQUIRE(cass_value_type(column) == CASS_VALUE_TYPE_VARCHAR);
    BOOST_REQUIRE(test_utils::Value<CassString>::get(column, &result_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal(result_value, cass_string_init(str(boost::format("test data %s") % y).c_str())));
  }
}

BOOST_AUTO_TEST_CASE(test_prepared)
{
  test_utils::CassBatchPtr batch(cass_batch_new(CASS_CONSISTENCY_ONE));
  std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(?, ?);") % BatchTests::SIMPLE_TABLE_NAME);

  test_utils::CassFuturePtr prepared_future(cass_session_prepare(session,
                                                                 cass_string_init2(insert_query.data(), insert_query.size())));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  for(int x = 0; x < 4; x++)
  {
    test_utils::CassStatementPtr insert_statement(cass_prepared_bind(prepared.get(), 2, CASS_CONSISTENCY_ONE));
    BOOST_REQUIRE(cass_statement_bind_int32(insert_statement.get(), 0, x) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_string(insert_statement.get(), 1, cass_string_init(str(boost::format("test data %s") % x).c_str())) == CASS_OK);
    cass_batch_add_statement(batch.get(), insert_statement.get());
  }

  test_utils::CassFuturePtr insert_future(cass_session_execute_batch(session, batch.get()));
  test_utils::wait_and_check_error(insert_future.get());

  validate_results(session, 4);
}

BOOST_AUTO_TEST_CASE(test_simple)
{
  test_utils::CassBatchPtr batch(cass_batch_new(CASS_CONSISTENCY_ONE));
  std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(?, ?);") % BatchTests::SIMPLE_TABLE_NAME);

  for(int x = 0; x < 4; x++)
  {
    test_utils::CassStatementPtr insert_statement(cass_statement_new(cass_string_init(insert_query.c_str()), 2, CASS_CONSISTENCY_ONE));
    BOOST_REQUIRE(cass_statement_bind_int32(insert_statement.get(), 0, x) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_string(insert_statement.get(), 1, cass_string_init(str(boost::format("test data %s") % x).c_str())) == CASS_OK);
    cass_batch_add_statement(batch.get(), insert_statement.get());
  }

  test_utils::CassFuturePtr insert_future(cass_session_execute_batch(session, batch.get()));
  test_utils::wait_and_check_error(insert_future.get());

  validate_results(session, 4);
}

BOOST_AUTO_TEST_CASE(test_mixed)
{
  test_utils::CassBatchPtr batch(cass_batch_new(CASS_CONSISTENCY_ONE));
  std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(?, ?);") % BatchTests::SIMPLE_TABLE_NAME);

  test_utils::CassFuturePtr prepared_future(cass_session_prepare(session,
                                                                 cass_string_init2(insert_query.data(), insert_query.size())));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  for(int x = 0; x < 1000; x++)
  {
    test_utils::CassStatementPtr insert_statement;
    if(x % 2 == 0) {
      insert_statement.reset(cass_prepared_bind(prepared.get(), 2, CASS_CONSISTENCY_ONE));
    } else {
      insert_statement.reset(cass_statement_new(cass_string_init(insert_query.c_str()), 2, CASS_CONSISTENCY_ONE));
    }
    BOOST_REQUIRE(cass_statement_bind_int32(insert_statement.get(), 0, x) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_string(insert_statement.get(), 1, cass_string_init(str(boost::format("test data %s") % x).c_str())) == CASS_OK);
    cass_batch_add_statement(batch.get(), insert_statement.get());
  }

  test_utils::CassFuturePtr insert_future(cass_session_execute_batch(session, batch.get()));
  test_utils::wait_and_check_error(insert_future.get());

  validate_results(session, 1000);
}

BOOST_AUTO_TEST_SUITE_END()
