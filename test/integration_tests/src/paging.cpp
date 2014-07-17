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

struct PagingTests : public test_utils::SingleSessionTest {
    PagingTests() : SingleSessionTest(1, 0) {
      test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                             % test_utils::SIMPLE_KEYSPACE % "1"));
      test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
      test_utils::execute_query(session, "CREATE TABLE test (part int, key timeuuid, value int, PRIMARY KEY(part, key));");
    }
};

BOOST_FIXTURE_TEST_SUITE(paging, PagingTests)

BOOST_AUTO_TEST_CASE(test_paging_simple)
{
  const int num_rows = 100;
  const int page_size = 5;

  CassString insert_query = cass_string_init("INSERT INTO test (part, key, value) VALUES (?, ?, ?);");

  const cass_int32_t part_key = 0;

  for (int i = 0; i < num_rows; ++i) {
    test_utils::CassStatementPtr statement = test_utils::make_shared(cass_statement_new(insert_query, 3));
    cass_statement_bind_int32(statement.get(), 0, part_key);
    cass_statement_bind_uuid(statement.get(), 1, test_utils::generate_time_uuid().uuid);
    cass_statement_bind_int32(statement.get(), 2, i);
    test_utils::CassFuturePtr future = test_utils::make_shared(cass_session_execute(session, statement.get()));
    BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);
  }

  CassString select_query = cass_string_init("SELECT value FROM test");

  test_utils::CassResultPtr result;
  test_utils::CassStatementPtr statement = test_utils::make_shared(cass_statement_new(select_query, 0));
  cass_statement_set_paging_size(statement.get(), page_size);

  cass_int32_t count = 0;
  do {
    test_utils::CassFuturePtr future = test_utils::make_shared(cass_session_execute(session, statement.get()));
    BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);
    result = test_utils::make_shared(cass_future_get_result(future.get()));

    test_utils::CassIteratorPtr iterator = test_utils::make_shared(cass_iterator_from_result(result.get()));

    while (cass_iterator_next(iterator.get())) {
      const CassRow* row = cass_iterator_get_row(iterator.get());
      cass_int32_t value;
      cass_value_get_int32(cass_row_get_column(row, 0), &value);
      BOOST_REQUIRE(value == count++);
    }

    if (cass_result_has_more_pages(result.get())) {
      cass_statement_set_paging_state(statement.get(), result.get());
    }
   } while (cass_result_has_more_pages(result.get()));
}

BOOST_AUTO_TEST_CASE(test_paging_empty)
{
  const int page_size = 5;

  CassString select_query = cass_string_init("SELECT value FROM test");

  test_utils::CassStatementPtr statement = test_utils::make_shared(cass_statement_new(select_query, 0));
  cass_statement_set_paging_size(statement.get(), page_size);

  test_utils::CassFuturePtr future = test_utils::make_shared(cass_session_execute(session, statement.get()));
  BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);
  test_utils::CassResultPtr result = test_utils::make_shared(cass_future_get_result(future.get()));
  BOOST_REQUIRE(cass_result_has_more_pages(result.get()) == cass_false);
}


BOOST_AUTO_TEST_SUITE_END()
