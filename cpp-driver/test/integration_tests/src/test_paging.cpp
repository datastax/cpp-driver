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

  ~PagingTests() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(session,
      str(boost::format(test_utils::DROP_KEYSPACE_FORMAT)
      % test_utils::SIMPLE_KEYSPACE));
  }

  void insert_rows(int num_rows) {
    std::string insert_query = "INSERT INTO test (part, key, value) VALUES (?, ?, ?);";
    test_utils::CassStatementPtr statement(cass_statement_new(insert_query.c_str(), 3));

    // Determine if bound parameters can be used based on C* version
    if (version.major_version == 1) {
      test_utils::CassPreparedPtr prepared = test_utils::prepare(session, insert_query.c_str());
      statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    }

    const cass_int32_t part_key = 0;

    for (int i = 0; i < num_rows; ++i) {
      cass_statement_bind_int32(statement.get(), 0, part_key);
      cass_statement_bind_uuid(statement.get(), 1, test_utils::generate_time_uuid(uuid_gen));
      cass_statement_bind_int32(statement.get(), 2, i);
      test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
      test_utils::wait_and_check_error(future.get());
    }
  }
};

BOOST_FIXTURE_TEST_SUITE(paging, PagingTests)

BOOST_AUTO_TEST_CASE(paging_simple)
{
  const int num_rows = 100;
  const int page_size = 5;

  std::string select_query = "SELECT value FROM test";

  insert_rows(num_rows);

  test_utils::CassResultPtr result;
  test_utils::CassStatementPtr statement(test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 0)));
  cass_statement_set_paging_size(statement.get(), page_size);

  cass_int32_t count = 0;
  do {
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    result = test_utils::CassResultPtr(cass_future_get_result(future.get()));

    test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

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

BOOST_AUTO_TEST_CASE(paging_raw)
{
  const int num_rows = 100;
  const int page_size = 5;

  std::string select_query = "SELECT value FROM test";

  insert_rows(num_rows);

  test_utils::CassResultPtr result;
  test_utils::CassStatementPtr statement(test_utils::CassStatementPtr(cass_statement_new(select_query.c_str(), 0)));
  cass_statement_set_paging_size(statement.get(), page_size);

  cass_int32_t count = 0;
  do {
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    result = test_utils::CassResultPtr(cass_future_get_result(future.get()));

    test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

    while (cass_iterator_next(iterator.get())) {
      const CassRow* row = cass_iterator_get_row(iterator.get());
      cass_int32_t value;
      cass_value_get_int32(cass_row_get_column(row, 0), &value);
      BOOST_REQUIRE(value == count++);
    }


    if (cass_result_has_more_pages(result.get())) {
      const char* paging_state;
      size_t paging_state_size;
      BOOST_CHECK(cass_result_paging_state_token(result.get(), &paging_state, &paging_state_size) == CASS_OK);
      cass_statement_set_paging_state_token(statement.get(), paging_state, paging_state_size);
    }
  } while (cass_result_has_more_pages(result.get()));
}

BOOST_AUTO_TEST_CASE(paging_empty)
{
  const int page_size = 5;

  const char* select_query = "SELECT value FROM test";

  test_utils::CassStatementPtr statement(cass_statement_new(select_query, 0));
  cass_statement_set_paging_size(statement.get(), page_size);

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
  test_utils::wait_and_check_error(future.get());
  test_utils::CassResultPtr result(cass_future_get_result(future.get()));
  BOOST_REQUIRE(cass_result_has_more_pages(result.get()) == cass_false);
}


BOOST_AUTO_TEST_SUITE_END()
