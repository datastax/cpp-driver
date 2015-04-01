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

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

struct BatchTests : public test_utils::SingleSessionTest {
  static const char* SIMPLE_TABLE_NAME;
  static const char* COUNTER_TABLE_NAME;

  BatchTests() : SingleSessionTest(3,0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
    test_utils::execute_query(session, str(boost::format("CREATE TABLE %s (tweet_id int PRIMARY KEY, test_val text);") % SIMPLE_TABLE_NAME));
    test_utils::execute_query(session, str(boost::format("CREATE TABLE %s (tweet_id int PRIMARY KEY, test_val counter);") % COUNTER_TABLE_NAME));
  }
};

const char* BatchTests::SIMPLE_TABLE_NAME = "simple_batch_testing_table";
const char* BatchTests::COUNTER_TABLE_NAME = "counter_batch_testing_table";

BOOST_FIXTURE_TEST_SUITE(batch, BatchTests)


void validate_results(CassSession* session, int num_rows) {
  std::string select_query = str(boost::format("SELECT * FROM %s WHERE tweet_id = ?;") % BatchTests::SIMPLE_TABLE_NAME);

  for (int y = 0; y < num_rows; y++)
  {
    test_utils::CassStatementPtr select_statement(cass_statement_new(select_query.c_str(), 1));
    BOOST_REQUIRE(cass_statement_bind_int32(select_statement.get(), 0, y) == CASS_OK);
    test_utils::CassFuturePtr select_future(cass_session_execute(session, select_statement.get()));
    test_utils::wait_and_check_error(select_future.get());
    test_utils::CassResultPtr result(cass_future_get_result(select_future.get()));
    const CassValue* column = cass_row_get_column(cass_result_first_row(result.get()), 1);

    CassString result_value;
    BOOST_REQUIRE(cass_value_type(column) == CASS_VALUE_TYPE_VARCHAR);
    BOOST_REQUIRE(test_utils::Value<CassString>::get(column, &result_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal(result_value, CassString(str(boost::format("test data %s") % y).c_str())));
  }
}

BOOST_AUTO_TEST_CASE(prepared)
{
  test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));
  std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(?, ?);") % BatchTests::SIMPLE_TABLE_NAME);

  test_utils::CassFuturePtr prepared_future(cass_session_prepare_n(session,
                                                                   insert_query.data(), insert_query.size()));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  for (int x = 0; x < 4; x++)
  {
    test_utils::CassStatementPtr insert_statement(cass_prepared_bind(prepared.get()));
    BOOST_REQUIRE(cass_statement_bind_int32(insert_statement.get(), 0, x) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_string(insert_statement.get(), 1, str(boost::format("test data %s") % x).c_str()) == CASS_OK);
    cass_batch_add_statement(batch.get(), insert_statement.get());
  }

  test_utils::CassFuturePtr insert_future(cass_session_execute_batch(session, batch.get()));
  test_utils::wait_and_check_error(insert_future.get());

  validate_results(session, 4);
}

BOOST_AUTO_TEST_CASE(simple)
{
  test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));
  std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(?, ?);") % BatchTests::SIMPLE_TABLE_NAME);

  for (int x = 0; x < 4; x++)
  {
    test_utils::CassStatementPtr insert_statement(cass_statement_new(insert_query.c_str(), 2));
    BOOST_REQUIRE(cass_statement_bind_int32(insert_statement.get(), 0, x) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_string(insert_statement.get(), 1, str(boost::format("test data %s") % x).c_str()) == CASS_OK);
    cass_batch_add_statement(batch.get(), insert_statement.get());
  }

  test_utils::CassFuturePtr insert_future(cass_session_execute_batch(session, batch.get()));
  test_utils::wait_and_check_error(insert_future.get());

  validate_results(session, 4);
}

BOOST_AUTO_TEST_CASE(mixed)
{
  test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));
  std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(?, ?);") % BatchTests::SIMPLE_TABLE_NAME);

  test_utils::CassFuturePtr prepared_future(cass_session_prepare_n(session,
                                                                   insert_query.data(), insert_query.size()));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  for (int x = 0; x < 1000; x++)
  {
    test_utils::CassStatementPtr insert_statement;
    if (x % 2 == 0) {
      insert_statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    } else {
      insert_statement = test_utils::CassStatementPtr(cass_statement_new(insert_query.c_str(), 2));
    }
    BOOST_REQUIRE(cass_statement_bind_int32(insert_statement.get(), 0, x) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_string(insert_statement.get(), 1, str(boost::format("test data %s") % x).c_str()) == CASS_OK);
    cass_batch_add_statement(batch.get(), insert_statement.get());
  }

  test_utils::CassFuturePtr insert_future(cass_session_execute_batch(session, batch.get()));
  test_utils::wait_and_check_error(insert_future.get());

  validate_results(session, 1000);
}

BOOST_AUTO_TEST_CASE(invalid_batch_type)
{
  test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));
  std::string update_query = str(boost::format("UPDATE %s SET test_val = test_val + ? WHERE tweet_id = ?;") % BatchTests::COUNTER_TABLE_NAME);

  test_utils::CassStatementPtr update_statement(cass_statement_new(update_query.c_str(), 2));

  const int some_value = 99;
  BOOST_REQUIRE(cass_statement_bind_int64(update_statement.get(), 0, some_value) == CASS_OK);
  BOOST_REQUIRE(cass_statement_bind_int32(update_statement.get(), 1, some_value) == CASS_OK);
  cass_batch_add_statement(batch.get(), update_statement.get());

  test_utils::CassFuturePtr update_future(cass_session_execute_batch(session, batch.get()));
  BOOST_REQUIRE(cass_future_error_code(update_future.get()) == CASS_ERROR_SERVER_INVALID_QUERY);
}

BOOST_AUTO_TEST_CASE(counter_mixed)
{
  test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_COUNTER));
  std::string update_query = str(boost::format("UPDATE %s SET test_val = test_val + ? WHERE tweet_id = ?;") % BatchTests::COUNTER_TABLE_NAME);

  test_utils::CassFuturePtr prepared_future(cass_session_prepare_n(session,
                                                                   update_query.data(), update_query.size()));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  for (int x = 0; x < 1000; x++)
  {
    test_utils::CassStatementPtr update_statement;
    if (x % 2 == 0) {
      update_statement = test_utils::CassStatementPtr(cass_prepared_bind(prepared.get()));
    } else {
      update_statement = test_utils::CassStatementPtr(cass_statement_new(update_query.c_str(), 2));
    }
    cass_statement_set_consistency(update_statement.get(), CASS_CONSISTENCY_QUORUM);
    BOOST_REQUIRE(cass_statement_bind_int64(update_statement.get(), 0, x) == CASS_OK);
    BOOST_REQUIRE(cass_statement_bind_int32(update_statement.get(), 1, x) == CASS_OK);
    cass_batch_add_statement(batch.get(), update_statement.get());
  }

  test_utils::CassFuturePtr update_future(cass_session_execute_batch(session, batch.get()));
  test_utils::wait_and_check_error(update_future.get());

  std::string select_query = str(boost::format("SELECT * FROM %s;") % BatchTests::COUNTER_TABLE_NAME);

  test_utils::CassResultPtr result;

  test_utils::execute_query(session, select_query, &result, CASS_CONSISTENCY_QUORUM);

  BOOST_REQUIRE(cass_result_row_count(result.get()) == 1000);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

  while (cass_iterator_next(iterator.get())) {
    const CassValue* column1 = cass_row_get_column(cass_iterator_get_row(iterator.get()), 0);
    BOOST_REQUIRE(cass_value_type(column1) == CASS_VALUE_TYPE_INT);
    cass_int32_t tweet_id;
    BOOST_REQUIRE(cass_value_get_int32(column1, &tweet_id) == CASS_OK);

    const CassValue* column2 = cass_row_get_column(cass_iterator_get_row(iterator.get()), 1);
    BOOST_REQUIRE(cass_value_type(column2) == CASS_VALUE_TYPE_COUNTER);
    cass_int64_t test_val;
    BOOST_REQUIRE(cass_value_get_int64(column2, &test_val) == CASS_OK);

    BOOST_REQUIRE(tweet_id == test_val);
  }
}

BOOST_AUTO_TEST_SUITE_END()
