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

struct PreparedOutageTests : public test_utils::SingleSessionTest {
  PreparedOutageTests() : SingleSessionTest(3, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "2"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }
};

BOOST_FIXTURE_TEST_SUITE(prepared_outage, PreparedOutageTests)

BOOST_AUTO_TEST_CASE(reprepared_on_new_node)
{
  std::string table_name = "test";

  test_utils::execute_query(session, str(boost::format("CREATE TABLE %s (key text PRIMARY KEY, value int);") % table_name));

  std::string insert_query = "INSERT INTO %s (key, value) VALUES ('%s', %d);";
  test_utils::execute_query(session, str(boost::format(insert_query) % table_name % "123" % 17));
  test_utils::execute_query(session, str(boost::format(insert_query) % table_name % "456" % 18));


  std::string select_query = str(boost::format("SELECT * FROM %s WHERE key = ?;") % table_name);
  test_utils::CassFuturePtr prepared_future(cass_session_prepare_n(session,
                                                                   select_query.data(), select_query.size()));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  {
    test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
    BOOST_REQUIRE(cass_statement_bind_string(statement.get(), 0, "123") == CASS_OK);
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result(cass_future_get_result(future.get()));
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

  for (int i = 0; i < 10; ++i) {
    test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
    BOOST_REQUIRE(cass_statement_bind_string(statement.get(), 0, "456") == CASS_OK);
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result(cass_future_get_result(future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

    const CassRow* row = cass_result_first_row(result.get());
    cass_int32_t result_value;
    BOOST_REQUIRE(cass_value_get_int32(cass_row_get_column(row, 1), &result_value) == CASS_OK);
    BOOST_REQUIRE(result_value == 18);
  }

  test_utils::execute_query(session, str(boost::format(insert_query) % table_name % "789" % 19));
  ccm->start(2);
  ccm->gossip(1, false);

  for (int i = 0; i < 10; ++i) {
    test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
    BOOST_REQUIRE(cass_statement_bind_string(statement.get(), 0, "789") == CASS_OK);
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result(cass_future_get_result(future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

    const CassRow* row = cass_result_first_row(result.get());
    cass_int32_t result_value;
    BOOST_REQUIRE(cass_value_get_int32(cass_row_get_column(row, 1), &result_value) == CASS_OK);
    BOOST_REQUIRE(result_value == 19);
  }

  ccm->gossip(1, true);
  ccm->binary(2, false);
  ccm->binary(1, false);

  //Ensure the binary protocol is disabled before executing the inserts
  boost::this_thread::sleep_for(boost::chrono::seconds(5));
  test_utils::execute_query(session, str(boost::format(insert_query) % table_name % "123456789" % 20));
  ccm->binary(2, true);


  for (int i = 0; i < 10; ++i) {
    test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
    BOOST_REQUIRE(cass_statement_bind_string(statement.get(), 0, "123456789") == CASS_OK);
    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
    test_utils::wait_and_check_error(future.get());
    test_utils::CassResultPtr result(cass_future_get_result(future.get()));
    BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 2);

    const CassRow* row = cass_result_first_row(result.get());
    cass_int32_t result_value;
    BOOST_REQUIRE(cass_value_get_int32(cass_row_get_column(row, 1), &result_value) == CASS_OK);
    BOOST_REQUIRE(result_value == 20);
  }
}

BOOST_AUTO_TEST_SUITE_END()
