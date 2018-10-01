/*
  Copyright (c) DataStax, Inc.

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

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

#include <sstream>

struct FuturesTests : public test_utils::MultipleNodesTest {
  FuturesTests() : test_utils::MultipleNodesTest(1, 0) {
  }
};

BOOST_FIXTURE_TEST_SUITE(future, FuturesTests)

BOOST_AUTO_TEST_CASE(error)
{
  test_utils::CassSessionPtr session(cass_session_new());
  test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster));
  test_utils::wait_and_check_error(connect_future.get());

  test_utils::CassStatementPtr statement(cass_statement_new("MALFORMED QUERY", 0));
  test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));

  BOOST_REQUIRE_NE(cass_future_error_code(future.get()), CASS_OK);

  // Should not be set
  BOOST_CHECK(!test_utils::CassResultPtr(cass_future_get_result(future.get())));
  BOOST_CHECK(!test_utils::CassPreparedPtr(cass_future_get_prepared(future.get())));
  BOOST_CHECK(test_utils::CassErrorResultPtr(cass_future_get_error_result(future.get())));

  BOOST_CHECK_EQUAL(cass_future_custom_payload_item_count(future.get()), 0);
  {
    const char* name;
    const cass_byte_t* value;
    size_t name_length, value_size;
    BOOST_REQUIRE_EQUAL(cass_future_custom_payload_item(future.get(), 0,
                                                        &name, &name_length,
                                                        &value, &value_size),
                        CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS);
  }
}

BOOST_AUTO_TEST_CASE(result_response)
{
  test_utils::CassSessionPtr session(cass_session_new());
  test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster));
  test_utils::wait_and_check_error(connect_future.get());

  test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.local", 0));
  test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));

  // Expected
  BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);
  BOOST_CHECK(test_utils::CassResultPtr(cass_future_get_result(future.get())));

  // Should not be set
  BOOST_CHECK(!test_utils::CassErrorResultPtr(cass_future_get_error_result(future.get())));
  BOOST_CHECK(!test_utils::CassPreparedPtr(cass_future_get_prepared(future.get())));

  BOOST_CHECK_EQUAL(cass_future_custom_payload_item_count(future.get()), 0);
  {
    const char* name;
    const cass_byte_t* value;
    size_t name_length, value_size;
    BOOST_REQUIRE_EQUAL(cass_future_custom_payload_item(future.get(), 0,
                                                        &name, &name_length,
                                                        &value, &value_size),
                        CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS);
  }
}

BOOST_AUTO_TEST_CASE(prepare_response)
{
  test_utils::CassSessionPtr session(cass_session_new());
  test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster));
  test_utils::wait_and_check_error(connect_future.get());

  test_utils::CassFuturePtr future(cass_session_prepare(session.get(), "SELECT * FROM system.local"));

  // Expected
  BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);
  BOOST_CHECK(test_utils::CassPreparedPtr(cass_future_get_prepared(future.get())));

  // This returns a value but probably shouldn't. We should consider fixing
  // this, but it could break existing applications.
  BOOST_CHECK(test_utils::CassResultPtr(cass_future_get_result(future.get())));

  // Should not be set
  BOOST_CHECK(!test_utils::CassErrorResultPtr(cass_future_get_error_result(future.get())));

  BOOST_CHECK_EQUAL(cass_future_custom_payload_item_count(future.get()), 0);
  {
    const char* name;
    const cass_byte_t* value;
    size_t name_length, value_size;
    BOOST_REQUIRE_EQUAL(cass_future_custom_payload_item(future.get(), 0,
                                                        &name, &name_length,
                                                        &value, &value_size),
                        CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS);
  }
}

BOOST_AUTO_TEST_SUITE_END()

