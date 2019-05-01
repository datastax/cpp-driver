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

#include <string>

#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/test/debug.hpp>
#include <boost/test/unit_test.hpp>

#include "cassandra.h"
#include "execute_request.hpp"
#include "statement.hpp"
#include "test_utils.hpp"

/**
 * Test harness for prepared from existing functionality.
 */
struct PreparedMetadataTests : public test_utils::SingleSessionTest {
  PreparedMetadataTests()
      : SingleSessionTest(1, 0)
      , keyspace(str(boost::format("ks_%s") % test_utils::generate_unique_str(uuid_gen))) {
    test_utils::execute_query(
        session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % keyspace % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % keyspace));
    test_utils::execute_query(session, "CREATE TABLE test (k text PRIMARY KEY, v text)");
    test_utils::execute_query(session, "INSERT INTO test (k, v) VALUES ('key1', 'value1')");
  }

  /**
   * Check the column count of a bound statement before and after adding a
   * column to a table.
   *
   * @param expected_column_count_after_update
   */
  void prepared_check_column_count_after_alter(size_t expected_column_count_after_update) {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    test_utils::execute_query(session.get(), str(boost::format("USE %s") % keyspace));

    test_utils::CassFuturePtr future(
        cass_session_prepare(session.get(), "SELECT * FROM test WHERE k = 'key1'"));
    BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

    test_utils::CassPreparedPtr prepared(cass_future_get_prepared(future.get()));
    BOOST_REQUIRE(prepared);

    test_utils::CassStatementPtr bound_statement(cass_prepared_bind(prepared.get()));
    BOOST_REQUIRE(bound_statement);

    // Verify that the table has two columns in the metadata
    {
      test_utils::CassFuturePtr result_future(
          cass_session_execute(session.get(), bound_statement.get()));
      BOOST_REQUIRE_EQUAL(cass_future_error_code(result_future.get()), CASS_OK);

      test_utils::CassResultPtr result(cass_future_get_result(result_future.get()));

      BOOST_CHECK_EQUAL(cass_result_column_count(result.get()), 2u);
    }

    // Add a column to the table
    test_utils::execute_query(session.get(), "ALTER TABLE test ADD v2 int");

    // The column count shouldn't have changed
    {
      test_utils::CassFuturePtr result_future(
          cass_session_execute(session.get(), bound_statement.get()));
      BOOST_REQUIRE_EQUAL(cass_future_error_code(result_future.get()), CASS_OK);

      test_utils::CassResultPtr result(cass_future_get_result(result_future.get()));

      BOOST_CHECK_EQUAL(cass_result_column_count(result.get()), expected_column_count_after_update);
    }
  }

  /**
   * The test's keyspace
   */
  std::string keyspace;
};

BOOST_FIXTURE_TEST_SUITE(prepared_metadata, PreparedMetadataTests)

/**
 * Verify that the column count of a bound statement's result metadata doesn't
 * change for older protocol versions (v4 and less) when a table's schema is altered.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(alter_doesnt_update_column_count) {
  cass_cluster_set_use_beta_protocol_version(cluster,
                                             cass_false); // Ensure beta protocol is not set
  BOOST_REQUIRE_EQUAL(cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4),
                      CASS_OK);

  // The column count will stay the same even after the alter
  prepared_check_column_count_after_alter(2u);
}

/**
 * Verify that the column count of a bound statement's result metadata is
 * properly updated for newer protocol versions (v5 and greater) when a table's
 * schema is altered.
 *
 * @since 2.8
 * @test_category prepared
 *
 */
BOOST_AUTO_TEST_CASE(alter_properly_updates_column_count) {
  if (!check_version("4.0.0")) return;

  // Ensure protocol v5 or greater
  BOOST_REQUIRE_EQUAL(cass_cluster_set_use_beta_protocol_version(cluster, cass_true), CASS_OK);

  // The column count will properly update after the alter
  prepared_check_column_count_after_alter(3u);
}

BOOST_AUTO_TEST_SUITE_END()
