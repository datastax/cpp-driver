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

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/chrono.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "execute_request.hpp"
#include "statement.hpp"
#include "test_utils.hpp"
#include "testing.hpp"

/**
 * Test harness for prepared statements in batches.
 */
struct PreparedBatchTests : public test_utils::SingleSessionTest {
  PreparedBatchTests()
    : SingleSessionTest(1, 0)
    , keyspace(str(boost::format("ks_%s") % test_utils::generate_unique_str(uuid_gen))) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % keyspace
                                           % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % keyspace));
    test_utils::execute_query(session, "CREATE TABLE test (k text PRIMARY KEY, v text)");
  }

  /**
   * Wait for a session to reconnect to a node.
   *
   * @param session The session to use for waiting
   * @param node The node to wait for
   */
  void wait_for_node(test_utils::CassSessionPtr session, int node) {
    std::stringstream ip_address;
    ip_address << ccm->get_ip_prefix() << node;

    for (int i = 0; i < 30; ++i) {
      test_utils::CassStatementPtr statement(cass_statement_new("SELECT * FROM system.peers", 0));
      test_utils::CassFuturePtr future(cass_session_execute(session.get(),
                                                            statement.get()));
      std::string host(cass::get_host_from_future(future.get()).c_str());
      if (cass_future_error_code(future.get()) == CASS_OK && host == ip_address.str()) {
        return;
      }
      boost::this_thread::sleep_for(boost::chrono::seconds(1));
    }
    BOOST_REQUIRE_MESSAGE(false,
                          "Failed to wait for node " <<
                          ip_address.str() <<
                          " to become availble");
  }

  /**
   * Validate that a given key and value have been added to the test table.
   *
   * @param session The session to use
   * @param key The key to validate
   * @param expected_value The expected value for the given key
   */
  void validate_result(test_utils::CassSessionPtr session,
                       const std::string& key,
                       const std::string& expected_value) {
    test_utils::CassResultPtr result;
    test_utils::execute_query(session.get(),
                              str(boost::format("SELECT * FROM test WHERE k = '%s'") % key),
                              &result);
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1);
    const CassRow* row = cass_result_first_row(result.get());
    BOOST_REQUIRE(row != NULL);

    const CassValue* column = cass_row_get_column_by_name(row, "v");
    BOOST_REQUIRE(column != NULL);

    const char* value;
    size_t value_length;
    BOOST_REQUIRE_EQUAL(cass_value_get_string(column, &value, &value_length), CASS_OK);

    BOOST_CHECK_EQUAL(std::string(value, value_length), expected_value);
  }

  /**
   * The test's keyspace
   */
  std::string keyspace;
};

BOOST_FIXTURE_TEST_SUITE(prepared_batch, PreparedBatchTests)

/**
 * Verify that a statement in a batch is properly reprepared.
 *
 * @since 2.8
 * @test_category batch
 *
 */
BOOST_AUTO_TEST_CASE(reprepare_batch) {
  cass_cluster_set_prepare_on_up_or_add_host(cluster, cass_false);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));
  test_utils::execute_query(session.get(), str(boost::format("USE %s") % keyspace));

  test_utils::CassFuturePtr prepare_future(cass_session_prepare(session.get(), "INSERT INTO test (k, v) VALUES (?, ?)"));
  test_utils::wait_and_check_error(prepare_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepare_future.get()));

  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));

  { // Run batch and validate value
    cass_statement_bind_string_by_name(statement.get(), "k", "key1");
    cass_statement_bind_string_by_name(statement.get(), "v", "value1");

    test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));
    cass_batch_add_statement(batch.get(), statement.get());

    test_utils::CassFuturePtr batch_future(cass_session_execute_batch(session.get(), batch.get()));
    test_utils::wait_and_check_error(batch_future.get());

    validate_result(session, "key1", "value1");
  }

  // Ensure that a reprepare happens
  if (version >= "3.10") {
    test_utils::execute_query(session.get(), "TRUNCATE TABLE system.prepared_statements");
  }

  ccm->stop_node(1);
  ccm->start_node(1);

  wait_for_node(session, 1);

  { // Rerun the batch and validate a new value and key
    cass_statement_bind_string_by_name(statement.get(), "k", "key2");
    cass_statement_bind_string_by_name(statement.get(), "v", "value2");

    test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));
    cass_batch_add_statement(batch.get(), statement.get());

    test_utils::CassFuturePtr batch_future(cass_session_execute_batch(session.get(), batch.get()));
    test_utils::wait_and_check_error(batch_future.get());

    validate_result(session, "key2", "value2");
  }
}

BOOST_AUTO_TEST_SUITE_END()
