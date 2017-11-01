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

#include <algorithm>
#include <map>
#include <string>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>

#include "cassandra.h"
#include "constants.hpp"
#include "test_utils.hpp"

/**
 * Test harness for set keyspace functionality.
 */
struct SetKeyspaceTests : public test_utils::SingleSessionTest {
  /**
   * Construct two differnt keyspaces to validate that the statement/batch
   * keyspace is be use for the queries instead of the session keyspace
   */
  SetKeyspaceTests()
    : SingleSessionTest(1, 0)
    , keyspace(str(boost::format("ks_%s") % test_utils::generate_unique_str(uuid_gen)))
    , keyspace2(str(boost::format("ks_%s") % test_utils::generate_unique_str(uuid_gen)))  {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % keyspace
                                           % "1"));
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % keyspace2
                                           % "1"));
    create_table();
    cass_cluster_set_use_beta_protocol_version(cluster, cass_true);
  }

  /**
   * Create table schema.
   */
  void create_table() {
    test_utils::execute_query(session, str(boost::format("CREATE TABLE %s.test (k text PRIMARY KEY, v text)")
                                           % keyspace2));
    test_utils::execute_query(session, str(boost::format("INSERT INTO %s.test (k, v) VALUES ('key1', 'value1')")
                                           % keyspace2));
  }

  /**
   * Drop table schema.
   */
  void drop_table() {
    test_utils::execute_query(session, str(boost::format("DROP TABLE %s.test")
                                           % keyspace2));
  }

  /**
   * Run a SELECT query using a simple statement and a session connected with
   * the provided keyspace. The result of the query is validated.
   *
   * @param session_keyspace The sessions's connected keyspace
   */
  void query_with_keyspace(const std::string& session_keyspace) {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    if (!session_keyspace.empty()) {
      test_utils::execute_query(session.get(), str(boost::format("USE %s") % session_keyspace));
    }

    test_utils::CassStatementPtr statement(cass_statement_new("SELECT v FROM test WHERE k = 'key1'", 0));

    cass_statement_set_keyspace(statement.get(), keyspace2.c_str());

    validate_query_result(test_utils::CassFuturePtr(cass_session_execute(session.get(), statement.get())));
  }

  /**
   * Run a SELECT query using a prepared statement and a session connected with
   * the provided keyspace. The result of the query if validated.
   *
   * @param session_keyspace The sessions's connected keyspace
   */
  void prepared_query_with_keyspace(const std::string& session_keyspace) {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    if (!session_keyspace.empty()) {
      test_utils::execute_query(session.get(), str(boost::format("USE %s") % session_keyspace));
    }

    test_utils::CassStatementPtr statement(prepare(session, "SELECT v FROM test WHERE k = 'key1'", keyspace2));

    validate_query_result(test_utils::CassFuturePtr(cass_session_execute(session.get(), statement.get())));
  }

  /**
   * Run INSERT statements using a batch and as session connected with the
   * provided keyspace. A SELECT query is used to validate the results of
   * the batch.
   *
   * @param session_keyspace The sessions's connected keyspace
   */
  void batch_query_with_keyspace(const std::string& session_keyspace) {
    test_utils::CassSessionPtr session(test_utils::create_session(cluster));

    test_utils::CassStatementPtr statement1(cass_statement_new("INSERT INTO test (k, v) VALUES ('key2', 'value2')", 0));
    test_utils::CassStatementPtr statement2(cass_statement_new("INSERT INTO test (k, v) VALUES ('key3', 'value3')", 0));

    test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));

    cass_batch_set_keyspace(batch.get(), keyspace2.c_str());

    cass_batch_add_statement(batch.get(), statement1.get());
    cass_batch_add_statement(batch.get(), statement2.get());

    test_utils::CassFuturePtr future(cass_session_execute_batch(session.get(), batch.get()));
    BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

    validate_batch_results();
  }

  /**
   * Prepare and bind a statement with provided query and keyspace.
   *
   * @param session The session to use perpare the query
   * @param query The query to prepare
   * @param keyspace  The keyspace used to prepare the query (optional)
   * @return A bound statement for the prepared query
   */
  test_utils::CassStatementPtr prepare(test_utils::CassSessionPtr session,
                                       const std::string& query,
                                       const std::string& keyspace = std::string()) {
    test_utils::CassStatementPtr existing_statement(cass_statement_new(query.c_str(), 0));

    if (!keyspace.empty()) {
      cass_statement_set_keyspace(existing_statement.get(), keyspace.c_str());
    }

    test_utils::CassFuturePtr future(cass_session_prepare_from_existing(session.get(), existing_statement.get()));
    BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

    test_utils::CassPreparedPtr prepared(cass_future_get_prepared(future.get()));
    BOOST_REQUIRE(prepared);

    test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
    BOOST_REQUIRE(statement);

    return statement;
  }

  /**
   * Validate the result of the provided future.
   *
   * @param future The result future to validate
   */
  void validate_query_result(test_utils::CassFuturePtr future) {
    BOOST_REQUIRE(future);
    BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

    const CassResult* result = cass_future_get_result(future.get());
    BOOST_REQUIRE(result != NULL);
    BOOST_REQUIRE_EQUAL(cass_result_row_count(result), 1u);
    BOOST_REQUIRE_EQUAL(cass_result_column_count(result), 1u);

    const CassRow* row = cass_result_first_row(result);
    BOOST_REQUIRE(row != NULL);

    const char* value;
    size_t value_length;
    BOOST_REQUIRE_EQUAL(cass_value_get_string(cass_row_get_column(row, 0), &value, &value_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(value, value_length), "value1");
  }

  /**
   * Run a SELECT query and validate the results of a batch test.
   */
  void validate_batch_results() {
    test_utils::CassResultPtr result;
    test_utils::execute_query(session, str(boost::format("SELECT k, v FROM %s.test") % keyspace2), &result);
    BOOST_REQUIRE(result);

    BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 3u);
    BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 2u);

    std::map<std::string, std::string> actual, expected;

    expected["key1"] = "value1";
    expected["key2"] = "value2";
    expected["key3"] = "value3";

    test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));
    while (cass_iterator_next(iterator.get())) {
      const CassRow* row = cass_iterator_get_row(iterator.get());
      BOOST_REQUIRE(row != NULL);

      const char* key;
      size_t key_length;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(cass_row_get_column(row, 0), &key, &key_length), CASS_OK);

      const char* value;
      size_t value_length;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(cass_row_get_column(row, 1), &value, &value_length), CASS_OK);

      actual[std::string(key, key_length)] = std::string(value, value_length);
    }

    BOOST_CHECK(actual == expected);
  }

  /**
   * Session keyspace
   */
  std::string keyspace;

  /**
   * Statement keyspace
   */
  std::string keyspace2;

};

BOOST_FIXTURE_TEST_SUITE(set_keyspace, SetKeyspaceTests)

/**
 * Verify that older protocols don't attempt to send a statement's keyspace.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(query_not_supported_by_older_protocol) {
  BOOST_CHECK_EQUAL(cass_cluster_set_use_beta_protocol_version(cluster, cass_false), CASS_OK);
  BOOST_CHECK_EQUAL(cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4), CASS_OK);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  test_utils::CassStatementPtr statement(cass_statement_new("SELECT v FROM test WHERE k = 'key1'", 0));

  // Attempt to set the keyspace with an older protocol
  cass_statement_set_keyspace(statement.get(), keyspace2.c_str());

  test_utils::CassFuturePtr future(cass_session_execute(session.get(), statement.get()));
  BOOST_CHECK_EQUAL(cass_future_error_code(future.get()), CASS_ERROR_SERVER_INVALID_QUERY);
}

/**
 * Verify that a simples statement's keyspace is used when no session keyspace
 * is set.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(query_with_no_session_keyspace) {
  if (!check_version("4.0.0")) return;
  query_with_keyspace("");
}

/**
 * Verify that a simple statement's keyspace is used when a different session
 * keyspace is set.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(query_with_different_session_keyspace) {
  if (!check_version("4.0.0")) return;
  query_with_keyspace(keyspace);
}

/**
 * Verify that a simple statement's keyspace is used when the same session
 * keyspace is set.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(query_with_same_session_keyspace) {
  if (!check_version("4.0.0")) return;
  query_with_keyspace(keyspace2);
}

/**
 * Verify that older protocols don't attempt to send a prepared statement's
 * keyspace.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(prepared_not_supported_by_older_protocol) {
  BOOST_CHECK_EQUAL(cass_cluster_set_use_beta_protocol_version(cluster, cass_false), CASS_OK);
  BOOST_CHECK_EQUAL(cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4), CASS_OK);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  test_utils::CassStatementPtr statement(cass_statement_new("SELECT v FROM test WHERE k = 'key1'", 0));

  // Attempt to set the keyspace with an older protocol
  cass_statement_set_keyspace(statement.get(), keyspace2.c_str());

  test_utils::CassFuturePtr future(cass_session_prepare_from_existing(session.get(), statement.get()));
  BOOST_CHECK_EQUAL(cass_future_error_code(future.get()), CASS_ERROR_SERVER_INVALID_QUERY);
}

/**
 * Verify that a re-prepare (as a result of a UNPREPARED response) correctly
 * prepares the statement with the original keyspace.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(prepared_should_reprepare_with_the_same_keyspace) {
  if (!check_version("4.0.0")) return;

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  test_utils::CassStatementPtr statement(cass_statement_new("SELECT v FROM test WHERE k = 'key1'", 0));

  // Attempt to set the keyspace with an older protocol
  cass_statement_set_keyspace(statement.get(), keyspace2.c_str());

  test_utils::CassFuturePtr future(cass_session_prepare_from_existing(session.get(), statement.get()));
  BOOST_CHECK_EQUAL(cass_future_error_code(future.get()), CASS_OK);

  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(future.get()));
  BOOST_CHECK(prepared);

  // Force the statement to be reprepared
  test_utils::execute_query(session.get(), "TRUNCATE system.prepared_statements"); // Required for 3.10+ (CASSANDRA-8831)
  drop_table();
  create_table();

  // Check to see if the statement reprepared with the correct keyspace
  test_utils::CassStatementPtr bound_statement(cass_prepared_bind(prepared.get()));
  BOOST_CHECK(bound_statement);

  validate_query_result(test_utils::CassFuturePtr(cass_session_execute(session.get(), bound_statement.get())));
}

/**
 * Verify that a prepared statement's keyspace is used when no session keyspace
 * is set.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(prepared_with_no_session_keyspace) {
  if (!check_version("4.0.0")) return;
  prepared_query_with_keyspace("");
}

/**
 * Verify that a prepared statement's keyspace is used when a different keyspace
 * is set.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(prepared_with_different_session_keyspace) {
  if (!check_version("4.0.0")) return;
  prepared_query_with_keyspace(keyspace);
}

/**
 * Verify that a prepared statement's keyspace is used when the same keyspace
 * is set.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(prepared_with_same_session_keyspace) {
  if (!check_version("4.0.0")) return;
  prepared_query_with_keyspace(keyspace2);
}

/**
 * Verify that a batch determines its keyspace from one the first simple
 * statement with a non-empty keyspace.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(batch_with_keyspace_from_simple_statement) {
  if (!check_version("4.0.0")) return;

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  test_utils::CassStatementPtr statement1(cass_statement_new("INSERT INTO test (k, v) VALUES ('key2', 'value2')", 0));
  test_utils::CassStatementPtr statement2(cass_statement_new("INSERT INTO test (k, v) VALUES ('key3', 'value3')", 0));

  // The batch should get the keyspace from the second statement
  cass_statement_set_keyspace(statement2.get(), keyspace2.c_str());

  test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));

  cass_batch_add_statement(batch.get(), statement1.get());
  cass_batch_add_statement(batch.get(), statement2.get());

  test_utils::CassFuturePtr future(cass_session_execute_batch(session.get(), batch.get()));
  BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

  validate_batch_results();
}

/**
 * Verify that a batch determines its keyspace from one the first prepared
 * statement with a non-empty keyspace.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(batch_with_keyspace_from_prepared) {
  if (!check_version("4.0.0")) return;

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  test_utils::CassStatementPtr statement1(cass_statement_new("INSERT INTO test (k, v) VALUES ('key2', 'value2')", 0));

  // The batch should get the keyspace from the second statement
  test_utils::CassStatementPtr statement2(prepare(session, "INSERT INTO test (k, v) VALUES ('key3', 'value3')", keyspace2));

  test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));

  cass_batch_add_statement(batch.get(), statement1.get());
  cass_batch_add_statement(batch.get(), statement2.get());

  test_utils::CassFuturePtr future(cass_session_execute_batch(session.get(), batch.get()));
  BOOST_REQUIRE_EQUAL(cass_future_error_code(future.get()), CASS_OK);

  validate_batch_results();
}

/**
 * Verify that the batch-level keyspace is not sent when using older protocols.
 *
 * @since 2.8
 * @test_category basic
 *
 */
BOOST_AUTO_TEST_CASE(batch_not_supported_by_older_protocol) {
  BOOST_CHECK_EQUAL(cass_cluster_set_use_beta_protocol_version(cluster, cass_false), CASS_OK);
  BOOST_CHECK_EQUAL(cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4), CASS_OK);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster));

  test_utils::CassStatementPtr statement1(cass_statement_new("INSERT INTO test (k, v) VALUES ('key2', 'value2')", 0));
  test_utils::CassStatementPtr statement2(cass_statement_new("INSERT INTO test (k, v) VALUES ('key3', 'value3')", 0));

  test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));

  cass_batch_set_keyspace(batch.get(), keyspace2.c_str());

  cass_batch_add_statement(batch.get(), statement1.get());
  cass_batch_add_statement(batch.get(), statement2.get());

  test_utils::CassFuturePtr future(cass_session_execute_batch(session.get(), batch.get()));
  BOOST_CHECK_EQUAL(cass_future_error_code(future.get()), CASS_ERROR_SERVER_INVALID_QUERY);
}

/**
 * Verify that a batch's keyspace is used when no session keyspace is set.
 *
 * @since 2.8
 * @test_category basic
 */
BOOST_AUTO_TEST_CASE(batch_with_no_session_keyspace) {
  if (!check_version("4.0.0")) return;
  batch_query_with_keyspace("");
}

/**
 * Verify that a batch's keyspace is used when a different session keyspace
 * is set.
 *
 * @since 2.8
 * @test_category basic
 */
BOOST_AUTO_TEST_CASE(batch_with_different_session_keyspace) {
  if (!check_version("4.0.0")) return;
  batch_query_with_keyspace(keyspace);
}

/**
 * Verify that a batch's keyspace is used when then same session keyspace
 * is set.
 *
 * @since 2.8
 * @test_category basic
 */
BOOST_AUTO_TEST_CASE(batch_with_same_session_keyspace) {
  if (!check_version("4.0.0")) return;
  batch_query_with_keyspace(keyspace2);
}

BOOST_AUTO_TEST_SUITE_END()
