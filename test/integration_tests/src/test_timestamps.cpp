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

#include "test_utils.hpp"

#include "cassandra.h"
#include "timestamp_generator.hpp"
#include "get_time.hpp"

#include <boost/format.hpp>
#include <boost/test/unit_test.hpp>

struct TimestampsTest : public test_utils::SingleSessionTest {
  typedef test_utils::CassBatchPtr CassBatchPtr;
  typedef test_utils::CassFuturePtr CassFuturePtr;
  typedef test_utils::CassStatementPtr CassStatementPtr;

  TimestampsTest()
    : SingleSessionTest(1, 0) {
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
  }

  ~TimestampsTest() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(session,
      str(boost::format(test_utils::DROP_KEYSPACE_FORMAT)
      % test_utils::SIMPLE_KEYSPACE));
  }

  CassStatementPtr create_insert_statement(const std::string table_name) {
    std::string query(str(boost::format("INSERT INTO %s (key, value) VALUES (?, ?)") % table_name));
    return CassStatementPtr(cass_statement_new(query.c_str(), 2));
  }

  cass_int64_t get_timestamp(const std::string table_name, const std::string& key) {
    cass_int64_t timestamp;
    test_utils::CassResultPtr result;
    std::string query = str(boost::format("SELECT writetime(value) FROM %s WHERE key = '%s'") % table_name % key);
    test_utils::execute_query(session, query, &result);
    BOOST_REQUIRE(cass_result_row_count(result.get()) > 0);
    BOOST_REQUIRE(cass_result_column_count(result.get()) > 0);
    const CassRow* row = cass_result_first_row(result.get());
    const CassValue* writetime = cass_row_get_column(row, 0);
    cass_value_get_int64(writetime, &timestamp);
    return timestamp;
  }
};

class TestTimestampGenerator : public cass::TimestampGenerator {
public:
  TestTimestampGenerator(int64_t timestamp)
    : TimestampGenerator(SERVER_SIDE) // Doesn't matter for this test
    , timestamp_(timestamp) { }

  virtual int64_t next() { return timestamp_; }

private:
  int64_t timestamp_;
};

BOOST_AUTO_TEST_SUITE(timestamps)

/**
 * Set timestamp directly on statement and batch
 *
 * Verifies that the timestamp set on a statement/batch is sent to the server.
 *
 * @since 2.1.0
 * @jira_ticket CPP-266
 * @test_category queries:timestamp
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(statement_and_batch)
{
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    TimestampsTest tester;
    std::string table_name("table_" + test_utils::generate_unique_str(tester.uuid_gen));
    std::string create_table = "CREATE TABLE " + table_name + "(key text PRIMARY KEY, value text)";
    test_utils::execute_query(tester.session, create_table.c_str());

    // Statement
    {
      std::string key(test_utils::generate_unique_str(tester.uuid_gen));
      test_utils::CassStatementPtr statement(tester.create_insert_statement(table_name));
      cass_statement_bind_string(statement.get(), 0, key.c_str());
      cass_statement_bind_string(statement.get(), 1, key.c_str());

      // Set the timestamp to a known value
      cass_statement_set_timestamp(statement.get(), 1234);

      test_utils::CassFuturePtr future(cass_session_execute(tester.session, statement.get()));
      BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);

      BOOST_CHECK_EQUAL(tester.get_timestamp(table_name, key), 1234);
    }

    // Batch
    {
      test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));

      std::string key1(test_utils::generate_unique_str(tester.uuid_gen));
      test_utils::CassStatementPtr statement1(tester.create_insert_statement(table_name));
      cass_statement_bind_string(statement1.get(), 0, key1.c_str());
      cass_statement_bind_string(statement1.get(), 1, key1.c_str());
      cass_batch_add_statement(batch.get(), statement1.get());

      std::string key2(test_utils::generate_unique_str(tester.uuid_gen));
      test_utils::CassStatementPtr statement2(tester.create_insert_statement(table_name));
      cass_statement_bind_string(statement2.get(), 0, key2.c_str());
      cass_statement_bind_string(statement2.get(), 1, key2.c_str());
      cass_batch_add_statement(batch.get(), statement2.get());

      // Set the timestamp to a known value
      cass_batch_set_timestamp(batch.get(), 1234);

      test_utils::CassFuturePtr future(cass_session_execute_batch(tester.session, batch.get()));
      BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);

      BOOST_CHECK_EQUAL(tester.get_timestamp(table_name, key1), 1234);
      BOOST_CHECK_EQUAL(tester.get_timestamp(table_name, key2), 1234);
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping timestamps/statement_and_batch" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 * Test timestamp generator
 *
 * Verifies that a timestamp generator is used when a statement's or batch's
 * timestamp is not set directly.
 *
 * @since 2.1.0
 * @jira_ticket CPP-266
 * @test_category queries:timestamp
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(generator)
{
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    TimestampsTest tester;
    cass::SharedRefPtr<TestTimestampGenerator> gen(new TestTimestampGenerator(1234));

    tester.close_session();
    cass_cluster_set_timestamp_gen(tester.cluster, CassTimestampGen::to(gen.get()));
    tester.create_session();

    test_utils::execute_query(tester.session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

    std::string table_name("table_" + test_utils::generate_unique_str(tester.uuid_gen));
    std::string create_table = "CREATE TABLE " + table_name + "(key text PRIMARY KEY, value text)";
    test_utils::execute_query(tester.session, create_table.c_str());

    // Statement
    {
      std::string table_name("table_" + test_utils::generate_unique_str(tester.uuid_gen));
      std::string create_table = "CREATE TABLE " + table_name + "(key text PRIMARY KEY, value text)";
      test_utils::execute_query(tester.session, create_table.c_str());

      std::string key(test_utils::generate_unique_str(tester.uuid_gen));
      test_utils::CassStatementPtr statement(tester.create_insert_statement(table_name));
      cass_statement_bind_string(statement.get(), 0, key.c_str());
      cass_statement_bind_string(statement.get(), 1, key.c_str());

      test_utils::CassFuturePtr future(cass_session_execute(tester.session, statement.get()));
      BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);

      BOOST_CHECK_EQUAL(tester.get_timestamp(table_name, key), 1234);
    }

    // Batch
    {
      test_utils::CassBatchPtr batch(cass_batch_new(CASS_BATCH_TYPE_LOGGED));

      std::string key1(test_utils::generate_unique_str(tester.uuid_gen));
      test_utils::CassStatementPtr statement1(tester.create_insert_statement(table_name));
      cass_statement_bind_string(statement1.get(), 0, key1.c_str());
      cass_statement_bind_string(statement1.get(), 1, key1.c_str());
      cass_batch_add_statement(batch.get(), statement1.get());

      std::string key2(test_utils::generate_unique_str(tester.uuid_gen));
      test_utils::CassStatementPtr statement2(tester.create_insert_statement(table_name));
      cass_statement_bind_string(statement2.get(), 0, key2.c_str());
      cass_statement_bind_string(statement2.get(), 1, key2.c_str());
      cass_batch_add_statement(batch.get(), statement2.get());

      test_utils::CassFuturePtr future(cass_session_execute_batch(tester.session, batch.get()));
      BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);

      BOOST_CHECK_EQUAL(tester.get_timestamp(table_name, key1), 1234);
      BOOST_CHECK_EQUAL(tester.get_timestamp(table_name, key2), 1234);
    }
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping timestamps/generator" << std::endl;
    BOOST_REQUIRE(true);
  }
}

/**
 * Test the default timestamp generator.
 *
 * Verifies that the timestamp is set by the server when no timestamp
 * generator is set and the timestamp is not set directly on the statement.
 *
 * @since 2.1.0
 * @jira_ticket CPP-266
 * @test_category queries:timestamp
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(server_side)
{
  CCM::CassVersion version = test_utils::get_version();
  if ((version.major_version >= 2 && version.minor_version >= 1) || version.major_version >= 3) {
    TimestampsTest tester;
    // Server-side is the default timestamp generator
    std::string table_name("table_" + test_utils::generate_unique_str(tester.uuid_gen));
    std::string create_table = "CREATE TABLE " + table_name + "(key text PRIMARY KEY, value text)";
    test_utils::execute_query(tester.session, create_table.c_str());

    std::string key(test_utils::generate_unique_str(tester.uuid_gen));
    test_utils::CassStatementPtr statement(tester.create_insert_statement(table_name));
    cass_statement_bind_string(statement.get(), 0, key.c_str());
    cass_statement_bind_string(statement.get(), 1, key.c_str());

    uint64_t timestamp = cass::get_time_since_epoch_ms();
    test_utils::CassFuturePtr future(cass_session_execute(tester.session, statement.get()));
    BOOST_REQUIRE(cass_future_error_code(future.get()) == CASS_OK);

    BOOST_CHECK_GE(tester.get_timestamp(table_name, key), static_cast<int64_t>(timestamp));
  } else {
    std::cout << "Unsupported Test for Cassandra v" << version.to_string() << ": Skipping timestamps/server_side" << std::endl;
    BOOST_REQUIRE(true);
  }
}

BOOST_AUTO_TEST_SUITE_END()
