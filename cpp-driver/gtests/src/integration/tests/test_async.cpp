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

#include "integration.hpp"

#define NUMBER_OF_CONCURRENT_REQUESTS 4096u

/**
 * Asynchronous integration tests
 */
class AsyncTests : public Integration {
public:
  void SetUp() {
    // Call the parent setup function
    Integration::SetUp();

    // Create the table
    session_.execute("CREATE TABLE " + table_name_ +
                     " (key timeuuid PRIMARY KEY, value_number int, value_text text)");
  }

  /**
   * Execute concurrent insert requests into the table
   *
   * @param futures Futures vector to store async futures
   * @return Time UUIDs generate for the insert keys
   */
  std::vector<TimeUuid> insert_async(Session session, std::vector<Future>* futures) {
    // Execute concurrent insert requests
    std::vector<TimeUuid> keys;
    for (size_t i = 0; i < NUMBER_OF_CONCURRENT_REQUESTS; ++i) {
      // Create the insert statement
      Statement insert("INSERT INTO " + table_name_ +
                           " (key, value_number, value_text) VALUES (?, ?, ?) IF NOT EXISTS",
                       3);

      // Bind the values to the insert statement
      TimeUuid key = uuid_generator_.generate_timeuuid();
      insert.bind<TimeUuid>(0, key);
      insert.bind<Integer>(1, Integer(i));
      insert.bind<Text>(2, Text(format_string("row-%d", i + 1)));

      // Execute the insert request asynchronously
      futures->push_back(session.execute_async(insert));
      keys.push_back(key);
    }

    // Return the generated time UUIDs
    return keys;
  }

  /**
   * Validate the asynchronous inserts into the table synchronously
   *
   * @param keys Time UUIDs keys generated for the inserts
   */
  void validate_async_inserts(const std::vector<TimeUuid>& keys) {
    // Select all the values from the table and validate
    Result result = session_.execute(default_select_all());
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_EQ(NUMBER_OF_CONCURRENT_REQUESTS, result.row_count());
    ASSERT_EQ(3u, result.column_count());
    Rows rows = result.rows();
    size_t number_of_rows = 0;
    for (size_t i = 0; i < rows.row_count(); ++i) {
      Row row = rows.next();
      TimeUuid key = row.next().as<TimeUuid>();
      ASSERT_TRUE(std::find(keys.begin(), keys.end(), key) != keys.end());
      ++number_of_rows;
    }
    ASSERT_EQ(NUMBER_OF_CONCURRENT_REQUESTS, number_of_rows);
  }
};

/**
 * Perform asynchronous inserts and validate operations completed successfully
 *
 * This test will perform multiple concurrent inserts using a simple statement
 * and ensure all the values were inserted into the table against a single node
 * cluster.
 *
 * @test_category queries:async
 * @since core:1.0.0
 * @expected_result Asynchronous inserts complete and are validated
 */
CASSANDRA_INTEGRATION_TEST_F(AsyncTests, Simple) {
  CHECK_FAILURE;

  // Insert rows asynchronously and gather the futures and keys
  std::vector<Future> futures;
  std::vector<TimeUuid> keys = insert_async(session_, &futures);

  // Wait on all futures to complete and validate the results
  for (std::vector<Future>::iterator it = futures.begin(); it != futures.end(); ++it) {
    it->wait_timed();
  }
  validate_async_inserts(keys);
}

/**
 * Perform asynchronous inserts and validate operations completed successfully
 * while prematurely closing the session
 *
 * This test will perform multiple concurrent inserts using a simple statement
 * and ensure all the values were inserted into the table against a single node
 * cluster while the session was close before the asynchronous operations were
 * able to complete (e.g. not waiting on the futures)
 *
 * @test_category queries:async
 * @since core:1.0.0
 * @expected_result Asynchronous inserts complete and are validated
 */
CASSANDRA_INTEGRATION_TEST_F(AsyncTests, Close) {
  CHECK_FAILURE;

  std::vector<TimeUuid> keys;
  {
    // Create a temporary session for prematurely closing during async inserts
    Session session = cluster_.connect(keyspace_name_);

    // Insert rows asynchronously and gather the futures and keys
    std::vector<Future> futures;
    keys = insert_async(session, &futures);
  } // Kills the session (scope)

  // Validate the results; pending requests should finish
  validate_async_inserts(keys);
}
