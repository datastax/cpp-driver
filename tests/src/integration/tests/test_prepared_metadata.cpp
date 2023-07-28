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

/**
 * Prepared metadata related tests
 */
class PreparedMetadataTests : public Integration {
public:
  void SetUp() {
    Integration::SetUp();
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "1", "99"));
  }

  /**
   * Check the column count of a bound statement before and after adding a
   * column to a table.
   *
   * @param session
   * @param expected_column_count_after_update
   */
  void prepared_check_column_count_after_alter(Session session,
                                               size_t expected_column_count_after_update) {
    Statement bound_statement =
        session.prepare(format_string("SELECT * FROM %s WHERE key = 1", table_name_.c_str()))
            .bind();

    // Verify that the table has two columns in the metadata
    {
      Result result = session.execute(bound_statement);
      EXPECT_EQ(2u, result.column_count());
    }

    // Add a column to the table
    session.execute(format_string("ALTER TABLE %s ADD value2 int", table_name_.c_str()));

    // The column count should match the expected after the alter
    {
      Result result = session.execute(bound_statement);
      EXPECT_EQ(expected_column_count_after_update, result.column_count());
    }
  }
};

/**
 * Verify that the column count of a bound statement's result metadata doesn't
 * change for older protocol versions (v4 and less) when a table's schema is altered.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(PreparedMetadataTests, AlterDoesntUpdateColumnCount) {
  CHECK_FAILURE;

  // Ensure beta protocol is not set
  Session session = default_cluster()
                        .with_protocol_version(CASS_PROTOCOL_VERSION_V4)
                        .connect(keyspace_name_);

  // The column count will stay the same even after the alter
  prepared_check_column_count_after_alter(session, 2u);
}

/**
 * Verify that the column count of a bound statement's result metadata is
 * properly updated for newer protocol versions (v5 and greater) when a table's
 * schema is altered.
 *
 * @since 2.8
 */
CASSANDRA_INTEGRATION_TEST_F(PreparedMetadataTests, AlterProperlyUpdatesColumnCount) {
  CHECK_FAILURE;
  CHECK_PROTOCOL_VERSION(CASS_PROTOCOL_VERSION_V5);

  // Ensure protocol v5 or greater
  Session session = default_cluster().connect(keyspace_name_);

  // The column count will properly update after the alter
  prepared_check_column_count_after_alter(session, 3u);
}
