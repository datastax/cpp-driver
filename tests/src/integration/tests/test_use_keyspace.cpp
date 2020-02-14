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

#include <locale>

/**
 * "USE <keyspace>" case-sensitive tests
 */
class UseKeyspaceCaseSensitiveTests : public Integration {
public:
  UseKeyspaceCaseSensitiveTests() {}

  // Make a case-sensitive keyspace capitalizing the first char and wrapping in double quotes
  virtual std::string default_keyspace() {
    std::string temp(Integration::default_keyspace());
    temp[0] = std::toupper(temp[0]);
    return "\"" + temp + "\"";
  }

  virtual void SetUp() {
    Integration::SetUp();
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "1", "2"));
  }
};

/**
 * Verify that case-sensitive keyspaces work when connecting a session with a keyspace.
 */
CASSANDRA_INTEGRATION_TEST_F(UseKeyspaceCaseSensitiveTests, ConnectWithKeyspace) {
  CHECK_FAILURE;
  Session session = default_cluster().connect(keyspace_name_);

  Result result =
      session.execute(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "1"));

  Row row = result.first_row();
  EXPECT_EQ(row.column_by_name<Integer>("value"), Integer(2));
}

/**
 * Verify that case-sensitive keyspaces work with "USE <keyspace>".
 */
CASSANDRA_INTEGRATION_TEST_F(UseKeyspaceCaseSensitiveTests, UseKeyspace) {
  CHECK_FAILURE;
  Session session = default_cluster().connect();

  { // Expect failure there's no keyspace set
    Result result =
        session.execute(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "1"),
                        CASS_CONSISTENCY_ONE, false, false);

    EXPECT_EQ(result.error_code(), CASS_ERROR_SERVER_INVALID_QUERY);
  }

  session.execute("USE " + keyspace_name_);

  { // Success
    Result result =
        session.execute(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "1"));

    Row row = result.first_row();
    EXPECT_EQ(row.column_by_name<Integer>("value"), Integer(2));
  }
}
