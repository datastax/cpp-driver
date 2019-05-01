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

class TracingTests : public Integration {};

CASSANDRA_INTEGRATION_TEST_F(TracingTests, Simple) {
  CHECK_FAILURE;

  Uuid tracing_id;

  { // Get tracing ID.
    Statement statement("SELECT release_version FROM system.local");
    statement.set_tracing(true);
    Result result = session_.execute(statement);
    tracing_id = result.tracing_id();
    ASSERT_FALSE(tracing_id.is_null());
  }

  { // Validate tracing ID by attempting to get the associated tracing data.
    Statement statement("SELECT * FROM system_traces.sessions WHERE session_id = ?", 1);
    statement.bind(0, tracing_id);
    Result result = session_.execute(statement);
    ASSERT_GT(result.row_count(), 0u);
    Uuid session_id = result.first_row().column_by_name<Uuid>("session_id");
    ASSERT_FALSE(session_id.is_null());
    EXPECT_EQ(tracing_id, session_id);
  }
}
