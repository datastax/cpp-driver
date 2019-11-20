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

class LoggingTests : public Integration {
public:
  LoggingTests() { is_ccm_requested_ = false; }

  static void log(const CassLogMessage* log, void* data) {
    bool* is_triggered = static_cast<bool*>(data);
    *is_triggered = true;
  }
};

/**
 * Ensure the driver is calling the client logging callback
 */
CASSANDRA_INTEGRATION_TEST_F(LoggingTests, Callback) {
  CHECK_FAILURE;

  bool is_triggered = false;
  cass_log_set_callback(LoggingTests::log, &is_triggered);
  default_cluster().connect("", false);
  EXPECT_TRUE(is_triggered);
}