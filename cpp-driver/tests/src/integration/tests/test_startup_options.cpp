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
 * Startup options integration tests
 */
class StartupOptionsTests : public Integration {};

/**
 * Verify driver name and version are assigned in startup options.
 *
 * This test will select the driver name and version from the
 * 'system_views.clients' table to validate that the driver options are
 * assigned when connection to the server.
 *
 * @test_category configuration
 * @test_category connection
 * @since core:2.11.0
 * @cassandra_version 4.0.0
 * @expected_result Driver startup options are validated.
 */
CASSANDRA_INTEGRATION_TEST_F(StartupOptionsTests, DriverOptions) {
  CHECK_FAILURE;
  CHECK_VERSION(4.0.0);
  if (!Options::is_cassandra()) {
    SKIP_TEST("Unsupported for DataStax Enterprise Version "
              << server_version_.to_string() << ": 'system_views.clients' is unavailable");
  }

  Result result = session_.execute("SELECT driver_name, driver_version from system_views.clients");
  ASSERT_EQ(2u, result.row_count()); // Control connection and request processor connection
  ASSERT_EQ(2u, result.column_count());
  Row row = result.first_row();
  ASSERT_EQ(Varchar(driver_name()), row.next().as<Varchar>());
  ASSERT_EQ(Varchar(driver_version()), row.next().as<Varchar>());
}
