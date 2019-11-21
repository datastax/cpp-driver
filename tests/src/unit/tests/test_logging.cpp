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

#include "unit.hpp"

#include "session.hpp"

using datastax::String;
using datastax::internal::get_time_monotonic_ns;
using datastax::internal::core::Address;
using datastax::internal::core::Config;
using datastax::internal::core::Future;
using datastax::internal::core::Session;
using datastax::internal::core::StringMultimap;
using mockssandra::Options;
using test::Utils;

class LoggingUnitTest : public Unit {
public:
  void TearDown() {
    EXPECT_TRUE(session_.close()->wait_for(WAIT_FOR_TIME));

    Unit::TearDown();
  }

  Future::Ptr connect_async(const Config& config = Config()) {
    Config temp(config);
    temp.contact_points().push_back(Address("127.0.0.1", 9042));
    return session_.connect(temp);
  }

  bool wait_for_logger(int expected_count) {
    // Wait for about 60 seconds
    for (int i = 0; i < 600 && logging_criteria_count() < expected_count; ++i) {
      Utils::msleep(100);
    }
    return logging_criteria_count() >= expected_count;
  }

private:
  Session session_;
};

/**
 * On a new control connection, logger message severity should be high (e.g.
 * CASS_LOG_ERROR).
 *
 * @jira_ticket CPP-337
 * @since 2.4.0
 */
TEST_F(LoggingUnitTest, ControlConnectionSeverityHigh) {
  add_logging_critera("Unable to establish a control connection to host 127.0.0.1", CASS_LOG_ERROR);
  Future::Ptr connect_future = connect_async();
  EXPECT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(1, logging_criteria_count());
}

/**
 * On a established control connection, logger message severity should be reduced
 * (e.g. CASS_LOG_WARN).
 *
 * @jira_ticket CPP-337
 * @since 2.4.0
 */
TEST_F(LoggingUnitTest, ControlConnectionSeverityReduced) {
  mockssandra::SimpleCluster cluster(simple());
  ASSERT_EQ(cluster.start_all(), 0);

  add_logging_critera("Lost control connection to host 127.0.0.1", CASS_LOG_WARN);
  Future::Ptr connect_future = connect_async();
  EXPECT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_EQ(0, logging_criteria_count());
  cluster.stop_all();
  EXPECT_TRUE(wait_for_logger(1));
}

/**
 * On a established connection the first connection pool logger message severity should be high
 * while subsequent messages should have reduced severity.
 *
 * @jira_ticket CPP-337
 * @since 2.4.0
 */
TEST_F(LoggingUnitTest, ConnectionPoolSeverityReduced) {
  mockssandra::SimpleCluster cluster(simple(), 2);
  ASSERT_EQ(cluster.start(1), 0);

  add_logging_critera(
      "Connection pool was unable to connect to host 127.0.0.2 because of the following error",
      CASS_LOG_ERROR);
  Config config;
  config.set_connection_heartbeat_interval_secs(1);
  config.set_connection_idle_timeout_secs(1);
  config.set_request_timeout(1000);
  config.set_constant_reconnect(100);
  Future::Ptr connect_future = connect_async(config);
  EXPECT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  EXPECT_TRUE(wait_for_logger(1));

  reset_logging_criteria();
  add_logging_critera(
      "Connection pool was unable to reconnect to host 127.0.0.2 because of the following error",
      CASS_LOG_WARN);
  EXPECT_TRUE(wait_for_logger(1));
}
