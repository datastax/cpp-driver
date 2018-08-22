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
#include "cassandra.h"

class SessionTest : public Integration {
public:
  void SetUp() {
    logger_.add_critera("Attempted to get metrics before connecting session object");
    logger_.add_critera("Attempted to get speculative execution metrics before connecting session object");
  }

  void check_metrics_log_count() {
    EXPECT_EQ(logger_.count(), 2u);
  }
};

CASSANDRA_INTEGRATION_TEST_F(SessionTest, MetricsWithoutConnecting) {
  Session session(cass_session_new());

  CassMetrics metrics;
  cass_session_get_metrics(session.get(), &metrics);

  EXPECT_EQ(metrics.requests.min, 0u);
  EXPECT_EQ(metrics.requests.one_minute_rate, 0.0);

  CassSpeculativeExecutionMetrics spec_ex_metrics;
  cass_session_get_speculative_execution_metrics(session.get(), &spec_ex_metrics);
  EXPECT_EQ(spec_ex_metrics.min, 0u);
  EXPECT_EQ(spec_ex_metrics.percentage, 0.0);

  check_metrics_log_count();
}
