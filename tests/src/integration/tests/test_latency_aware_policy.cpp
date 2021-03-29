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

#include "cassandra.h"
#include "integration.hpp"

#include <algorithm>
#include <iterator>

class LatencyAwarePolicyTest : public Integration {
public:
  void SetUp() {
    number_dc1_nodes_ = 3;
    is_session_requested_ = false;
    Integration::SetUp();
  }
};

/**
 * Validates that latency-aware policy is enabled and updating the minimum average latency.
 *
 * @since 2.16.1
 * @jira_ticket CPP-935
 * @test_category load_balancing_policy:latency_aware
 */
CASSANDRA_INTEGRATION_TEST_F(LatencyAwarePolicyTest, IsEnabled) {
  CHECK_FAILURE
  cluster_ = default_cluster();
  cluster_.with_load_balance_round_robin();
  cass_cluster_set_token_aware_routing(cluster_.get(), cass_false);
  cass_cluster_set_latency_aware_routing(cluster_.get(), cass_true);
  cass_cluster_set_latency_aware_routing_settings(cluster_.get(), 2.0, 100LL * 1000LL * 1000LL,
                                                  10LL * 1000LL * 1000LL * 1000LL, 100, 1);
  connect(cluster_);

  logger_.reset();
  logger_.add_critera("Calculated new minimum:");

  for (int i = 0; i < 9; ++i) { // Greater than min measured
    session_.execute("SELECT release_version FROM system.local");
  }

  msleep(250);

  EXPECT_GT(logger_.count(), 0u);
}
