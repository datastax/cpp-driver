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

#include "objects/cluster.hpp"

/**
 * Set local dc to null for dc-aware lbp
 *
 * @jira_ticket CPP-368
 * @test_category configuration
 * @expected_result Error out because it is illegal to specify a null local-dc.
 */
TEST(ClusterTest, SetLoadBalanceDcAwareNullLocalDc) {
  test::driver::Cluster cluster;
  EXPECT_EQ(CASS_ERROR_LIB_BAD_PARAMS,
    cass_cluster_set_load_balance_dc_aware(cluster.get(), NULL, 99, cass_false));
}
