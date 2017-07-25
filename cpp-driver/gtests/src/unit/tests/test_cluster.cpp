/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
