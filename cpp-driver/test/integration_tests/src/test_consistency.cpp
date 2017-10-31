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

#include "test_utils.hpp"
#include "policy_tools.hpp"

#include "cassandra.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp>

struct ConsistencyTests {
public:
  boost::shared_ptr<CCM::Bridge> ccm;
  std::string ip_prefix;

  ConsistencyTests()
    : ccm(new CCM::Bridge("config.txt"))
    , ip_prefix(ccm->get_ip_prefix()) {}
};

BOOST_FIXTURE_TEST_SUITE(consistency, ConsistencyTests)

BOOST_AUTO_TEST_CASE(simple_two_nodes)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(2)) {
    ccm->start_cluster();
  }

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 2);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  PolicyTool policy_tool;
  policy_tool.create_schema(session.get(), 1); // replication_factor = 1

  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_ONE);	// Should work
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_ONE);	// Should work
    BOOST_CHECK_EQUAL(init_result,  CASS_OK);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }
  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_ANY);  // Should work
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_ANY);  // Should fail (ANY is for writing only)
    BOOST_CHECK_EQUAL(init_result, CASS_OK);
    BOOST_CHECK_EQUAL(query_result, CASS_ERROR_SERVER_INVALID_QUERY);
  }
  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_LOCAL_QUORUM);  // Should fail (LOCAL_QUORUM is incompatible with SimpleStrategy)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_LOCAL_QUORUM);  // Should fail (see above)
    BOOST_CHECK_EQUAL(init_result, CASS_OK); // TODO(mpenick): Shouldn't be CASS_OK?
    BOOST_CHECK_EQUAL(query_result, CASS_OK); // TODO(mpenick): Shouldn't be CASS_OK?
  }
  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_EACH_QUORUM);  // Should fail (EACH_QUORUM is incompatible with SimpleStrategy)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_EACH_QUORUM);  // Should fail (see above)
    BOOST_CHECK_EQUAL(init_result, CASS_OK); // TODO(mpenick): Shouldn't be CASS_OK?
    // Handle EACH_QUORUM read support added to C* v3.0.0 [CASSANDRA-9602]
    // https://issues.apache.org/jira/browse/CASSANDRA-9602
    if (test_utils::get_version() >= "3.0.0") {
      BOOST_CHECK_EQUAL(query_result, CASS_OK);
    } else {
      BOOST_CHECK_EQUAL(query_result, CASS_ERROR_SERVER_INVALID_QUERY);
    }
  }
  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_THREE);  // Should fail (N=2, RF=1)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_THREE);  // Should fail (N=2, RF=1)
    BOOST_CHECK_EQUAL(init_result, CASS_ERROR_SERVER_UNAVAILABLE);
    BOOST_CHECK_EQUAL(query_result, CASS_ERROR_SERVER_UNAVAILABLE);
  }

  // Ensure the keyspace is dropped
  policy_tool.drop_schema(session.get());
}

BOOST_AUTO_TEST_CASE(one_node_down)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(3)) {
    ccm->start_cluster();
  }

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 3);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  PolicyTool policy_tool;
  policy_tool.create_schema(session.get(), 3); // replication_factor = 3

  {
    // Sanity check
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_ALL);	// Should work (N=3, RF=3)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_ALL);	// Should work (N=3, RF=3)
    BOOST_CHECK_EQUAL(init_result,  CASS_OK);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }

  ccm->force_decommission_node(2);

  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_ONE);	// Should work (N=2, RF=3)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_ONE);	// Should work (N=2, RF=3)
    BOOST_CHECK_EQUAL(init_result,  CASS_OK);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }
  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_TWO);	// Should work (N=2, RF=3)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_TWO);	// Should work (N=2, RF=3)
    BOOST_CHECK_EQUAL(init_result,  CASS_OK);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }
  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_ALL);	// Should fail (N=2, RF=3)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_ALL);	// Should fail (N=2, RF=3)
    BOOST_CHECK_NE(init_result, CASS_OK);
    BOOST_CHECK_NE(query_result, CASS_OK);
  }
  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_QUORUM);	// Should work (N=2, RF=3, quorum=2)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_QUORUM);	// Should work (N=2, RF=3, quorum=2)
    BOOST_CHECK_EQUAL(init_result,  CASS_OK);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }

  // Destroy the current cluster (chaotic tests; decommissioned nodes)
  ccm->remove_cluster();
}

BOOST_AUTO_TEST_CASE(two_nodes_down)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(3)) {
    ccm->start_cluster();
  }

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 3);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  PolicyTool policy_tool;
  policy_tool.create_schema(session.get(), 3); // replication_factor = 3

  {
    // Sanity check
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_ALL);	// Should work (N=3, RF=3)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_ALL);	// Should work (N=3, RF=3)
    BOOST_CHECK_EQUAL(init_result,  CASS_OK);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }

  ccm->force_decommission_node(2);
  ccm->force_decommission_node(3);

  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_ONE);	// Should work (N=1, RF=3)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_ONE);	// Should work (N=1, RF=3)
    BOOST_CHECK_EQUAL(init_result,  CASS_OK);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }
  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_TWO);	// Should fail (N=1, RF=3)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_TWO);	// Should fail (N=1, RF=3)
    BOOST_CHECK(init_result  != CASS_OK);
    BOOST_CHECK(query_result != CASS_OK);
  }
  {
    CassError init_result  = policy_tool.init_return_error(session.get(),  12, CASS_CONSISTENCY_QUORUM);	// Should fail (N=1, RF=3, quorum=2)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_QUORUM);	// Should fail (N=1, RF=3, quorum=2)
    BOOST_CHECK(init_result  != CASS_OK);
    BOOST_CHECK(query_result != CASS_OK);
  }

  // Destroy the current cluster (chaotic tests; decommissioned nodes)
  ccm->remove_cluster();
}

BOOST_AUTO_TEST_CASE(retry_policy_downgrading)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());
  CassRetryPolicy* downgrading_policy = cass_retry_policy_downgrading_consistency_new();
  cass_cluster_set_retry_policy(cluster.get(), downgrading_policy);
  cass_cluster_set_connection_heartbeat_interval(cluster.get(), 1);
  cass_cluster_set_connection_idle_timeout(cluster.get(), 10);

  if (ccm->create_cluster(3)) {
    ccm->start_cluster();
  }

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 3);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  PolicyTool policy_tool;
  policy_tool.create_schema(session.get(), 3); // replication_factor = 3

  {
    // Sanity check
    CassError init_result = policy_tool.init_return_error(session.get(), 12, CASS_CONSISTENCY_ALL);	// Should work (N=3, RF=3)
    CassError query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_ALL);	// Should work (N=3, RF=3)
    BOOST_CHECK_EQUAL(init_result, CASS_OK);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }

  ccm->stop_node(2);

  {
    CassError init_result;
    do {
      init_result = policy_tool.init_return_error(session.get(), 12, CASS_CONSISTENCY_QUORUM);	// Should work (N=2, RF=3)
    } while (init_result == CASS_ERROR_LIB_REQUEST_TIMED_OUT);
    BOOST_CHECK_EQUAL(init_result, CASS_OK);
    CassError query_result;
    do {
      query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_QUORUM);	// Should work (N=2, RF=3)
    } while (init_result == CASS_ERROR_LIB_REQUEST_TIMED_OUT);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }

  ccm->stop_node(3);

  {
    CassError init_result;
    do {
      init_result = policy_tool.init_return_error(session.get(), 12, CASS_CONSISTENCY_QUORUM);	// Should work (N=1, RF=3)
    } while (init_result == CASS_ERROR_LIB_REQUEST_TIMED_OUT);
    BOOST_CHECK_EQUAL(init_result, CASS_OK);
    CassError query_result;
    do {
      query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_QUORUM);	// Should work (N=1, RF=3)
    } while (init_result == CASS_ERROR_LIB_REQUEST_TIMED_OUT);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }

  {
    CassError init_result;
    do {
      init_result = policy_tool.init_return_error(session.get(), 12, CASS_CONSISTENCY_TWO);	// Should work (N=1, RF=3)
    } while (init_result == CASS_ERROR_LIB_REQUEST_TIMED_OUT);
    BOOST_CHECK_EQUAL(init_result, CASS_OK);
    CassError query_result;
    do {
      query_result = policy_tool.query_return_error(session.get(), 12, CASS_CONSISTENCY_TWO);	// Should work (N=1, RF=3)
    } while (init_result == CASS_ERROR_LIB_REQUEST_TIMED_OUT);
    BOOST_CHECK_EQUAL(query_result, CASS_OK);
  }

  cass_retry_policy_free(downgrading_policy);

  // Ensure the keyspace is dropped
  policy_tool.drop_schema(session.get());
}

BOOST_AUTO_TEST_SUITE_END()
