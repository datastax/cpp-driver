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

#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/test/debug.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "policy_tools.hpp"
#include "test_utils.hpp"

#define MAX_RETRIES 50

struct LoadBalancingTests {
public:
  boost::shared_ptr<CCM::Bridge> ccm;
  std::string ip_prefix;

  LoadBalancingTests()
      : ccm(new CCM::Bridge("config.txt"))
      , ip_prefix(ccm->get_ip_prefix()) {}

  /**
   * Wait for the total number of connections established
   *
   * @param session Session to get metrics from
   * @param number_of_connections Number of connections to verify
   */
  void wait_for_total_connections(test_utils::CassSessionPtr session,
                                  cass_uint64_t number_of_connections) {
    CassMetrics metrics;
    unsigned int count = 0;
    do {
      boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
      cass_session_get_metrics(session.get(), &metrics);
    } while (metrics.stats.total_connections != number_of_connections && ++count <= MAX_RETRIES);
    BOOST_REQUIRE_EQUAL(metrics.stats.total_connections, number_of_connections);
  }
};

BOOST_FIXTURE_TEST_SUITE(load_balancing, LoadBalancingTests)

BOOST_AUTO_TEST_CASE(round_robin) {
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(3)) {
    ccm->start_cluster();
  }

  cass_cluster_set_load_balance_round_robin(cluster.get());

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
  wait_for_total_connections(session, 3);

  PolicyTool policy_tool;
  policy_tool.create_schema(session.get(), 1);

  policy_tool.init(session.get(), 12, CASS_CONSISTENCY_ONE);
  policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

  std::string host1(ip_prefix + "1");
  std::string host2(ip_prefix + "2");
  std::string host3(ip_prefix + "3");

  policy_tool.assert_queried(host1, 4);
  policy_tool.assert_queried(host2, 4);
  policy_tool.assert_queried(host3, 4);

  policy_tool.reset_coordinators();
  ccm->stop_node(1);

  policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

  policy_tool.assert_queried(host2, 6);
  policy_tool.assert_queried(host3, 6);

  // Restart stopped nodes
  ccm->start_node(1);
}

BOOST_AUTO_TEST_CASE(dc_aware) {
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(2, 1)) {
    ccm->start_cluster();
  }

  cass_cluster_set_load_balance_dc_aware(cluster.get(), "dc1", 1, cass_false);

  test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
  wait_for_total_connections(session, 3);

  PolicyTool policy_tool;
  policy_tool.create_schema(session.get(), 2, 1);

  policy_tool.init(session.get(), 12, CASS_CONSISTENCY_EACH_QUORUM);
  policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

  std::string host1(ip_prefix + "1");
  std::string host2(ip_prefix + "2");
  std::string host3(ip_prefix + "3");

  policy_tool.assert_queried(host1, 6);
  policy_tool.assert_queried(host2, 6);

  policy_tool.reset_coordinators();
  ccm->stop_node(1);
  ccm->stop_node(2);

  policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

  policy_tool.assert_queried(host3, 12);

  // Restart stopped nodes
  ccm->start_node(1);
  ccm->start_node(2);
}

BOOST_AUTO_TEST_CASE(blacklist) {
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(2)) {
    ccm->start_cluster();
  }

  std::string host1(ip_prefix + "1");
  std::string host2(ip_prefix + "2");

  // test with valid host
  {
    cass_cluster_set_blacklist_filtering(cluster.get(), host2.c_str());

    test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
    wait_for_total_connections(session, 1);

    PolicyTool policy_tool;
    policy_tool.create_schema(session.get(), 1);

    policy_tool.init(session.get(), 12, CASS_CONSISTENCY_ONE);
    policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

    policy_tool.assert_queried(host1, 12);

    policy_tool.drop_schema(session.get());
  }

  // test reset blacklist
  {
    cass_cluster_set_blacklist_filtering(cluster.get(), "");

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
    wait_for_total_connections(session, 2);

    PolicyTool policy_tool;
    policy_tool.create_schema(session.get(), 1);

    policy_tool.init(session.get(), 12, CASS_CONSISTENCY_ONE);
    policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

    policy_tool.assert_queried(host1, 6);
    policy_tool.assert_queried(host2, 6);

    policy_tool.drop_schema(session.get());
  }
}

BOOST_AUTO_TEST_CASE(blacklist_dc) {
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  if (ccm->create_cluster(2, 2)) {
    ccm->start_cluster();
  }

  cass_cluster_set_load_balance_round_robin(cluster.get());

  std::string host1(ip_prefix + "1");
  std::string host2(ip_prefix + "2");
  std::string host3(ip_prefix + "3");
  std::string host4(ip_prefix + "4");

  // test with valid dc
  {
    cass_cluster_set_blacklist_dc_filtering(cluster.get(), "dc2");

    test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
    wait_for_total_connections(session, 2);

    PolicyTool policy_tool;
    policy_tool.create_schema(session.get(), 1);

    policy_tool.init(session.get(), 12, CASS_CONSISTENCY_EACH_QUORUM);
    policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

    policy_tool.assert_queried(host1, 6);
    policy_tool.assert_queried(host2, 6);
    policy_tool.assert_queried(host3, 0);
    policy_tool.assert_queried(host4, 0);

    policy_tool.drop_schema(session.get());
  }

  // test reset blacklist
  {
    cass_cluster_set_blacklist_dc_filtering(cluster.get(), "");

    test_utils::initialize_contact_points(cluster.get(), ip_prefix, 1);

    test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));
    wait_for_total_connections(session, 4);

    PolicyTool policy_tool;
    policy_tool.create_schema(session.get(), 2);

    policy_tool.init(session.get(), 12, CASS_CONSISTENCY_EACH_QUORUM);
    policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

    policy_tool.assert_queried(host1, 3);
    policy_tool.assert_queried(host2, 3);
    policy_tool.assert_queried(host3, 3);
    policy_tool.assert_queried(host4, 3);

    policy_tool.drop_schema(session.get());
  }
}

BOOST_AUTO_TEST_SUITE_END()
