/*
  Copyright (c) 2014-2015 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "test_utils.hpp"
#include "policy_tools.hpp"

#include "cql_ccm_bridge.hpp"

struct LoadBalancingTests {
};

BOOST_FIXTURE_TEST_SUITE(load_balancing, LoadBalancingTests)

BOOST_AUTO_TEST_CASE(round_robin)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 3);

  cass_cluster_set_load_balance_round_robin(cluster.get());;

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  PolicyTool policy_tool;
  policy_tool.create_schema(session.get(), 1);

  policy_tool.init(session.get(), 12, CASS_CONSISTENCY_ONE);
  policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

  std::string host1(conf.ip_prefix() + "1");
  std::string host2(conf.ip_prefix() + "2");
  std::string host3(conf.ip_prefix() + "3");

  policy_tool.assert_queried(host1, 4);
  policy_tool.assert_queried(host2, 4);
  policy_tool.assert_queried(host3, 4);

  policy_tool.reset_coordinators();
  ccm->stop(1);

  policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

  policy_tool.assert_queried(host2, 6);
  policy_tool.assert_queried(host3, 6);
}

BOOST_AUTO_TEST_CASE(dc_aware)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  const cql::cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", 2, 1);

  cass_cluster_set_load_balance_dc_aware(cluster.get(), "dc1", 1, cass_false);

  test_utils::initialize_contact_points(cluster.get(), conf.ip_prefix(), 1, 0);

  test_utils::CassSessionPtr session(test_utils::create_session(cluster.get()));

  PolicyTool policy_tool;
  policy_tool.create_schema(session.get(), 2, 1);

  policy_tool.init(session.get(), 12, CASS_CONSISTENCY_EACH_QUORUM);
  policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

  std::string host1(conf.ip_prefix() + "1");
  std::string host2(conf.ip_prefix() + "2");
  std::string host3(conf.ip_prefix() + "3");

  policy_tool.assert_queried(host1, 6);
  policy_tool.assert_queried(host2, 6);

  policy_tool.reset_coordinators();
  ccm->stop(1);
  ccm->stop(2);

  policy_tool.query(session.get(), 12, CASS_CONSISTENCY_ONE);

  policy_tool.assert_queried(host3, 12);
}

BOOST_AUTO_TEST_SUITE_END()
