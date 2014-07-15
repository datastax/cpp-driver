#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <chrono>
#include <algorithm>
#include <future>

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

struct LoadBalancingTests : test_utils::SingleSessionTest {
    LoadBalancingTests() : SingleSessionTest(3, 0) {}
};

BOOST_FIXTURE_TEST_SUITE(load_balancing, LoadBalancingTests)

BOOST_AUTO_TEST_CASE(test_round_robin)
{
  PolicyTool policy_tool;
  policy_tool.create_schema(session, 1);

  policy_tool.init(session, 12, CASS_CONSISTENCY_ONE);
  policy_tool.query(session, 12, CASS_CONSISTENCY_ONE);

  cass::Address host1;
  BOOST_REQUIRE(cass::Address::from_string(conf.ip_prefix() + "1", 9042, &host1));

  cass::Address host2;
  BOOST_REQUIRE(cass::Address::from_string(conf.ip_prefix() + "2", 9042, &host2));

  cass::Address host3;
  BOOST_REQUIRE(cass::Address::from_string(conf.ip_prefix() + "3", 9042, &host3));

  policy_tool.assertQueried(host1, 4);
  policy_tool.assertQueried(host2, 4);
  policy_tool.assertQueried(host3, 4);

  policy_tool.reset_coordinators();
  ccm->decommission(1);

  policy_tool.query(session, 12, CASS_CONSISTENCY_ONE);

  // TODO(mpenick): This currently wrong, because we don't have a state listener interface
  // and control connection to remove the host from the load balancing policy
  policy_tool.assertQueried(host2, 8);
  policy_tool.assertQueried(host3, 4);
}

BOOST_AUTO_TEST_SUITE_END()
