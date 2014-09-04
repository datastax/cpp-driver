/*
  Copyright (c) 2014 DataStax

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

#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "address.hpp"
#include "dc_aware_policy.hpp"

#include <boost/scoped_ptr.hpp>
#include <boost/test/unit_test.hpp>

#include <string>

using std::string;

const string LOCAL_DC = "local";
const string REMOTE_DC = "remote";

void populate_hosts(size_t count, const std::string& rack,
                    const std::string& dc, cass::HostMap* hosts) {
  cass::Address addr("0.0.0.0", 9042);
  size_t first = hosts->size() + 1;
  for (size_t i = first; i < first+count; ++i) {
    addr.addr_in()->sin_addr.s_addr = i;
    cass::Host* host = new cass::Host(addr, false);
    host->set_up();
    host->set_rack_and_dc(rack, dc);
    (*hosts)[addr] = cass::SharedRefPtr<cass::Host>(host);
  }
}

BOOST_AUTO_TEST_SUITE(dc_aware_lb)

void test_dc_aware_policy(size_t local_count, size_t remote_count) {
  cass::HostMap hosts;
  populate_hosts(local_count, "rack", LOCAL_DC, &hosts);
  populate_hosts(remote_count, "rack", REMOTE_DC, &hosts);
  cass::DCAwarePolicy policy(LOCAL_DC);
  policy.init(hosts);

  const size_t total_hosts = local_count + remote_count;

  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan());
  cass::Address expected("0.0.0.0", 9042);
  cass::Address received;

  for (size_t i = 0; i < total_hosts; ++i) {
    BOOST_REQUIRE(qp->compute_next(&received));
    ++expected.addr_in()->sin_addr.s_addr;
    BOOST_CHECK_EQUAL(expected, received);
  }

  BOOST_CHECK(!qp->compute_next(&received));
}

BOOST_AUTO_TEST_CASE(simple) {
  test_dc_aware_policy(2, 1);
  test_dc_aware_policy(2, 0);
  test_dc_aware_policy(0, 2);
  test_dc_aware_policy(0, 0);
}

BOOST_AUTO_TEST_CASE(some_dc_local_unspecified)
{
  const size_t total_hosts = 3;
  cass::HostMap hosts;
  populate_hosts(total_hosts, "rack", LOCAL_DC, &hosts);
  cass::Host* h = hosts.begin()->second.get();
  h->set_rack_and_dc("", "");
  cass::Address addr_no_dc = h->address();

  cass::DCAwarePolicy policy(LOCAL_DC);
  policy.init(hosts);

  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan());

  cass::Address received;
  for (size_t i = 0; i < total_hosts - 1; ++i) {
    BOOST_REQUIRE(qp->compute_next(&received));
    BOOST_CHECK_NE(received, addr_no_dc);
  }

  BOOST_REQUIRE(qp->compute_next(&received));
  BOOST_CHECK_EQUAL(received, addr_no_dc);

  BOOST_CHECK(!qp->compute_next(&received));
}

BOOST_AUTO_TEST_CASE(single_local_down)
{
  const size_t local_count = 3;
  cass::HostMap hosts;
  populate_hosts(local_count, "rack", LOCAL_DC, &hosts);
  cass::SharedRefPtr<cass::Host> target_host = hosts.begin()->second;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);
  const size_t total_hosts = local_count + 1;

  cass::DCAwarePolicy policy(LOCAL_DC);
  policy.init(hosts);

  boost::scoped_ptr<cass::QueryPlan> qp_before(policy.new_query_plan());// has down host ptr in plan
  target_host->set_down();
  policy.on_down(target_host);
  boost::scoped_ptr<cass::QueryPlan> qp_after(policy.new_query_plan());// should not have down host ptr in plan

  cass::Address received;
  for (size_t i = 0; i < total_hosts - 1; ++i) {
    BOOST_REQUIRE(qp_before->compute_next(&received));
    BOOST_CHECK_NE(received, target_host->address());
  }
  BOOST_CHECK(!qp_before->compute_next(&received));

  for (size_t i = 0; i < total_hosts - 1; ++i) {
    BOOST_REQUIRE(qp_after->compute_next(&received));
    BOOST_CHECK_NE(received, target_host->address());
  }
  BOOST_CHECK(!qp_after->compute_next(&received));
}

BOOST_AUTO_TEST_CASE(all_local_removed_returned)
{
  cass::HostMap hosts;
  populate_hosts(1, "rack", LOCAL_DC, &hosts);
  cass::SharedRefPtr<cass::Host> target_host = hosts.begin()->second;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);
  const size_t total_hosts = 2;

  cass::DCAwarePolicy policy(LOCAL_DC);
  policy.init(hosts);

  boost::scoped_ptr<cass::QueryPlan> qp_before(policy.new_query_plan());// has down host ptr in plan
  target_host->set_down();
  policy.on_down(target_host);
  boost::scoped_ptr<cass::QueryPlan> qp_after(policy.new_query_plan());// should not have down host ptr in plan

  cass::Address received;
  for (size_t i = 0; i < total_hosts - 1; ++i) {
    BOOST_REQUIRE(qp_before->compute_next(&received));
    BOOST_CHECK_NE(received, target_host->address());
  }
  BOOST_CHECK(!qp_before->compute_next(&received));

  for (size_t i = 0; i < total_hosts - 1; ++i) {
    BOOST_REQUIRE(qp_after->compute_next(&received));
    BOOST_CHECK_NE(received, target_host->address());
  }
  BOOST_CHECK(!qp_after->compute_next(&received));

  target_host->set_up();
  policy.on_up(target_host);

  // make sure we get the local node first after on_up
  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan());
  cass::Address expected("0.0.0.0", 9042);
  for (size_t i = 0; i < total_hosts; ++i) {
    BOOST_REQUIRE(qp->compute_next(&received));
    ++expected.addr_in()->sin_addr.s_addr;
    BOOST_CHECK_EQUAL(expected, received);
  }
  BOOST_CHECK(!qp_before->compute_next(&received));
}

BOOST_AUTO_TEST_CASE(remote_removed_returned)
{
  cass::HostMap hosts;
  populate_hosts(1, "rack", LOCAL_DC, &hosts);
  populate_hosts(1, "rack", REMOTE_DC, &hosts);
  const size_t total_hosts = 2;
  cass::Address target_addr("2.0.0.0", 9042);
  cass::SharedRefPtr<cass::Host> target_host = hosts[target_addr];

  cass::DCAwarePolicy policy(LOCAL_DC);
  policy.init(hosts);

  boost::scoped_ptr<cass::QueryPlan> qp_before(policy.new_query_plan());// has down host ptr in plan
  target_host->set_down();
  policy.on_down(target_host);
  boost::scoped_ptr<cass::QueryPlan> qp_after(policy.new_query_plan());// should not have down host ptr in plan

  cass::Address received;
  for (size_t i = 0; i < total_hosts - 1; ++i) {
    BOOST_REQUIRE(qp_before->compute_next(&received));
    BOOST_CHECK_NE(received, target_host->address());
  }
  BOOST_CHECK(!qp_before->compute_next(&received));

  for (size_t i = 0; i < total_hosts - 1; ++i) {
    BOOST_REQUIRE(qp_after->compute_next(&received));
    BOOST_CHECK_NE(received, target_host->address());
  }
  BOOST_CHECK(!qp_after->compute_next(&received));

  target_host->set_up();
  policy.on_up(target_host);

  // make sure we get both nodes, correct order after
  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan());
  cass::Address expected("0.0.0.0", 9042);
  for (size_t i = 0; i < total_hosts; ++i) {
    BOOST_REQUIRE(qp->compute_next(&received));
    ++expected.addr_in()->sin_addr.s_addr;
    BOOST_CHECK_EQUAL(expected, received);
  }
  BOOST_CHECK(!qp_before->compute_next(&received));
}

BOOST_AUTO_TEST_SUITE_END()

