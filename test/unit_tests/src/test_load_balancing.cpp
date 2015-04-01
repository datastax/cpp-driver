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

#include "address.hpp"
#include "dc_aware_policy.hpp"
#include "query_request.hpp"
#include "token_aware_policy.hpp"
#include "token_map.hpp"
#include "replication_strategy.hpp"

#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/test/unit_test.hpp>

#include <limits>
#include <string>

#include "murmur3.hpp"


using std::string;

const string LOCAL_DC = "local";
const string REMOTE_DC = "remote";

#define VECTOR_FROM(t, a) std::vector<t>(a, a + sizeof(a)/sizeof(a[0]))

cass::Address addr_for_sequence(size_t i) {
  cass::Address addr("0.0.0.0", 9042);
  addr.addr_in()->sin_addr.s_addr = i;
  return addr;
}

cass::SharedRefPtr<cass::Host> host_for_addr(const cass::Address addr,
                                             const std::string& rack = "rack",
                                             const std::string& dc = "dc") {
  cass::SharedRefPtr<cass::Host>host(new cass::Host(addr, false));
  host->set_up();
  host->set_rack_and_dc(rack, dc);
  return host;
}

void populate_hosts(size_t count, const std::string& rack,
                    const std::string& dc, cass::HostMap* hosts) {
  cass::Address addr;
  size_t first = hosts->size() + 1;
  for (size_t i = first; i < first+count; ++i) {
    addr = addr_for_sequence(i);
    (*hosts)[addr] = host_for_addr(addr, rack, dc);
  }
}

void verify_sequence(cass::QueryPlan* qp, const std::vector<size_t>& sequence) {
  cass::Address expected("0.0.0.0", 9042);
  cass::Address received;
  for (std::vector<size_t>::const_iterator it = sequence.begin();
                                           it!= sequence.end();
                                         ++it) {
    BOOST_REQUIRE(qp->compute_next(&received));
    expected.addr_in()->sin_addr.s_addr = *it;
    BOOST_CHECK_EQUAL(expected, received);
  }
  BOOST_CHECK(!qp->compute_next(&received));
}

BOOST_AUTO_TEST_SUITE(round_robin_lb)

BOOST_AUTO_TEST_CASE(simple) {
  cass::HostMap hosts;
  populate_hosts(2, "rack", "dc", &hosts);

  cass::RoundRobinPolicy policy;
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::TokenMap tokenMap;

  // start on first elem
  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, tokenMap));
  const size_t seq1[] = {1, 2};
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));

  // rotate starting element
  boost::scoped_ptr<cass::QueryPlan> qp2(policy.new_query_plan("ks", NULL, tokenMap));
  const size_t seq2[] = {2, 1};
  verify_sequence(qp2.get(), VECTOR_FROM(size_t, seq2));

  // back around
  boost::scoped_ptr<cass::QueryPlan> qp3(policy.new_query_plan("ks", NULL, tokenMap));
  verify_sequence(qp3.get(), VECTOR_FROM(size_t, seq1));
}

BOOST_AUTO_TEST_CASE(on_add)
{
  cass::HostMap hosts;
  populate_hosts(2, "rack", "dc", &hosts);

  cass::RoundRobinPolicy policy;
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::TokenMap tokenMap;

  // baseline
  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, tokenMap));
  const size_t seq1[] = {1, 2};
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));

  const size_t seq_new = 5;
  cass::Address addr_new = addr_for_sequence(seq_new);
  cass::SharedRefPtr<cass::Host> host = host_for_addr(addr_new);
  policy.on_add(host);

  boost::scoped_ptr<cass::QueryPlan> qp2(policy.new_query_plan("ks", NULL, tokenMap));
  const size_t seq2[] = {2, seq_new, 1};
  verify_sequence(qp2.get(), VECTOR_FROM(size_t, seq2));
}

BOOST_AUTO_TEST_CASE(on_remove)
{
  cass::HostMap hosts;
  populate_hosts(3, "rack", "dc", &hosts);

  cass::RoundRobinPolicy policy;
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::TokenMap tokenMap;

  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, tokenMap));
  cass::SharedRefPtr<cass::Host> host = hosts.begin()->second;
  policy.on_remove(host);

  boost::scoped_ptr<cass::QueryPlan> qp2(policy.new_query_plan("ks", NULL, tokenMap));

  // first query plan has it
  // (note: not manipulating Host::state_ for dynamic removal)
  const size_t seq1[] = {1, 2, 3};
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));

  // second one does not
  const size_t seq2[] = {3, 2};
  verify_sequence(qp2.get(), VECTOR_FROM(size_t, seq2));
}

BOOST_AUTO_TEST_CASE(on_down_on_up)
{
  cass::HostMap hosts;
  populate_hosts(3, "rack", "dc", &hosts);

  cass::RoundRobinPolicy policy;
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::TokenMap tokenMap;

  boost::scoped_ptr<cass::QueryPlan> qp_before1(policy.new_query_plan("ks", NULL, tokenMap));
  boost::scoped_ptr<cass::QueryPlan> qp_before2(policy.new_query_plan("ks", NULL, tokenMap));
  cass::SharedRefPtr<cass::Host> host = hosts.begin()->second;
  policy.on_down(host);

  // 'before' qp both have the down host
  // Ahead of set_down, it will be returned
  {
    const size_t seq[] = {1, 2, 3};
    verify_sequence(qp_before1.get(), VECTOR_FROM(size_t, seq));
  }

  host->set_down();
  // Following set_down, it is dynamically excluded
  {
    const size_t seq[] = {2, 3};
    verify_sequence(qp_before2.get(), VECTOR_FROM(size_t, seq));
  }

  // host is added to the list, but not 'up'
  policy.on_up(host);

  boost::scoped_ptr<cass::QueryPlan> qp_after1(policy.new_query_plan("ks", NULL, tokenMap));
  boost::scoped_ptr<cass::QueryPlan> qp_after2(policy.new_query_plan("ks", NULL, tokenMap));

  // 1 is dynamically excluded from plan
  {
    const size_t seq[] = {2, 3};
    verify_sequence(qp_after1.get(), VECTOR_FROM(size_t, seq));
  }

  host->set_up();

  // now included
  {
    const size_t seq[] = {2, 3, 1};
    verify_sequence(qp_after2.get(), VECTOR_FROM(size_t, seq));
  }
}

BOOST_AUTO_TEST_SUITE_END()


BOOST_AUTO_TEST_SUITE(dc_aware_lb)

void test_dc_aware_policy(size_t local_count, size_t remote_count) {
  cass::HostMap hosts;
  populate_hosts(local_count, "rack", LOCAL_DC, &hosts);
  populate_hosts(remote_count, "rack", REMOTE_DC, &hosts);
  cass::DCAwarePolicy policy(LOCAL_DC, remote_count, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  const size_t total_hosts = local_count + remote_count;
  cass::TokenMap tokenMap;

  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, tokenMap));
  std::vector<size_t> seq(total_hosts);
  for (size_t i = 0; i < total_hosts; ++i) seq[i] = i + 1;
  verify_sequence(qp.get(), seq);
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

  cass::DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::TokenMap tokenMap;
  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, tokenMap));

  const size_t seq[] = {2, 3, 1};
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
}

BOOST_AUTO_TEST_CASE(single_local_down)
{
  cass::HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  cass::SharedRefPtr<cass::Host> target_host = hosts.begin()->second;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);

  cass::DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::TokenMap tokenMap;

  boost::scoped_ptr<cass::QueryPlan> qp_before(policy.new_query_plan("ks", NULL, tokenMap));// has down host ptr in plan
  target_host->set_down();
  policy.on_down(target_host);
  boost::scoped_ptr<cass::QueryPlan> qp_after(policy.new_query_plan("ks", NULL, tokenMap));// should not have down host ptr in plan

  {
    const size_t seq[] = {2, 3, 4};
    verify_sequence(qp_before.get(), VECTOR_FROM(size_t, seq));
  }

  {
    const size_t seq[] = {3, 2, 4};// local dc wrapped before remote offered
    verify_sequence(qp_after.get(), VECTOR_FROM(size_t, seq));
  }
}

BOOST_AUTO_TEST_CASE(all_local_removed_returned)
{
  cass::HostMap hosts;
  populate_hosts(1, "rack", LOCAL_DC, &hosts);
  cass::SharedRefPtr<cass::Host> target_host = hosts.begin()->second;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);

  cass::DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::TokenMap tokenMap;

  boost::scoped_ptr<cass::QueryPlan> qp_before(policy.new_query_plan("ks", NULL, tokenMap));// has down host ptr in plan
  target_host->set_down();
  policy.on_down(target_host);
  boost::scoped_ptr<cass::QueryPlan> qp_after(policy.new_query_plan("ks", NULL, tokenMap));// should not have down host ptr in plan

  {
    const size_t seq[] = {2};
    verify_sequence(qp_before.get(), VECTOR_FROM(size_t, seq));
    verify_sequence(qp_after.get(), VECTOR_FROM(size_t, seq));
  }

  target_host->set_up();
  policy.on_up(target_host);

  // make sure we get the local node first after on_up
  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, tokenMap));
  {
    const size_t seq[] = {1, 2};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

BOOST_AUTO_TEST_CASE(remote_removed_returned)
{
  cass::HostMap hosts;
  populate_hosts(1, "rack", LOCAL_DC, &hosts);
  populate_hosts(1, "rack", REMOTE_DC, &hosts);
  cass::Address target_addr("2.0.0.0", 9042);
  cass::SharedRefPtr<cass::Host> target_host = hosts[target_addr];

  cass::DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::TokenMap tokenMap;

  boost::scoped_ptr<cass::QueryPlan> qp_before(policy.new_query_plan("ks", NULL, tokenMap));// has down host ptr in plan
  target_host->set_down();
  policy.on_down(target_host);
  boost::scoped_ptr<cass::QueryPlan> qp_after(policy.new_query_plan("ks", NULL, tokenMap));// should not have down host ptr in plan

  {
    const size_t seq[] = {1};
    verify_sequence(qp_before.get(), VECTOR_FROM(size_t, seq));
    verify_sequence(qp_after.get(), VECTOR_FROM(size_t, seq));
  }

  target_host->set_up();
  policy.on_up(target_host);

  // make sure we get both nodes, correct order after
  boost::scoped_ptr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, tokenMap));
  {
    const size_t seq[] = {1, 2};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

BOOST_AUTO_TEST_CASE(used_hosts_per_remote_dc)
{
  cass::HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  populate_hosts(3, "rack", REMOTE_DC, &hosts);

  for (size_t used_hosts = 0; used_hosts < 3; ++used_hosts) {
    cass::DCAwarePolicy policy(LOCAL_DC, used_hosts, false);
    policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, cass::TokenMap()));
    size_t total_hosts = 3 + used_hosts;
    std::vector<size_t> seq(total_hosts);
    for (size_t i = 0; i < total_hosts; ++i) seq[i] = i + 1;
    verify_sequence(qp.get(), seq);
  }
}

BOOST_AUTO_TEST_CASE(allow_remote_dcs_for_local_cl)
{
  cass::HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  populate_hosts(3, "rack", REMOTE_DC, &hosts);

  {
    // Not allowing remote DCs for local CLs
    bool allow_remote_dcs_for_local_cl = false;
    cass::DCAwarePolicy policy(LOCAL_DC, 3, !allow_remote_dcs_for_local_cl);
    policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

    // Set local CL
    cass::SharedRefPtr<cass::QueryRequest> request(new cass::QueryRequest());
    request->set_consistency(CASS_CONSISTENCY_LOCAL_ONE);

    // Check for only local hosts are used
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", request.get(), cass::TokenMap()));
    const size_t seq[] = {1, 2, 3};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  {
    // Allowing remote DCs for local CLs
    bool allow_remote_dcs_for_local_cl = true;
    cass::DCAwarePolicy policy(LOCAL_DC, 3, !allow_remote_dcs_for_local_cl);
    policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

    // Set local CL
    cass::SharedRefPtr<cass::QueryRequest> request(new cass::QueryRequest());
    request->set_consistency(CASS_CONSISTENCY_LOCAL_QUORUM);

    // Check for only local hosts are used
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", request.get(), cass::TokenMap()));
    const size_t seq[] = {1, 2, 3, 4, 5, 6};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

BOOST_AUTO_TEST_CASE(start_with_empty_local_dc)
{
  cass::HostMap hosts;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);
  populate_hosts(3, "rack", LOCAL_DC, &hosts);

  // Set local DC using connected host
  {
    cass::DCAwarePolicy policy("", 0, false);
    policy.init(hosts[cass::Address("2.0.0.0", 4092)], hosts);

    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, cass::TokenMap()));
    const size_t seq[] = {2, 3, 4};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Set local DC using first host with non-empty DC
  {
    cass::DCAwarePolicy policy("", 0, false);
    policy.init(cass::SharedRefPtr<cass::Host>(
                  new cass::Host(cass::Address("0.0.0.0", 4092), false)), hosts);

    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, cass::TokenMap()));
    const size_t seq[] = {1};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

BOOST_AUTO_TEST_SUITE_END()


BOOST_AUTO_TEST_SUITE(token_aware_lb)

int64_t murmur3_hash(const std::string& s) {
  return cass::MurmurHash3_x64_128(s.data(), s.size(), 0);
}

BOOST_AUTO_TEST_CASE(simple)
{
  const int64_t num_hosts = 4;
  cass::HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);
  cass::TokenAwarePolicy policy(new cass::RoundRobinPolicy());
  cass::TokenMap token_map;

  token_map.set_partitioner(cass::Murmur3Partitioner::PARTITIONER_CLASS);
  cass::SharedRefPtr<cass::ReplicationStrategy> strategy(new cass::SimpleStrategy("", 3));
  token_map.set_replication_strategy("test", strategy);

  // Tokens
  // 1.0.0.0 -4611686018427387905
  // 2.0.0.0 -2
  // 3.0.0.0  4611686018427387901
  // 4.0.0.0  9223372036854775804

  uint64_t partition_size = std::numeric_limits<uint64_t>::max() / num_hosts;
  int64_t t = std::numeric_limits<int64_t>::min() + partition_size;
  for (cass::HostMap::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    std::string ts = boost::lexical_cast<std::string>(t);
    cass::TokenStringList tokens;
    tokens.push_back(boost::string_ref(ts));
    token_map.update_host(i->second, tokens);
    t += partition_size;
  }

  token_map.build();
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::SharedRefPtr<cass::QueryRequest> request(new cass::QueryRequest(1));
  const char* value = "kjdfjkldsdjkl"; // hash: 9024137376112061887
  request->bind(0, value, strlen(value));
  request->add_key_index(0);

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request.get(), token_map));
    const size_t seq[] = { 4, 1, 2, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Bring down the first host
  cass::HostMap::iterator curr_host_it = hosts.begin(); // 1.0.0.0
  curr_host_it->second->set_down();

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request.get(), token_map));
    const size_t seq[] = { 2, 4, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Restore the first host and bring down the first token aware replica
  curr_host_it->second->set_up();
  ++curr_host_it; // 2.0.0.0
  ++curr_host_it; // 3.0.0.0
  ++curr_host_it; // 4.0.0.0
  curr_host_it->second->set_down();

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request.get(), token_map));
    const size_t seq[] = { 2, 1, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

BOOST_AUTO_TEST_CASE(network_topology)
{
  const size_t num_hosts = 7;
  cass::HostMap hosts;

  for (size_t i = 1; i <= num_hosts; ++i) {
    cass::Address addr = addr_for_sequence(i);
    if (i % 2 == 0) {
      hosts[addr] = host_for_addr(addr, "rack1", REMOTE_DC);
    } else {
      hosts[addr] = host_for_addr(addr, "rack1", LOCAL_DC);
    }
  }

  cass::TokenAwarePolicy policy(new cass::DCAwarePolicy(LOCAL_DC, num_hosts / 2, false));
  cass::TokenMap token_map;

  token_map.set_partitioner(cass::Murmur3Partitioner::PARTITIONER_CLASS);
  cass::NetworkTopologyStrategy::DCReplicaCountMap replication_factors;
  replication_factors[LOCAL_DC] = 3;
  replication_factors[REMOTE_DC] = 2;
  cass::SharedRefPtr<cass::ReplicationStrategy> strategy(new cass::NetworkTopologyStrategy("", replication_factors));
  token_map.set_replication_strategy("test", strategy);

  // Tokens
  // 1.0.0.0 local  -6588122883467697006
  // 2.0.0.0 remote -3952873730080618204
  // 3.0.0.0 local  -1317624576693539402
  // 4.0.0.0 remote  1317624576693539400
  // 5.0.0.0 local   3952873730080618202
  // 6.0.0.0 remote  6588122883467697004
  // 7.0.0.0 local   9223372036854775806

  uint64_t partition_size = std::numeric_limits<uint64_t>::max() / num_hosts;
  int64_t t = std::numeric_limits<int64_t>::min() + partition_size;
  for (cass::HostMap::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    std::string ts = boost::lexical_cast<std::string>(t);
    cass::TokenStringList tokens;
    tokens.push_back(boost::string_ref(ts));
    token_map.update_host(i->second, tokens);
    t += partition_size;
  }

  token_map.build();
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts);

  cass::SharedRefPtr<cass::QueryRequest> request(new cass::QueryRequest(1));
  const char* value = "abc"; // hash: -5434086359492102041
  request->bind(0, value, strlen(value));
  request->add_key_index(0);

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request.get(), token_map));
    const size_t seq[] = { 3, 5, 7, 1, 4, 6, 2 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Bring down the first host
  cass::HostMap::iterator curr_host_it = hosts.begin(); // 1.0.0.0
  curr_host_it->second->set_down();

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request.get(), token_map));
    const size_t seq[] = { 3, 5, 7, 6, 2, 4 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Restore the first host and bring down the first token aware replica
  curr_host_it->second->set_up();
  ++curr_host_it; // 2.0.0.0
  ++curr_host_it; // 3.0.0.0
  curr_host_it->second->set_down();

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request.get(), token_map));
    const size_t seq[] = { 5, 7, 1, 2, 4, 6 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

BOOST_AUTO_TEST_SUITE_END()
