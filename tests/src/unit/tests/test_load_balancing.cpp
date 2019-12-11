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

#include <gtest/gtest.h>

#include "address.hpp"
#include "blacklist_dc_policy.hpp"
#include "blacklist_policy.hpp"
#include "constants.hpp"
#include "dc_aware_policy.hpp"
#include "event_loop.hpp"
#include "latency_aware_policy.hpp"
#include "murmur3.hpp"
#include "query_request.hpp"
#include "random.hpp"
#include "request_handler.hpp"
#include "scoped_ptr.hpp"
#include "string.hpp"
#include "token_aware_policy.hpp"
#include "whitelist_dc_policy.hpp"
#include "whitelist_policy.hpp"

#include "test_token_map_utils.hpp"
#include "test_utils.hpp"

#include <uv.h>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

const String LOCAL_DC = "local";
const String REMOTE_DC = "remote";
const String BACKUP_DC = "backup";

#define VECTOR_FROM(t, a) Vector<t>(a, a + sizeof(a) / sizeof(a[0]))

Address addr_for_sequence(size_t i) {
  char temp[64];
  sprintf(temp, "%d.%d.%d.%d", static_cast<int>(i & 0xFF), static_cast<int>((i >> 8) & 0xFF),
          static_cast<int>((i >> 16) & 0xFF), static_cast<int>((i >> 24) & 0xFF));
  return Address(temp, 9042);
}

SharedRefPtr<Host> host_for_addr(const Address addr, const String& rack = "rack",
                                 const String& dc = "dc") {
  SharedRefPtr<Host> host(new Host(addr));
  host->set_rack_and_dc(rack, dc);
  return host;
}

void populate_hosts(size_t count, const String& rack, const String& dc, HostMap* hosts) {
  Address addr;
  size_t first = hosts->size() + 1;
  for (size_t i = first; i < first + count; ++i) {
    addr = addr_for_sequence(i);
    (*hosts)[addr] = host_for_addr(addr, rack, dc);
  }
}

void verify_sequence(QueryPlan* qp, const Vector<size_t>& sequence) {
  Address received;
  for (Vector<size_t>::const_iterator it = sequence.begin(); it != sequence.end(); ++it) {
    ASSERT_TRUE(qp->compute_next(&received));
    EXPECT_EQ(addr_for_sequence(*it), received);
  }
  EXPECT_FALSE(qp->compute_next(&received));
}

typedef Map<Address, int> QueryCounts;

QueryCounts run_policy(LoadBalancingPolicy& policy, int count) {
  QueryCounts counts;
  for (int i = 0; i < 12; ++i) {
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
    Host::Ptr host(qp->compute_next());
    if (host) {
      counts[host->address()] += 1;
    }
  }
  return counts;
}

void verify_dcs(const QueryCounts& counts, const HostMap& hosts, const String& expected_dc) {
  for (QueryCounts::const_iterator it = counts.begin(), end = counts.end(); it != end; ++it) {
    HostMap::const_iterator host_it = hosts.find(it->first);
    ASSERT_NE(host_it, hosts.end());
    EXPECT_EQ(expected_dc, host_it->second->dc());
  }
}

void verify_query_counts(const QueryCounts& counts, int expected_count) {
  for (QueryCounts::const_iterator it = counts.begin(), end = counts.end(); it != end; ++it) {
    EXPECT_EQ(expected_count, it->second);
  }
}

struct RunPeriodicTask : public EventLoop {
  RunPeriodicTask(LatencyAwarePolicy* policy)
      : policy(policy) {
    async.data = this;
  }

  int init() {
    int rc = EventLoop::init();
    if (rc != 0) return rc;
    rc = uv_async_init(loop(), &async, on_async);
    if (rc != 0) return rc;
    policy->register_handles(loop());
    return rc;
  }

  void done() { uv_async_send(&async); }

  static void on_async(uv_async_t* handle) {
    RunPeriodicTask* task = static_cast<RunPeriodicTask*>(handle->data);
    task->close_handles();
    task->policy->close_handles();
    uv_close(reinterpret_cast<uv_handle_t*>(&task->async), NULL);
  }

  uv_async_t async;
  LatencyAwarePolicy* policy;
};

// Latency-aware utility functions

// Don't make "time_between_ns" too high because it spin waits
uint64_t calculate_moving_average(uint64_t first_latency_ns, uint64_t second_latency_ns,
                                  uint64_t time_between_ns) {
  const uint64_t scale = 100LL;
  const uint64_t min_measured = 15LL;
  const uint64_t threshold_to_account = (30LL * min_measured) / 100LL;

  Host host(Address("0.0.0.0", 9042));
  host.enable_latency_tracking(scale, min_measured);

  for (uint64_t i = 0; i < threshold_to_account; ++i) {
    host.update_latency(0); // This can be anything because it's not recorded
  }

  host.update_latency(first_latency_ns);

  // Spin wait
  uint64_t start = uv_hrtime();
  while (uv_hrtime() - start < time_between_ns) {
  }

  host.update_latency(second_latency_ns);
  TimestampedAverage current = host.get_current_average();
  return current.average;
}

void test_dc_aware_policy(size_t local_count, size_t remote_count) {
  HostMap hosts;
  populate_hosts(local_count, "rack", LOCAL_DC, &hosts);
  populate_hosts(remote_count, "rack", REMOTE_DC, &hosts);
  DCAwarePolicy policy(LOCAL_DC, remote_count, false);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  const size_t total_hosts = local_count + remote_count;

  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  Vector<size_t> seq(total_hosts);
  for (size_t i = 0; i < total_hosts; ++i)
    seq[i] = i + 1;
  verify_sequence(qp.get(), seq);
}

TEST(RoundRobinLoadBalancingUnitTest, Simple) {
  HostMap hosts;
  populate_hosts(2, "rack", "dc", &hosts);

  RoundRobinPolicy policy;
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  // start on first elem
  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  const size_t seq1[] = { 1, 2 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));

  // rotate starting element
  ScopedPtr<QueryPlan> qp2(policy.new_query_plan("ks", NULL, NULL));
  const size_t seq2[] = { 2, 1 };
  verify_sequence(qp2.get(), VECTOR_FROM(size_t, seq2));

  // back around
  ScopedPtr<QueryPlan> qp3(policy.new_query_plan("ks", NULL, NULL));
  verify_sequence(qp3.get(), VECTOR_FROM(size_t, seq1));
}

TEST(RoundRobinLoadBalancingUnitTest, OnAdd) {
  HostMap hosts;
  populate_hosts(2, "rack", "dc", &hosts);

  RoundRobinPolicy policy;
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  // baseline
  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  const size_t seq1[] = { 1, 2 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));

  const size_t seq_new = 5;
  Address addr_new = addr_for_sequence(seq_new);
  SharedRefPtr<Host> host = host_for_addr(addr_new);
  policy.on_host_added(host);
  policy.on_host_up(host);

  ScopedPtr<QueryPlan> qp2(policy.new_query_plan("ks", NULL, NULL));
  const size_t seq2[] = { 2, seq_new, 1 };
  verify_sequence(qp2.get(), VECTOR_FROM(size_t, seq2));
}

TEST(RoundRobinLoadBalancingUnitTest, OnRemove) {
  HostMap hosts;
  populate_hosts(3, "rack", "dc", &hosts);

  RoundRobinPolicy policy;
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  SharedRefPtr<Host> host = hosts.begin()->second;
  policy.on_host_removed(host);

  ScopedPtr<QueryPlan> qp2(policy.new_query_plan("ks", NULL, NULL));

  // Both should not have the removed host
  const size_t seq1[] = { 2, 3 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));

  const size_t seq2[] = { 3, 2 };
  verify_sequence(qp2.get(), VECTOR_FROM(size_t, seq2));
}

TEST(RoundRobinLoadBalancingUnitTest, OnUpAndDown) {
  HostMap hosts;
  populate_hosts(3, "rack", "dc", &hosts);

  RoundRobinPolicy policy;
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp_before1(policy.new_query_plan("ks", NULL, NULL));
  ScopedPtr<QueryPlan> qp_before2(policy.new_query_plan("ks", NULL, NULL));
  SharedRefPtr<Host> host = hosts.begin()->second;

  // 'before' qp both have the down host
  // Ahead of set_down, it will be returned
  {
    const size_t seq[] = { 1, 2, 3 };
    verify_sequence(qp_before1.get(), VECTOR_FROM(size_t, seq));
  }

  policy.on_host_down(host->address());
  // Following set_down, it is dynamically excluded
  {
    const size_t seq[] = { 2, 3 };
    verify_sequence(qp_before2.get(), VECTOR_FROM(size_t, seq));
  }

  // host is added to the list, but not 'up'
  policy.on_host_up(host);

  ScopedPtr<QueryPlan> qp_after1(policy.new_query_plan("ks", NULL, NULL));
  ScopedPtr<QueryPlan> qp_after2(policy.new_query_plan("ks", NULL, NULL));

  policy.on_host_down(host->address());
  // 1 is dynamically excluded from plan
  {
    const size_t seq[] = { 2, 3 };
    verify_sequence(qp_after1.get(), VECTOR_FROM(size_t, seq));
  }

  policy.on_host_up(host);
  // now included
  {
    const size_t seq[] = { 2, 3, 1 };
    verify_sequence(qp_after2.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(RoundRobinLoadBalancingUnitTest, VerifyEqualDistribution) {
  HostMap hosts;
  populate_hosts(3, "rack", "dc", &hosts);

  RoundRobinPolicy policy;
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  { // All nodes
    QueryCounts counts(run_policy(policy, 12));
    ASSERT_EQ(counts.size(), 3u);
    verify_query_counts(counts, 4);
  }

  policy.on_host_down(hosts.begin()->first);

  { // One node down
    QueryCounts counts(run_policy(policy, 12));
    ASSERT_EQ(counts.size(), 2u);
    verify_query_counts(counts, 6);
  }

  policy.on_host_up(hosts.begin()->second);

  { // All nodes again
    QueryCounts counts(run_policy(policy, 12));
    ASSERT_EQ(counts.size(), 3u);
    verify_query_counts(counts, 4);
  }

  policy.on_host_removed(hosts.begin()->second);

  { // One node removed
    QueryCounts counts(run_policy(policy, 12));
    ASSERT_EQ(counts.size(), 2u);
    verify_query_counts(counts, 6);
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, SomeDatacenterLocalUnspecified) {
  const size_t total_hosts = 3;
  HostMap hosts;
  populate_hosts(total_hosts, "rack", LOCAL_DC, &hosts);
  Host* h = hosts.begin()->second.get();
  h->set_rack_and_dc("", "");

  DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  const size_t seq[] = { 2, 3, 1 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
}

TEST(DatacenterAwareLoadBalancingUnitTest, SingleLocalDown) {
  HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  SharedRefPtr<Host> target_host = hosts.begin()->second;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);

  DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp_before(
      policy.new_query_plan("ks", NULL, NULL)); // has down host ptr in plan
  ScopedPtr<QueryPlan> qp_after(
      policy.new_query_plan("ks", NULL, NULL)); // should not have down host ptr in plan

  policy.on_host_down(target_host->address());
  {
    const size_t seq[] = { 2, 3, 4 };
    verify_sequence(qp_before.get(), VECTOR_FROM(size_t, seq));
  }

  policy.on_host_up(target_host);
  {
    const size_t seq[] = { 2, 3, 1, 4 }; // local dc wrapped before remote offered
    verify_sequence(qp_after.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, AllLocalRemovedReturned) {
  HostMap hosts;
  populate_hosts(1, "rack", LOCAL_DC, &hosts);
  SharedRefPtr<Host> target_host = hosts.begin()->second;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);

  DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp_before(
      policy.new_query_plan("ks", NULL, NULL)); // has down host ptr in plan
  policy.on_host_down(target_host->address());
  ScopedPtr<QueryPlan> qp_after(
      policy.new_query_plan("ks", NULL, NULL)); // should not have down host ptr in plan

  {
    const size_t seq[] = { 2 };
    verify_sequence(qp_before.get(), VECTOR_FROM(size_t, seq));
    verify_sequence(qp_after.get(), VECTOR_FROM(size_t, seq));
  }

  policy.on_host_up(target_host);

  // make sure we get the local node first after on_up
  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  {
    const size_t seq[] = { 1, 2 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, RemoteRemovedReturned) {
  HostMap hosts;
  populate_hosts(1, "rack", LOCAL_DC, &hosts);
  populate_hosts(1, "rack", REMOTE_DC, &hosts);
  Address target_addr("2.0.0.0", 9042);
  SharedRefPtr<Host> target_host = hosts[target_addr];

  DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp_before(
      policy.new_query_plan("ks", NULL, NULL)); // has down host ptr in plan
  policy.on_host_down(target_host->address());
  ScopedPtr<QueryPlan> qp_after(
      policy.new_query_plan("ks", NULL, NULL)); // should not have down host ptr in plan

  {
    const size_t seq[] = { 1 };
    verify_sequence(qp_before.get(), VECTOR_FROM(size_t, seq));
    verify_sequence(qp_after.get(), VECTOR_FROM(size_t, seq));
  }

  policy.on_host_up(target_host);

  // make sure we get both nodes, correct order after
  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  {
    const size_t seq[] = { 1, 2 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, UsedHostsPerDatacenter) {
  HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  populate_hosts(3, "rack", REMOTE_DC, &hosts);

  for (size_t used_hosts = 0; used_hosts < 4; ++used_hosts) {
    DCAwarePolicy policy(LOCAL_DC, used_hosts, false);
    policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

    ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
    Vector<size_t> seq;
    size_t index = 0;

    seq.reserve(3 + used_hosts);

    // Local DC hosts
    for (size_t i = 0; i < 3; ++i) {
      seq.push_back(index++ + 1);
    }

    // Remote DC hosts
    for (size_t i = 0; i < used_hosts; ++i) {
      // DC-aware only uses hosts up to the used host count so we need to wrap
      // around.
      seq.push_back(3 + (index++ % used_hosts) + 1);
    }

    verify_sequence(qp.get(), seq);
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, AllowRemoteDatacentersForLocalConsistencyLevel) {
  HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  populate_hosts(3, "rack", REMOTE_DC, &hosts);

  {
    // Not allowing remote DCs for local CLs
    bool allow_remote_dcs_for_local_cl = false;
    DCAwarePolicy policy(LOCAL_DC, 3, !allow_remote_dcs_for_local_cl);
    policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

    // Set local CL
    QueryRequest::Ptr request(new QueryRequest("", 0));
    request->set_consistency(CASS_CONSISTENCY_LOCAL_ONE);
    SharedRefPtr<RequestHandler> request_handler(
        new RequestHandler(request, ResponseFuture::Ptr()));

    // Check for only local hosts are used
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", request_handler.get(), NULL));
    const size_t seq[] = { 1, 2, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  {
    // Allowing remote DCs for local CLs
    bool allow_remote_dcs_for_local_cl = true;
    DCAwarePolicy policy(LOCAL_DC, 3, !allow_remote_dcs_for_local_cl);
    policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

    // Set local CL
    QueryRequest::Ptr request(new QueryRequest("", 0));
    request->set_consistency(CASS_CONSISTENCY_LOCAL_QUORUM);
    SharedRefPtr<RequestHandler> request_handler(
        new RequestHandler(request, ResponseFuture::Ptr()));

    // Check for only local hosts are used
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", request_handler.get(), NULL));
    const size_t seq[] = { 1, 2, 3, 4, 5, 6 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, StartWithEmptyLocalDatacenter) {
  HostMap hosts;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);
  populate_hosts(3, "rack", LOCAL_DC, &hosts);

  // Set local DC using connected host
  {
    DCAwarePolicy policy("", 0, false);
    policy.init(hosts[Address("2.0.0.0", 9042)], hosts, NULL, "");

    ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
    const size_t seq[] = { 2, 3, 4 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Set local DC using first host with non-empty DC
  {
    DCAwarePolicy policy("", 0, false);
    policy.init(SharedRefPtr<Host>(new Host(Address("0.0.0.0", 9042))), hosts, NULL, "");

    ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
    const size_t seq[] = { 1 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

Vector<String> single_token(int64_t token) {
  OStringStream ss;
  ss << token;
  return Vector<String>(1, ss.str());
}

TEST(DatacenterAwareLoadBalancingUnitTest, VerifyEqualDistributionLocalDc) {
  HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  populate_hosts(3, "rack", REMOTE_DC, &hosts);

  DCAwarePolicy policy("", 0, false);
  policy.init(hosts.begin()->second, hosts, NULL, "");

  { // All local nodes
    QueryCounts counts(run_policy(policy, 12));
    verify_dcs(counts, hosts, LOCAL_DC);
    ASSERT_EQ(counts.size(), 3u);
    verify_query_counts(counts, 4);
  }

  policy.on_host_down(hosts.begin()->first);

  { // One local node down
    QueryCounts counts(run_policy(policy, 12));
    verify_dcs(counts, hosts, LOCAL_DC);
    ASSERT_EQ(counts.size(), 2u);
    verify_query_counts(counts, 6);
  }

  policy.on_host_up(hosts.begin()->second);

  { // All local nodes again
    QueryCounts counts(run_policy(policy, 12));
    verify_dcs(counts, hosts, LOCAL_DC);
    ASSERT_EQ(counts.size(), 3u);
    verify_query_counts(counts, 4);
  }

  policy.on_host_removed(hosts.begin()->second);

  { // One local node removed
    QueryCounts counts(run_policy(policy, 12));
    verify_dcs(counts, hosts, LOCAL_DC);
    ASSERT_EQ(counts.size(), 2u);
    verify_query_counts(counts, 6);
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, VerifyEqualDistributionRemoteDc) {
  HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  populate_hosts(3, "rack", REMOTE_DC, &hosts);

  DCAwarePolicy policy("", 3, false); // Allow all remote DC nodes
  policy.init(hosts.begin()->second, hosts, NULL, "");

  Host::Ptr remote_dc_node1;
  { // Mark down all local nodes
    HostMap::iterator it = hosts.begin();
    for (int i = 0; i < 3; ++i) {
      policy.on_host_down(it->first);
      it++;
    }
    remote_dc_node1 = it->second;
  }

  { // All remote nodes
    QueryCounts counts(run_policy(policy, 12));
    verify_dcs(counts, hosts, REMOTE_DC);
    ASSERT_EQ(counts.size(), 3u);
    verify_query_counts(counts, 4);
  }

  policy.on_host_down(remote_dc_node1->address());

  { // One remote node down
    QueryCounts counts(run_policy(policy, 12));
    verify_dcs(counts, hosts, REMOTE_DC);
    ASSERT_EQ(counts.size(), 2u);
    verify_query_counts(counts, 6);
  }

  policy.on_host_up(remote_dc_node1);

  { // All remote nodes again
    QueryCounts counts(run_policy(policy, 12));
    verify_dcs(counts, hosts, REMOTE_DC);
    ASSERT_EQ(counts.size(), 3u);
    verify_query_counts(counts, 4);
  }

  policy.on_host_removed(remote_dc_node1);

  { // One remote node removed
    QueryCounts counts(run_policy(policy, 12));
    verify_dcs(counts, hosts, REMOTE_DC);
    ASSERT_EQ(counts.size(), 2u);
    verify_query_counts(counts, 6);
  }
}

TEST(TokenAwareLoadBalancingUnitTest, Simple) {
  const int64_t num_hosts = 4;
  HostMap hosts;
  TokenMap::Ptr token_map(TokenMap::from_partitioner(Murmur3Partitioner::name()));

  // Tokens
  // 1.0.0.0 -4611686018427387905
  // 2.0.0.0 -2
  // 3.0.0.0  4611686018427387901
  // 4.0.0.0  9223372036854775804

  const uint64_t partition_size = CASS_UINT64_MAX / num_hosts;
  Murmur3Partitioner::Token token = CASS_INT64_MIN + static_cast<int64_t>(partition_size);

  for (size_t i = 1; i <= num_hosts; ++i) {
    Host::Ptr host(create_host(addr_for_sequence(i), single_token(token),
                               Murmur3Partitioner::name().to_string(), "rack1", LOCAL_DC));

    hosts[host->address()] = host;
    token_map->add_host(host);
    token += partition_size;
  }

  add_keyspace_simple("test", 3, token_map.get());
  token_map->build();

  TokenAwarePolicy policy(new RoundRobinPolicy(), false);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  QueryRequest::Ptr request(new QueryRequest("", 1));
  const char* value = "kjdfjkldsdjkl"; // hash: 9024137376112061887
  request->set(0, CassString(value, strlen(value)));
  request->add_key_index(0);
  SharedRefPtr<RequestHandler> request_handler(new RequestHandler(request, ResponseFuture::Ptr()));

  {
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 4, 1, 2, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Bring down the first host
  HostMap::iterator curr_host_it = hosts.begin(); // 1.0.0.0
  policy.on_host_down(curr_host_it->second->address());

  {
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 4, 2, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Restore the first host and bring down the first token aware replica
  policy.on_host_up(curr_host_it->second);
  ++curr_host_it; // 2.0.0.0
  ++curr_host_it; // 3.0.0.0
  ++curr_host_it; // 4.0.0.0
  policy.on_host_down(curr_host_it->second->address());

  {
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 1, 2, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(TokenAwareLoadBalancingUnitTest, NetworkTopology) {
  const size_t num_hosts = 7;
  HostMap hosts;

  TokenMap::Ptr token_map(TokenMap::from_partitioner(Murmur3Partitioner::name()));

  // Tokens
  // 1.0.0.0 local  -6588122883467697006
  // 2.0.0.0 remote -3952873730080618204
  // 3.0.0.0 local  -1317624576693539402
  // 4.0.0.0 remote  1317624576693539400
  // 5.0.0.0 local   3952873730080618202
  // 6.0.0.0 remote  6588122883467697004
  // 7.0.0.0 local   9223372036854775806

  const uint64_t partition_size = CASS_UINT64_MAX / num_hosts;
  Murmur3Partitioner::Token token = CASS_INT64_MIN + static_cast<int64_t>(partition_size);

  for (size_t i = 1; i <= num_hosts; ++i) {
    Host::Ptr host(create_host(addr_for_sequence(i), single_token(token),
                               Murmur3Partitioner::name().to_string(), "rack1",
                               i % 2 == 0 ? REMOTE_DC : LOCAL_DC));

    hosts[host->address()] = host;
    token_map->add_host(host);
    token += partition_size;
  }

  ReplicationMap replication;
  replication[LOCAL_DC] = "3";
  replication[REMOTE_DC] = "2";
  add_keyspace_network_topology("test", replication, token_map.get());
  token_map->build();

  TokenAwarePolicy policy(new DCAwarePolicy(LOCAL_DC, num_hosts / 2, false), false);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  QueryRequest::Ptr request(new QueryRequest("", 1));
  const char* value = "abc"; // hash: -5434086359492102041
  request->set(0, CassString(value, strlen(value)));
  request->add_key_index(0);
  SharedRefPtr<RequestHandler> request_handler(new RequestHandler(request, ResponseFuture::Ptr()));

  {
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 3, 5, 7, 1, 4, 6, 2 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Bring down the first host
  HostMap::iterator curr_host_it = hosts.begin(); // 1.0.0.0
  policy.on_host_down(curr_host_it->second->address());

  {
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 3, 5, 7, 4, 6, 2 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Restore the first host and bring down the first token aware replica
  policy.on_host_up(curr_host_it->second);
  ++curr_host_it; // 2.0.0.0
  ++curr_host_it; // 3.0.0.0
  policy.on_host_down(curr_host_it->second->address());

  {
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 5, 7, 1, 6, 2, 4 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(TokenAwareLoadBalancingUnitTest, ShuffleReplicas) {
  Random random;

  const int64_t num_hosts = 4;
  HostMap hosts;
  TokenMap::Ptr token_map(TokenMap::from_partitioner(Murmur3Partitioner::name()));

  // Tokens
  // 1.0.0.0 -4611686018427387905
  // 2.0.0.0 -2
  // 3.0.0.0  4611686018427387901
  // 4.0.0.0  9223372036854775804

  const uint64_t partition_size = CASS_UINT64_MAX / num_hosts;
  Murmur3Partitioner::Token token = CASS_INT64_MIN + static_cast<int64_t>(partition_size);

  for (size_t i = 1; i <= num_hosts; ++i) {
    Host::Ptr host(create_host(addr_for_sequence(i), single_token(token),
                               Murmur3Partitioner::name().to_string(), "rack1", LOCAL_DC));

    hosts[host->address()] = host;
    token_map->add_host(host);
    token += partition_size;
  }

  add_keyspace_simple("test", 3, token_map.get());
  token_map->build();

  QueryRequest::Ptr request(new QueryRequest("", 1));
  const char* value = "kjdfjkldsdjkl"; // hash: 9024137376112061887
  request->set(0, CassString(value, strlen(value)));
  request->add_key_index(0);
  SharedRefPtr<RequestHandler> request_handler(new RequestHandler(request, ResponseFuture::Ptr()));

  HostVec not_shuffled;
  {
    TokenAwarePolicy policy(new RoundRobinPolicy(), false); // Not shuffled
    policy.init(SharedRefPtr<Host>(), hosts, &random, "");
    ScopedPtr<QueryPlan> qp1(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    for (int i = 0; i < num_hosts; ++i) {
      not_shuffled.push_back(qp1->compute_next());
    }

    // Verify that not shuffled will repeat the same order
    HostVec not_shuffled_again;
    ScopedPtr<QueryPlan> qp2(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    for (int i = 0; i < num_hosts; ++i) {
      not_shuffled_again.push_back(qp2->compute_next());
    }
    EXPECT_EQ(not_shuffled_again, not_shuffled);
  }

  // Verify that the shuffle setting does indeed shuffle the replicas
  {
    TokenAwarePolicy shuffle_policy(new RoundRobinPolicy(), true); // Shuffled
    shuffle_policy.init(SharedRefPtr<Host>(), hosts, &random, "");

    HostVec shuffled_previous;
    ScopedPtr<QueryPlan> qp(
        shuffle_policy.new_query_plan("test", request_handler.get(), token_map.get()));
    for (int i = 0; i < num_hosts; ++i) {
      shuffled_previous.push_back(qp->compute_next());
    }

    int count;
    const int max_iterations = num_hosts * num_hosts;
    for (count = 0; count < max_iterations; ++count) {
      ScopedPtr<QueryPlan> qp(
          shuffle_policy.new_query_plan("test", request_handler.get(), token_map.get()));

      HostVec shuffled;
      for (int j = 0; j < num_hosts; ++j) {
        Host::Ptr host(qp->compute_next());
        EXPECT_GT(std::count(not_shuffled.begin(), not_shuffled.end(), host), 0);
        shuffled.push_back(host);
      }
      // Exit if we prove that we shuffled the hosts
      if (shuffled != not_shuffled && shuffled != shuffled_previous) {
        break;
      }
    }

    EXPECT_NE(count, max_iterations);
  }
}

TEST(LatencyAwareLoadBalancingUnitTest, ThreadholdToAccount) {
  const uint64_t scale = 100LL;
  const uint64_t min_measured = 15LL;
  const uint64_t threshold_to_account = (30LL * min_measured) / 100LL;
  const uint64_t one_ms = 1000000LL; // 1 ms in ns

  Host host(Address("0.0.0.0", 9042));
  host.enable_latency_tracking(scale, min_measured);

  TimestampedAverage current = host.get_current_average();
  for (uint64_t i = 0; i < threshold_to_account; ++i) {
    host.update_latency(one_ms);
    current = host.get_current_average();
    EXPECT_EQ(current.num_measured, i + 1);
    EXPECT_EQ(current.average, -1);
  }

  host.update_latency(one_ms);
  current = host.get_current_average();
  EXPECT_EQ(current.num_measured, threshold_to_account + 1);
  EXPECT_EQ(current.average, static_cast<int64_t>(one_ms));
}

TEST(LatencyAwareLoadBalancingUnitTest, MovingAverage) {
  const uint64_t one_ms = 1000000LL; // 1 ms in ns

  // Verify average is approx. the same when recording the same latency twice
  EXPECT_NEAR(static_cast<double>(calculate_moving_average(one_ms, one_ms, 100LL)),
              static_cast<double>(one_ms), 0.2 * one_ms);

  EXPECT_NEAR(static_cast<double>(calculate_moving_average(one_ms, one_ms, 1000LL)),
              static_cast<double>(one_ms), 0.2 * one_ms);

  // First average is 100 us and second average is 50 us, expect a 75 us average approx.
  // after a short wait time. This has a high tolerance because the time waited varies.
  EXPECT_NEAR(
      static_cast<double>(calculate_moving_average(one_ms, one_ms / 2LL, 50LL)),
      static_cast<double>((3LL * one_ms) / 4LL),
      50.0 * one_ms); // Highly variable because it's in the early part of the logarithmic curve

  // First average is 100 us and second average is 50 us, expect a 50 us average approx.
  // after a longer wait time. This has a high tolerance because the time waited varies
  EXPECT_NEAR(static_cast<double>(calculate_moving_average(one_ms, one_ms / 2LL, 100000LL)),
              static_cast<double>(one_ms / 2LL), 2.0 * one_ms);
}

#if _MSC_VER == 1700 && _M_IX86
TEST(LatencyAwareLoadBalancingUnitTest,
     DISABLED_Simple) { // Disabled: See https://datastax-oss.atlassian.net/browse/CPP-654
#else
TEST(LatencyAwareLoadBalancingUnitTest, Simple) {
#endif
  LatencyAwarePolicy::Settings settings;

  // Disable min_measured
  settings.min_measured = 0L;

  // Latencies can't exceed 2x the minimum latency
  settings.exclusion_threshold = 2.0;

  // Set the retry period to 1 second
  settings.retry_period_ns = 1000LL * 1000LL * 1000L;

  const int64_t num_hosts = 4;
  HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);
  LatencyAwarePolicy policy(new RoundRobinPolicy(), settings);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  // Record some latencies with 100 ns being the minimum
  for (HostMap::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    i->second->enable_latency_tracking(settings.scale_ns, settings.min_measured);
  }

  hosts[Address("1.0.0.0", 9042)]->update_latency(100);
  hosts[Address("4.0.0.0", 9042)]->update_latency(150);

  // Hosts 2 and 3 will exceed the exclusion threshold
  hosts[Address("2.0.0.0", 9042)]->update_latency(201);
  hosts[Address("3.0.0.0", 9042)]->update_latency(1000);

  // Verify we don't have a current minimum average
  EXPECT_EQ(policy.min_average(), -1);

  // Run minimum average calculation
  RunPeriodicTask task(&policy);
  ASSERT_EQ(task.init(), 0);
  task.run();

  // Wait for task to run (minimum average calculation will happen after 100 ms)
  test::Utils::msleep(150);

  task.done();
  task.join();

  // Verify current minimum average
  EXPECT_EQ(policy.min_average(), 100);

  // 1 and 4  are under the minimum, but 2 and 3 will be skipped
  {
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("", NULL, NULL));
    const size_t seq1[] = { 1, 4, 2, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  }

  // Exceed retry period
  test::Utils::msleep(1000); // 1 second

  // After waiting no hosts should be skipped (notice 2 and 3 tried first)
  {
    ScopedPtr<QueryPlan> qp(policy.new_query_plan("", NULL, NULL));
    const size_t seq1[] = { 2, 3, 4, 1 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  }
}

#if _MSC_VER == 1700 && _M_IX86
TEST(LatencyAwareLoadBalancingUnitTest,
     DISABLED_MinAverageUnderMinMeasured) { // Disabled: See
                                            // https://datastax-oss.atlassian.net/browse/CPP-654
#else
TEST(LatencyAwareLoadBalancingUnitTest, MinAverageUnderMinMeasured) {
#endif
  LatencyAwarePolicy::Settings settings;

  const int64_t num_hosts = 4;
  HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);
  LatencyAwarePolicy policy(new RoundRobinPolicy(), settings);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  int count = 1;
  for (HostMap::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    i->second->enable_latency_tracking(settings.scale_ns, settings.min_measured);
    i->second->update_latency(100 * count++);
  }

  // Verify we don't have a current minimum average
  EXPECT_EQ(policy.min_average(), -1);

  // Run minimum average calculation
  RunPeriodicTask task(&policy);
  ASSERT_EQ(task.init(), 0);
  task.run();

  // Wait for task to run (minimum average calculation will happen after 100 ms)
  test::Utils::msleep(150);

  task.done();
  task.join();

  // No hosts have the minimum measured
  EXPECT_EQ(policy.min_average(), -1);
}

TEST(WhitelistLoadBalancingUnitTest, Hosts) {
  const int64_t num_hosts = 100;
  HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);
  ContactPointList whitelist_hosts;
  whitelist_hosts.push_back("37.0.0.0");
  whitelist_hosts.push_back("83.0.0.0");
  WhitelistPolicy policy(new RoundRobinPolicy(), whitelist_hosts);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  // Verify only hosts 37 and 83 are computed in the query plan
  const size_t seq1[] = { 37, 83 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  // The query plan should now be exhausted
  Address next_address;
  ASSERT_FALSE(qp.get()->compute_next(&next_address));
}

TEST(WhitelistLoadBalancingUnitTest, Datacenters) {
  HostMap hosts;
  populate_hosts(3, "rack1", LOCAL_DC, &hosts);
  populate_hosts(3, "rack1", BACKUP_DC, &hosts);
  populate_hosts(3, "rack1", REMOTE_DC, &hosts);
  DcList whitelist_dcs;
  whitelist_dcs.push_back(LOCAL_DC);
  whitelist_dcs.push_back(REMOTE_DC);
  WhitelistDCPolicy policy(new RoundRobinPolicy(), whitelist_dcs);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  // Verify only hosts LOCAL_DC and REMOTE_DC are computed in the query plan
  const size_t seq1[] = { 1, 2, 3, 7, 8, 9 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  // The query plan should now be exhausted
  Address next_address;
  ASSERT_FALSE(qp.get()->compute_next(&next_address));
}

TEST(BlacklistLoadBalancingUnitTest, Hosts) {
  const int64_t num_hosts = 5;
  HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);
  ContactPointList blacklist_hosts;
  blacklist_hosts.push_back("2.0.0.0");
  blacklist_hosts.push_back("3.0.0.0");
  BlacklistPolicy policy(new RoundRobinPolicy(), blacklist_hosts);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  // Verify only hosts 1, 4 and 5 are computed in the query plan
  const size_t seq1[] = { 1, 4, 5 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  // The query plan should now be exhausted
  Address next_address;
  ASSERT_FALSE(qp.get()->compute_next(&next_address));
}

TEST(BlacklistLoadBalancingUnitTest, Datacenters) {
  HostMap hosts;
  populate_hosts(3, "rack1", LOCAL_DC, &hosts);
  populate_hosts(3, "rack1", BACKUP_DC, &hosts);
  populate_hosts(3, "rack1", REMOTE_DC, &hosts);
  DcList blacklist_dcs;
  blacklist_dcs.push_back(LOCAL_DC);
  blacklist_dcs.push_back(REMOTE_DC);
  BlacklistDCPolicy policy(new RoundRobinPolicy(), blacklist_dcs);
  policy.init(SharedRefPtr<Host>(), hosts, NULL, "");

  ScopedPtr<QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  // Verify only hosts from BACKUP_DC are computed in the query plan
  const size_t seq1[] = { 4, 5, 6 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  // The query plan should now be exhausted
  Address next_address;
  ASSERT_FALSE(qp.get()->compute_next(&next_address));
}
