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
#include <string>
#include <vector>

#include "address.hpp"
#include "constants.hpp"
#include "dc_aware_policy.hpp"
#include "latency_aware_policy.hpp"
#include "loop_thread.hpp"
#include "murmur3.hpp"
#include "query_request.hpp"
#include "random.hpp"
#include "request_handler.hpp"
#include "scoped_ptr.hpp"
#include "token_aware_policy.hpp"
#include "whitelist_policy.hpp"
#include "blacklist_policy.hpp"
#include "whitelist_dc_policy.hpp"
#include "blacklist_dc_policy.hpp"

#include "test_token_map_utils.hpp"
#include "test_utils.hpp"

#include <uv.h>

const std::string LOCAL_DC = "local";
const std::string REMOTE_DC = "remote";
const std::string BACKUP_DC = "backup";

#define VECTOR_FROM(t, a) std::vector<t>(a, a + sizeof(a)/sizeof(a[0]))

cass::Address addr_for_sequence(size_t i) {
  char temp[64];
  sprintf(temp, "%d.%d.%d.%d",
          static_cast<int>(i & 0xFF),
          static_cast<int>((i >> 8) & 0xFF),
          static_cast<int>((i >> 16) & 0xFF),
          static_cast<int>((i >> 24) & 0xFF));
  return cass::Address(temp, 9042);
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
  cass::Address received;
  for (std::vector<size_t>::const_iterator it = sequence.begin();
                                           it!= sequence.end();
                                         ++it) {
    ASSERT_TRUE(qp->compute_next(&received));
    EXPECT_EQ(addr_for_sequence(*it), received);
  }
  EXPECT_FALSE(qp->compute_next(&received));
}

struct RunPeriodicTask : public cass::LoopThread {
  RunPeriodicTask(cass::LatencyAwarePolicy* policy)
    : policy(policy) {
    async.data = this;
  }

  int init() {
    int rc = cass::LoopThread::init();
    if (rc != 0) return rc;
    rc = uv_async_init(loop(), &async, on_async);
    if (rc != 0) return rc;
    policy->register_handles(loop());
    return rc;
  }

  void done() {
    uv_async_send(&async);
  }

#if UV_VERSION_MAJOR == 0
  static void on_async(uv_async_t *handle, int status) {
#else
  static void on_async(uv_async_t *handle) {
#endif
    RunPeriodicTask* task = static_cast<RunPeriodicTask*>(handle->data);
    task->close_handles();
    task->policy->close_handles();
    uv_close(reinterpret_cast<uv_handle_t*>(&task->async), NULL);
  }

  uv_async_t async;
  cass::LatencyAwarePolicy* policy;
};

// Latency-aware utility functions

// Don't make "time_between_ns" too high because it spin waits
uint64_t calculate_moving_average(uint64_t first_latency_ns,
                                  uint64_t second_latency_ns,
                                  uint64_t time_between_ns) {
  const uint64_t scale = 100LL;
  const uint64_t min_measured = 15LL;
  const uint64_t threshold_to_account = (30LL * min_measured) / 100LL;

  cass::Host host(cass::Address("0.0.0.0", 9042), false);
  host.enable_latency_tracking(scale, min_measured);

  for (uint64_t i = 0; i < threshold_to_account; ++i) {
    host.update_latency(0); // This can be anything because it's not recorded
  }

  host.update_latency(first_latency_ns);

  // Spin wait
  uint64_t start = uv_hrtime();
  while (uv_hrtime() - start < time_between_ns) {}

  host.update_latency(second_latency_ns);
  cass::TimestampedAverage current = host.get_current_average();
  return current.average;
}

void test_dc_aware_policy(size_t local_count, size_t remote_count) {
  cass::HostMap hosts;
  populate_hosts(local_count, "rack", LOCAL_DC, &hosts);
  populate_hosts(remote_count, "rack", REMOTE_DC, &hosts);
  cass::DCAwarePolicy policy(LOCAL_DC, remote_count, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  const size_t total_hosts = local_count + remote_count;

  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  std::vector<size_t> seq(total_hosts);
  for (size_t i = 0; i < total_hosts; ++i) seq[i] = i + 1;
  verify_sequence(qp.get(), seq);
}

TEST(RoundRobinLoadBalancingUnitTest, Simple) {
  cass::HostMap hosts;
  populate_hosts(2, "rack", "dc", &hosts);

  cass::RoundRobinPolicy policy;
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  // start on first elem
  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  const size_t seq1[] = {1, 2};
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));

  // rotate starting element
  cass::ScopedPtr<cass::QueryPlan> qp2(policy.new_query_plan("ks", NULL, NULL));
  const size_t seq2[] = {2, 1};
  verify_sequence(qp2.get(), VECTOR_FROM(size_t, seq2));

  // back around
  cass::ScopedPtr<cass::QueryPlan> qp3(policy.new_query_plan("ks", NULL, NULL));
  verify_sequence(qp3.get(), VECTOR_FROM(size_t, seq1));
}

TEST(RoundRobinLoadBalancingUnitTest, OnAdd) {
  cass::HostMap hosts;
  populate_hosts(2, "rack", "dc", &hosts);

  cass::RoundRobinPolicy policy;
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  // baseline
  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  const size_t seq1[] = {1, 2};
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));

  const size_t seq_new = 5;
  cass::Address addr_new = addr_for_sequence(seq_new);
  cass::SharedRefPtr<cass::Host> host = host_for_addr(addr_new);
  policy.on_add(host);

  cass::ScopedPtr<cass::QueryPlan> qp2(policy.new_query_plan("ks", NULL, NULL));
  const size_t seq2[] = {2, seq_new, 1};
  verify_sequence(qp2.get(), VECTOR_FROM(size_t, seq2));
}

TEST(RoundRobinLoadBalancingUnitTest, OnRemove) {
  cass::HostMap hosts;
  populate_hosts(3, "rack", "dc", &hosts);

  cass::RoundRobinPolicy policy;
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  cass::SharedRefPtr<cass::Host> host = hosts.begin()->second;
  policy.on_remove(host);

  cass::ScopedPtr<cass::QueryPlan> qp2(policy.new_query_plan("ks", NULL, NULL));

  // first query plan has it
  // (note: not manipulating Host::state_ for dynamic removal)
  const size_t seq1[] = {1, 2, 3};
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));

  // second one does not
  const size_t seq2[] = {3, 2};
  verify_sequence(qp2.get(), VECTOR_FROM(size_t, seq2));
}

TEST(RoundRobinLoadBalancingUnitTest, OnUpAndDown) {
  cass::HostMap hosts;
  populate_hosts(3, "rack", "dc", &hosts);

  cass::RoundRobinPolicy policy;
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp_before1(policy.new_query_plan("ks", NULL, NULL));
  cass::ScopedPtr<cass::QueryPlan> qp_before2(policy.new_query_plan("ks", NULL, NULL));
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

  cass::ScopedPtr<cass::QueryPlan> qp_after1(policy.new_query_plan("ks", NULL, NULL));
  cass::ScopedPtr<cass::QueryPlan> qp_after2(policy.new_query_plan("ks", NULL, NULL));

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

TEST(DatacenterAwareLoadBalancingUnitTest, SomeDatacenterLocalUnspecified) {
  const size_t total_hosts = 3;
  cass::HostMap hosts;
  populate_hosts(total_hosts, "rack", LOCAL_DC, &hosts);
  cass::Host* h = hosts.begin()->second.get();
  h->set_rack_and_dc("", "");

  cass::DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  const size_t seq[] = {2, 3, 1};
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
}

TEST(DatacenterAwareLoadBalancingUnitTest, SingleLocalDown) {
  cass::HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  cass::SharedRefPtr<cass::Host> target_host = hosts.begin()->second;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);

  cass::DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp_before(policy.new_query_plan("ks", NULL, NULL));// has down host ptr in plan
  target_host->set_down();
  policy.on_down(target_host);
  cass::ScopedPtr<cass::QueryPlan> qp_after(policy.new_query_plan("ks", NULL, NULL));// should not have down host ptr in plan

  {
    const size_t seq[] = {2, 3, 4};
    verify_sequence(qp_before.get(), VECTOR_FROM(size_t, seq));
  }

  {
    const size_t seq[] = {3, 2, 4};// local dc wrapped before remote offered
    verify_sequence(qp_after.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, AllLocalRemovedReturned) {
  cass::HostMap hosts;
  populate_hosts(1, "rack", LOCAL_DC, &hosts);
  cass::SharedRefPtr<cass::Host> target_host = hosts.begin()->second;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);

  cass::DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp_before(policy.new_query_plan("ks", NULL, NULL));// has down host ptr in plan
  target_host->set_down();
  policy.on_down(target_host);
  cass::ScopedPtr<cass::QueryPlan> qp_after(policy.new_query_plan("ks", NULL, NULL));// should not have down host ptr in plan

  {
    const size_t seq[] = {2};
    verify_sequence(qp_before.get(), VECTOR_FROM(size_t, seq));
    verify_sequence(qp_after.get(), VECTOR_FROM(size_t, seq));
  }

  target_host->set_up();
  policy.on_up(target_host);

  // make sure we get the local node first after on_up
  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  {
    const size_t seq[] = {1, 2};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, RemoteRemovedReturned) {
  cass::HostMap hosts;
  populate_hosts(1, "rack", LOCAL_DC, &hosts);
  populate_hosts(1, "rack", REMOTE_DC, &hosts);
  cass::Address target_addr("2.0.0.0", 9042);
  cass::SharedRefPtr<cass::Host> target_host = hosts[target_addr];

  cass::DCAwarePolicy policy(LOCAL_DC, 1, false);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp_before(policy.new_query_plan("ks", NULL, NULL));// has down host ptr in plan
  target_host->set_down();
  policy.on_down(target_host);
  cass::ScopedPtr<cass::QueryPlan> qp_after(policy.new_query_plan("ks", NULL, NULL));// should not have down host ptr in plan

  {
    const size_t seq[] = {1};
    verify_sequence(qp_before.get(), VECTOR_FROM(size_t, seq));
    verify_sequence(qp_after.get(), VECTOR_FROM(size_t, seq));
  }

  target_host->set_up();
  policy.on_up(target_host);

  // make sure we get both nodes, correct order after
  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
  {
    const size_t seq[] = {1, 2};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, UsedHostsPerDatacenter) {
  cass::HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  populate_hosts(3, "rack", REMOTE_DC, &hosts);

  for (size_t used_hosts = 0; used_hosts < 4; ++used_hosts) {
    cass::DCAwarePolicy policy(LOCAL_DC, used_hosts, false);
    policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
    std::vector<size_t> seq;
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
  cass::HostMap hosts;
  populate_hosts(3, "rack", LOCAL_DC, &hosts);
  populate_hosts(3, "rack", REMOTE_DC, &hosts);

  {
    // Not allowing remote DCs for local CLs
    bool allow_remote_dcs_for_local_cl = false;
    cass::DCAwarePolicy policy(LOCAL_DC, 3, !allow_remote_dcs_for_local_cl);
    policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

    // Set local CL
    cass::SharedRefPtr<cass::QueryRequest> request(new cass::QueryRequest("", 0));
    request->set_consistency(CASS_CONSISTENCY_LOCAL_ONE);
    cass::SharedRefPtr<cass::RequestHandler> request_handler(
      new cass::RequestHandler(request, cass::ResponseFuture::Ptr()));

    // Check for only local hosts are used
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", request_handler.get(), NULL));
    const size_t seq[] = {1, 2, 3};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  {
    // Allowing remote DCs for local CLs
    bool allow_remote_dcs_for_local_cl = true;
    cass::DCAwarePolicy policy(LOCAL_DC, 3, !allow_remote_dcs_for_local_cl);
    policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

    // Set local CL
    cass::SharedRefPtr<cass::QueryRequest> request(new cass::QueryRequest("", 0));
    request->set_consistency(CASS_CONSISTENCY_LOCAL_QUORUM);
    cass::SharedRefPtr<cass::RequestHandler> request_handler(
      new cass::RequestHandler(request, cass::ResponseFuture::Ptr()));

    // Check for only local hosts are used
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", request_handler.get(), NULL));
    const size_t seq[] = {1, 2, 3, 4, 5, 6};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(DatacenterAwareLoadBalancingUnitTest, StartWithEmptyLocalDatacenter) {
  cass::HostMap hosts;
  populate_hosts(1, "rack", REMOTE_DC, &hosts);
  populate_hosts(3, "rack", LOCAL_DC, &hosts);

  // Set local DC using connected host
  {
    cass::DCAwarePolicy policy("", 0, false);
    policy.init(hosts[cass::Address("2.0.0.0", 9042)], hosts, NULL);

    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
    const size_t seq[] = {2, 3, 4};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Set local DC using first host with non-empty DC
  {
    cass::DCAwarePolicy policy("", 0, false);
    policy.init(cass::SharedRefPtr<cass::Host>(
                  new cass::Host(cass::Address("0.0.0.0", 9042), false)), hosts, NULL);

    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));
    const size_t seq[] = {1};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(TokenAwareLoadBalancingUnitTest, Simple) {
  const int64_t num_hosts = 4;
  cass::HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);

  // Tokens
  // 1.0.0.0 -4611686018427387905
  // 2.0.0.0 -2
  // 3.0.0.0  4611686018427387901
  // 4.0.0.0  9223372036854775804

  cass::ScopedPtr<cass::TokenMap> token_map(cass::TokenMap::from_partitioner(cass::Murmur3Partitioner::name()));

  uint64_t partition_size = CASS_UINT64_MAX / num_hosts;
  int64_t token = CASS_INT64_MIN + partition_size;
  for (cass::HostMap::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    TokenCollectionBuilder builder;
    builder.append_token(token);
    token_map->add_host(i->second, builder.finish());
    token += partition_size;
  }

  add_keyspace_simple("test", 3, token_map.get());
  token_map->build();

  cass::TokenAwarePolicy policy(new cass::RoundRobinPolicy());
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::SharedRefPtr<cass::QueryRequest> request(new cass::QueryRequest("", 1));
  const char* value = "kjdfjkldsdjkl"; // hash: 9024137376112061887
  request->set(0, cass::CassString(value, strlen(value)));
  request->add_key_index(0);
  cass::SharedRefPtr<cass::RequestHandler> request_handler(
      new cass::RequestHandler(request, cass::ResponseFuture::Ptr()));

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 4, 1, 2, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Bring down the first host
  cass::HostMap::iterator curr_host_it = hosts.begin(); // 1.0.0.0
  curr_host_it->second->set_down();

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
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
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 2, 1, 3 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(TokenAwareLoadBalancingUnitTest, NetworkTopology) {
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

  // Tokens
  // 1.0.0.0 local  -6588122883467697006
  // 2.0.0.0 remote -3952873730080618204
  // 3.0.0.0 local  -1317624576693539402
  // 4.0.0.0 remote  1317624576693539400
  // 5.0.0.0 local   3952873730080618202
  // 6.0.0.0 remote  6588122883467697004
  // 7.0.0.0 local   9223372036854775806

  cass::ScopedPtr<cass::TokenMap> token_map(cass::TokenMap::from_partitioner(cass::Murmur3Partitioner::name()));

  uint64_t partition_size = CASS_UINT64_MAX / num_hosts;
  int64_t token = CASS_INT64_MIN + partition_size;
  for (cass::HostMap::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    TokenCollectionBuilder builder;
    builder.append_token(token);
    token_map->add_host(i->second, builder.finish());
    token += partition_size;
  }

  ReplicationMap replication;
  replication[LOCAL_DC] = "3";
  replication[REMOTE_DC] = "2";
  add_keyspace_network_topology("test", replication, token_map.get());
  token_map->build();

  cass::TokenAwarePolicy policy(new cass::DCAwarePolicy(LOCAL_DC, num_hosts / 2, false));
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::SharedRefPtr<cass::QueryRequest> request(new cass::QueryRequest("", 1));
  const char* value = "abc"; // hash: -5434086359492102041
  request->set(0, cass::CassString(value, strlen(value)));
  request->add_key_index(0);
  cass::SharedRefPtr<cass::RequestHandler> request_handler(
      new cass::RequestHandler(request, cass::ResponseFuture::Ptr()));

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 3, 5, 7, 1, 4, 6, 2 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Bring down the first host
  cass::HostMap::iterator curr_host_it = hosts.begin(); // 1.0.0.0
  curr_host_it->second->set_down();

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 3, 5, 7, 6, 2, 4 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }

  // Restore the first host and bring down the first token aware replica
  curr_host_it->second->set_up();
  ++curr_host_it; // 2.0.0.0
  ++curr_host_it; // 3.0.0.0
  curr_host_it->second->set_down();

  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("test", request_handler.get(), token_map.get()));
    const size_t seq[] = { 5, 7, 1, 2, 4, 6 };
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq));
  }
}

TEST(LatencyAwaryLoadBalancingUnitTest, ThreadholdToAccount) {
  const uint64_t scale = 100LL;
  const uint64_t min_measured = 15LL;
  const uint64_t threshold_to_account = (30LL * min_measured) / 100LL;
  const uint64_t one_ms = 1000000LL; // 1 ms in ns

  cass::Host host(cass::Address("0.0.0.0", 9042), false);
  host.enable_latency_tracking(scale, min_measured);

  cass::TimestampedAverage current = host.get_current_average();
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

TEST(LatencyAwaryLoadBalancingUnitTest, MovingAverage) {
  const uint64_t one_ms = 1000000LL; // 1 ms in ns

  // Verify average is approx. the same when recording the same latency twice
  EXPECT_NEAR(static_cast<double>(calculate_moving_average(one_ms, one_ms, 100LL)),
              static_cast<double>(one_ms),
              0.2 * one_ms);

  EXPECT_NEAR(static_cast<double>(calculate_moving_average(one_ms, one_ms, 1000LL)),
              static_cast<double>(one_ms),
              0.2 * one_ms);

  // First average is 100 us and second average is 50 us, expect a 75 us average approx.
  // after a short wait time. This has a high tolerance because the time waited varies.
  EXPECT_NEAR(static_cast<double>(calculate_moving_average(one_ms, one_ms / 2LL, 50LL)),
              static_cast<double>((3LL * one_ms) / 4LL),
              50.0 * one_ms); // Highly variable because it's in the early part of the logarithmic curve

  // First average is 100 us and second average is 50 us, expect a 50 us average approx.
  // after a longer wait time. This has a high tolerance because the time waited varies
  EXPECT_NEAR(static_cast<double>(calculate_moving_average(one_ms, one_ms / 2LL, 100000LL)),
              static_cast<double>(one_ms / 2LL),
              2.0 * one_ms);
}

TEST(LatencyAwaryLoadBalancingUnitTest, Simple) {
  cass::LatencyAwarePolicy::Settings settings;

  // Disable min_measured
  settings.min_measured = 0L;

  // Latencies can't excceed 2x the minimum latency
  settings.exclusion_threshold = 2.0;

  // Set the retry period to 1 second
  settings.retry_period_ns = 1000LL * 1000LL * 1000L;

  const int64_t num_hosts = 4;
  cass::HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);
  cass::LatencyAwarePolicy policy(new cass::RoundRobinPolicy(), settings);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  // Record some latencies with 100 ns being the minimum
  for (cass::HostMap::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    i->second->enable_latency_tracking(settings.scale_ns, settings.min_measured);
  }

  hosts[cass::Address("1.0.0.0", 9042)]->update_latency(100);
  hosts[cass::Address("4.0.0.0", 9042)]->update_latency(150);

  // Hosts 2 and 3 will excceed the exclusion threshold
  hosts[cass::Address("2.0.0.0", 9042)]->update_latency(201);
  hosts[cass::Address("3.0.0.0", 9042)]->update_latency(1000);

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
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("", NULL, NULL));
    const size_t seq1[] = {1, 4, 2, 3};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  }

  // Excceed retry period
  test::Utils::msleep(1000); // 1 second

  // After waiting no hosts should be skipped (notice 2 and 3 tried first)
  {
    cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("", NULL, NULL));
    const size_t seq1[] = {2, 3, 4, 1};
    verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  }
}

TEST(LatencyAwaryLoadBalancingUnitTest, MinAverageUnderMinMeasured) {
  cass::LatencyAwarePolicy::Settings settings;

  const int64_t num_hosts = 4;
  cass::HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);
  cass::LatencyAwarePolicy policy(new cass::RoundRobinPolicy(), settings);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  int count = 1;
  for (cass::HostMap::iterator i = hosts.begin(); i != hosts.end(); ++i) {
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
  cass::HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);
  cass::ContactPointList whitelist_hosts;
  whitelist_hosts.push_back("37.0.0.0");
  whitelist_hosts.push_back("83.0.0.0");
  cass::WhitelistPolicy policy(new cass::RoundRobinPolicy(), whitelist_hosts);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  // Verify only hosts 37 and 83 are computed in the query plan
  const size_t seq1[] = { 37, 83 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  // The query plan should now be exhausted
  cass::Address next_address;
  ASSERT_FALSE(qp.get()->compute_next(&next_address));
}

TEST(WhitelistLoadBalancingUnitTest, Datacenters) {
  cass::HostMap hosts;
  populate_hosts(3, "rack1", LOCAL_DC, &hosts);
  populate_hosts(3, "rack1", BACKUP_DC, &hosts);
  populate_hosts(3, "rack1", REMOTE_DC, &hosts);
  cass::DcList whitelist_dcs;
  whitelist_dcs.push_back(LOCAL_DC);
  whitelist_dcs.push_back(REMOTE_DC);
  cass::WhitelistDCPolicy policy(new cass::RoundRobinPolicy(), whitelist_dcs);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  // Verify only hosts LOCAL_DC and REMOTE_DC are computed in the query plan
  const size_t seq1[] = { 1, 2, 3, 7, 8, 9 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  // The query plan should now be exhausted
  cass::Address next_address;
  ASSERT_FALSE(qp.get()->compute_next(&next_address));
}

TEST(BlacklistLoadBalancingUnitTest, Hosts) {
  const int64_t num_hosts = 5;
  cass::HostMap hosts;
  populate_hosts(num_hosts, "rack1", LOCAL_DC, &hosts);
  cass::ContactPointList blacklist_hosts;
  blacklist_hosts.push_back("2.0.0.0");
  blacklist_hosts.push_back("3.0.0.0");
  cass::BlacklistPolicy policy(new cass::RoundRobinPolicy(), blacklist_hosts);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  // Verify only hosts 1, 4 and 5 are computed in the query plan
  const size_t seq1[] = { 1, 4, 5 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  // The query plan should now be exhausted
  cass::Address next_address;
  ASSERT_FALSE(qp.get()->compute_next(&next_address));
}

TEST(BlacklistLoadBalancingUnitTest, Datacenters) {
  cass::HostMap hosts;
  populate_hosts(3, "rack1", LOCAL_DC, &hosts);
  populate_hosts(3, "rack1", BACKUP_DC, &hosts);
  populate_hosts(3, "rack1", REMOTE_DC, &hosts);
  cass::DcList blacklist_dcs;
  blacklist_dcs.push_back(LOCAL_DC);
  blacklist_dcs.push_back(REMOTE_DC);
  cass::BlacklistDCPolicy policy(new cass::RoundRobinPolicy(), blacklist_dcs);
  policy.init(cass::SharedRefPtr<cass::Host>(), hosts, NULL);

  cass::ScopedPtr<cass::QueryPlan> qp(policy.new_query_plan("ks", NULL, NULL));

  // Verify only hosts from BACKUP_DC are computed in the query plan
  const size_t seq1[] = { 4, 5, 6 };
  verify_sequence(qp.get(), VECTOR_FROM(size_t, seq1));
  // The query plan should now be exhausted
  cass::Address next_address;
  ASSERT_FALSE(qp.get()->compute_next(&next_address));
}
