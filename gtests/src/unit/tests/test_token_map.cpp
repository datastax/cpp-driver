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

#include "test_token_map_utils.hpp"
#include "map.hpp"
#include "set.hpp"

namespace {

template <class Partitioner>
struct TestTokenMap {
  typedef typename cass::ReplicationStrategy<Partitioner>::Token Token;
  typedef cass::Map<Token, cass::Host::Ptr> TokenHostMap;

  TokenHostMap tokens;
  cass::TokenMap::Ptr token_map;

  TestTokenMap()
    : token_map(cass::TokenMap::from_partitioner(Partitioner::name())) { }

  void add_host(const cass::Host::Ptr& host) {
    for (cass::Vector<cass::String>::const_iterator i = host->tokens().begin(),
         end = host->tokens().end(); i != end; ++i) {
      const cass::String v(*i);
      tokens[Partitioner::from_string(*i)] = host;
    }
  }

  void build(const cass::String& keyspace_name = "ks", size_t replication_factor = 3) {
    add_keyspace_simple(keyspace_name, replication_factor, token_map.get());
    for (typename TokenHostMap::const_iterator i = tokens.begin(),
         end = tokens.end(); i != end; ++i) {
      token_map->add_host(i->second);
    }
    token_map->build();
  }

  const cass::Host::Ptr& get_replica(const cass::String& key) {
    typename TokenHostMap::const_iterator i = tokens.upper_bound(Partitioner::hash(key));
    if (i != tokens.end()) {
      return i->second;
    } else {
      return tokens.begin()->second;
    }
  }

  void verify(const cass::String& keyspace_name = "ks") {
    const cass::String keys[] = { "test", "abc", "def", "a", "b", "c", "d" };

    for (size_t i = 0; i < sizeof(keys)/sizeof(keys[0]); ++i) {
      const cass::String& key = keys[i];

      const cass::CopyOnWriteHostVec& hosts = token_map->get_replicas(keyspace_name, key);
      ASSERT_TRUE(hosts);
      ASSERT_GT(hosts->size(), 0);

      const cass::Host::Ptr& host = get_replica(key);
      ASSERT_TRUE(host);

      EXPECT_EQ(hosts->front()->address(), host->address());
    }
  }
};

} // namespace

TEST(TokenMapUnitTest, Murmur3)
{
  TestTokenMap<cass::Murmur3Partitioner> test_murmur3;

  test_murmur3.add_host(create_host("1.0.0.1", single_token(CASS_INT64_MIN / 2)));
  test_murmur3.add_host(create_host("1.0.0.2", single_token(0)));
  test_murmur3.add_host(create_host("1.0.0.3", single_token(CASS_INT64_MAX / 2)));

  test_murmur3.build();
  test_murmur3.verify();
}

TEST(TokenMapUnitTest, Murmur3MultipleTokensPerHost)
{
  TestTokenMap<cass::Murmur3Partitioner> test_murmur3;

  const size_t tokens_per_host = 256;
  MT19937_64 rng;

  test_murmur3.add_host(create_host("1.0.0.1", random_murmur3_tokens(rng, tokens_per_host)));
  test_murmur3.add_host(create_host("1.0.0.2", random_murmur3_tokens(rng, tokens_per_host)));
  test_murmur3.add_host(create_host("1.0.0.3", random_murmur3_tokens(rng, tokens_per_host)));
  test_murmur3.add_host(create_host("1.0.0.4", random_murmur3_tokens(rng, tokens_per_host)));

  test_murmur3.build();
  test_murmur3.verify();
}

TEST(TokenMapUnitTest, Murmur3LargeNumberOfVnodes)
{
  TestTokenMap<cass::Murmur3Partitioner> test_murmur3;

  size_t num_dcs = 3;
  size_t num_racks = 3;
  size_t num_hosts = 4;
  size_t num_vnodes = 256;
  size_t replication_factor = 3;

  ReplicationMap replication;
  MT19937_64 rng;
  cass::TokenMap* token_map = test_murmur3.token_map.get();

  // Populate tokens
  int host_count = 1;
  for (size_t i = 1; i <= num_dcs; ++i) {
    char dc[32];
    sprintf(dc, "dc%d", (int)i);
    char rf[32];
    sprintf(rf, "%d", (int)replication_factor);
    replication[dc] = rf;

    for (size_t j = 1; j <= num_racks; ++j) {
      char rack[32];
      sprintf(rack, "rack%d", (int)j);

      for (size_t k = 1; k <= num_hosts; ++k) {
        char ip[32];
        sprintf(ip, "127.0.%d.%d", host_count / 255, host_count % 255);
        host_count++;

        cass::Host::Ptr host(create_host(ip,
                                         random_murmur3_tokens(rng, num_vnodes),
                                         cass::Murmur3Partitioner::name().to_string(),
                                         rack, dc));

        test_murmur3.add_host(host);
        token_map->add_host(host);
      }
    }
  }

  // Build token map
  add_keyspace_network_topology("ks1", replication, token_map);
  token_map->build();

  const cass::String keys[] = { "test", "abc", "def", "a", "b", "c", "d" };

  for (size_t i = 0; i < sizeof(keys)/sizeof(keys[0]); ++i) {
    const cass::String& key = keys[i];

    const cass::CopyOnWriteHostVec& hosts = token_map->get_replicas("ks1", key);
    ASSERT_TRUE(hosts && hosts->size() == replication_factor * num_dcs);

    typedef cass::Map<cass::String, cass::Set<cass::String> > DcRackMap;

    // Verify rack counts
    DcRackMap dc_racks;
    for (cass::HostVec::const_iterator i = hosts->begin(),
         end = hosts->end(); i != end; ++i) {
      const cass::Host::Ptr& host = (*i);
      dc_racks[host->dc()].insert(host->rack());
    }
    EXPECT_EQ(dc_racks.size(), num_dcs);

    for (DcRackMap::const_iterator i = dc_racks.begin(),
         end = dc_racks.end(); i != end; ++i) {
      EXPECT_GE(i->second.size(), std::min(num_racks, replication_factor));
    }

    // Verify replica
    cass::Host::Ptr host = test_murmur3.get_replica(key);
    ASSERT_TRUE(host);

    EXPECT_EQ((*hosts)[0]->address(), host->address());
  }
}

TEST(TokenMapUnitTest, Random)
{
  cass::TokenMap::Ptr token_map(cass::TokenMap::from_partitioner(cass::RandomPartitioner::name()));

  TestTokenMap<cass::RandomPartitioner> test_random;

  test_random.add_host(create_host("1.0.0.1", single_token(create_random_token("42535295865117307932921825928971026432")))); // 2^127 / 4
  test_random.add_host(create_host("1.0.0.2", single_token(create_random_token("85070591730234615865843651857942052864")))); // 2^127 / 2
  test_random.add_host(create_host("1.0.0.3", single_token(create_random_token("1605887595351923798765477786913079296")))); // 2^127 * 3 / 4

  test_random.build();
  test_random.verify();
}

TEST(TokenMapUnitTest, ByteOrdered)
{
  cass::TokenMap::Ptr token_map(cass::TokenMap::from_partitioner(cass::ByteOrderedPartitioner::name()));

  TestTokenMap<cass::ByteOrderedPartitioner> test_byte_ordered;

  test_byte_ordered.add_host(create_host("1.0.0.1", single_token(create_byte_ordered_token("g"))));
  test_byte_ordered.add_host(create_host("1.0.0.2", single_token(create_byte_ordered_token("m"))));
  test_byte_ordered.add_host(create_host("1.0.0.3", single_token(create_byte_ordered_token("s"))));

  test_byte_ordered.build();
  test_byte_ordered.verify();
}

TEST(TokenMapUnitTest, RemoveHost)
{
  TestTokenMap<cass::Murmur3Partitioner> test_remove_host;

  test_remove_host.add_host(create_host("1.0.0.1", single_token(CASS_INT64_MIN / 2)));
  test_remove_host.add_host(create_host("1.0.0.2", single_token(0)));
  test_remove_host.add_host(create_host("1.0.0.3", single_token(CASS_INT64_MAX / 2)));

  test_remove_host.build("ks", 2);
  test_remove_host.verify();

  cass::TokenMap* token_map = test_remove_host.token_map.get();

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    ASSERT_TRUE(replicas && replicas->size() == 2);
    EXPECT_EQ((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    EXPECT_EQ((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
  }

  TestTokenMap<cass::Murmur3Partitioner>::TokenHostMap::iterator host_to_remove_it = test_remove_host.tokens.begin();

  token_map->remove_host_and_build(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    ASSERT_TRUE(replicas && replicas->size() == 2);
    EXPECT_EQ((*replicas)[0]->address(), cass::Address("1.0.0.2", 9042));
    EXPECT_EQ((*replicas)[1]->address(), cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map->remove_host_and_build(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    ASSERT_TRUE(replicas && replicas->size() == 1);
    EXPECT_EQ((*replicas)[0]->address(), cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map->remove_host_and_build(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("test", "abc");

    EXPECT_FALSE(replicas);
  }
}

TEST(TokenMapUnitTest, UpdateHost)
{
  TestTokenMap<cass::Murmur3Partitioner> test_update_host;

  test_update_host.add_host(create_host("1.0.0.1", single_token(CASS_INT64_MIN / 2)));
  test_update_host.add_host(create_host("1.0.0.2", single_token(CASS_INT64_MIN / 4)));

  test_update_host.build("ks", 4);
  test_update_host.verify();

  cass::TokenMap* token_map = test_update_host.token_map.get();

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    ASSERT_TRUE(replicas && replicas->size() == 2);
    EXPECT_EQ((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    EXPECT_EQ((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
  }

  {
    cass::Host::Ptr host(create_host("1.0.0.3", single_token(0)));
    test_update_host.add_host(host);
    token_map->update_host_and_build(host);
  }

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    ASSERT_TRUE(replicas && replicas->size() == 3);
    EXPECT_EQ((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    EXPECT_EQ((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
    EXPECT_EQ((*replicas)[2]->address(), cass::Address("1.0.0.3", 9042));
  }

  {
    cass::Host::Ptr host(create_host("1.0.0.4", single_token(CASS_INT64_MAX / 2)));
    test_update_host.add_host(host);
    token_map->update_host_and_build(host);
  }

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    ASSERT_TRUE(replicas && replicas->size() == 4);
    EXPECT_EQ((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    EXPECT_EQ((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
    EXPECT_EQ((*replicas)[2]->address(), cass::Address("1.0.0.3", 9042));
    EXPECT_EQ((*replicas)[3]->address(), cass::Address("1.0.0.4", 9042));
  }
}

/**
 * Add/Remove hosts from a token map (using Murmur3 tokens)
 *
 * This test will verify that adding and removing hosts from a token map
 * correctly updates the tokens array.
 *
 * @jira_ticket CPP-464
 * @test_category token_map
 * @expected_results Host's tokens should be added and removed from the token map.
 */
TEST(TokenMapUnitTest, UpdateRemoveHostsMurmur3)
{
  cass::TokenMapImpl<cass::Murmur3Partitioner> token_map;

  // Add hosts and build token map
  Murmur3TokenVec tokens1;
  tokens1.push_back(-3LL);
  tokens1.push_back(-1LL);
  tokens1.push_back(1LL);
  tokens1.push_back(3LL);

  cass::Host::Ptr host1(create_host("1.0.0.1",
                                    murmur3_tokens(tokens1),
                                    cass::Murmur3Partitioner::name().to_string(),
                                    "rack1", "dc1"));

  token_map.add_host(host1);

  Murmur3TokenVec tokens2;
  tokens2.push_back(-4LL);
  tokens2.push_back(-2LL);
  tokens2.push_back(2LL);
  tokens2.push_back(4LL);

  cass::Host::Ptr host2(create_host("1.0.0.2",
                                    murmur3_tokens(tokens2),
                                    cass::Murmur3Partitioner::name().to_string(),
                                    "rack1", "dc2"));

  token_map.add_host(host2);

  ReplicationMap replication;

  replication["dc1"] = "1";
  replication["dc2"] = "1";

  add_keyspace_network_topology("ks1", replication, &token_map);

  token_map.build();

  // Verify all tokens are added to the array
  EXPECT_TRUE(token_map.contains(-3LL));
  EXPECT_TRUE(token_map.contains(-1LL));
  EXPECT_TRUE(token_map.contains(1LL));
  EXPECT_TRUE(token_map.contains(3LL));

  EXPECT_TRUE(token_map.contains(-4LL));
  EXPECT_TRUE(token_map.contains(-2LL));
  EXPECT_TRUE(token_map.contains(2LL));
  EXPECT_TRUE(token_map.contains(4LL));

  // Remove host1 and check that its tokens have been removed
  token_map.remove_host_and_build(host1);

  EXPECT_FALSE(token_map.contains(-3LL));
  EXPECT_FALSE(token_map.contains(-1LL));
  EXPECT_FALSE(token_map.contains(1LL));
  EXPECT_FALSE(token_map.contains(3LL));

  EXPECT_TRUE(token_map.contains(-4LL));
  EXPECT_TRUE(token_map.contains(-2LL));
  EXPECT_TRUE(token_map.contains(2LL));
  EXPECT_TRUE(token_map.contains(4LL));

  // Add host1 and check that its tokens have been added (same as the initial state)
  token_map.update_host_and_build(host1);

  EXPECT_TRUE(token_map.contains(-3LL));
  EXPECT_TRUE(token_map.contains(-1LL));
  EXPECT_TRUE(token_map.contains(1LL));
  EXPECT_TRUE(token_map.contains(3LL));

  EXPECT_TRUE(token_map.contains(-4LL));
  EXPECT_TRUE(token_map.contains(-2LL));
  EXPECT_TRUE(token_map.contains(2LL));
  EXPECT_TRUE(token_map.contains(4LL));

  // Remove host2 and check that its tokens have been removed
  token_map.remove_host_and_build(host2);

  EXPECT_TRUE(token_map.contains(-3LL));
  EXPECT_TRUE(token_map.contains(-1LL));
  EXPECT_TRUE(token_map.contains(1LL));
  EXPECT_TRUE(token_map.contains(3LL));

  EXPECT_FALSE(token_map.contains(-4LL));
  EXPECT_FALSE(token_map.contains(-2LL));
  EXPECT_FALSE(token_map.contains(2LL));
  EXPECT_FALSE(token_map.contains(4LL));

  // Add host2 and check that its tokens have been added (same as the initial state)
  token_map.update_host_and_build(host2);

  EXPECT_TRUE(token_map.contains(-3LL));
  EXPECT_TRUE(token_map.contains(-1LL));
  EXPECT_TRUE(token_map.contains(1LL));
  EXPECT_TRUE(token_map.contains(3LL));

  EXPECT_TRUE(token_map.contains(-4LL));
  EXPECT_TRUE(token_map.contains(-2LL));
  EXPECT_TRUE(token_map.contains(2LL));
  EXPECT_TRUE(token_map.contains(4LL));
}

TEST(TokenMapUnitTest, DropKeyspace)
{
  TestTokenMap<cass::Murmur3Partitioner> test_drop_keyspace;

  test_drop_keyspace.add_host(create_host("1.0.0.1", single_token(CASS_INT64_MIN / 2)));
  test_drop_keyspace.add_host(create_host("1.0.0.2", single_token(0)));
  test_drop_keyspace.add_host(create_host("1.0.0.3", single_token(CASS_INT64_MAX / 2)));

  test_drop_keyspace.build("ks", 2);
  test_drop_keyspace.verify();

  cass::TokenMap* token_map = test_drop_keyspace.token_map.get();

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    ASSERT_TRUE(replicas && replicas->size() == 2);
    EXPECT_EQ((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    EXPECT_EQ((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
  }

  token_map->drop_keyspace("ks");

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    EXPECT_FALSE(replicas);
  }
}
