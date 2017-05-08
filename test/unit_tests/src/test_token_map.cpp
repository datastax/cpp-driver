/*
  Copyright (c) 2014-2016 DataStax

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

#include "test_token_map_utils.hpp"

#include <boost/test/unit_test.hpp>

namespace {

template <class Partitioner>
struct TestTokenMap {
  typedef typename cass::ReplicationStrategy<Partitioner>::Token Token;
  typedef std::map<Token, cass::Host::Ptr> TokenHostMap;

  TokenHostMap tokens;
  cass::ScopedPtr<cass::TokenMap> token_map;

  TestTokenMap()
    : token_map(cass::TokenMap::from_partitioner(Partitioner::name())) { }

  void build(const std::string& keyspace_name = "ks", size_t replication_factor = 3) {
    add_keyspace_simple(keyspace_name, replication_factor, token_map.get());
    for (typename TokenHostMap::const_iterator i = tokens.begin(),
         end = tokens.end(); i != end; ++i) {
      TokenCollectionBuilder builder;
      builder.append_token(i->first);
      token_map->add_host(i->second, builder.finish());
    }
    token_map->build();
  }

  const cass::Host::Ptr& get_replica(const std::string& key) {
    typename TokenHostMap::const_iterator i = tokens.upper_bound(Partitioner::hash(key));
    if (i != tokens.end()) {
      return i->second;
    } else {
      return tokens.begin()->second;
    }
  }

  void verify(const std::string& keyspace_name = "ks") {
    const std::string keys[] = { "test", "abc", "def", "a", "b", "c", "d" };

    for (size_t i = 0; i < sizeof(keys)/sizeof(keys[0]); ++i) {
      const std::string& key = keys[i];

      const cass::CopyOnWriteHostVec& hosts = token_map->get_replicas(keyspace_name, key);
      BOOST_REQUIRE(hosts && hosts->size() > 0);

      const cass::Host::Ptr& host = get_replica(key);
      BOOST_REQUIRE(host);

      BOOST_CHECK_EQUAL(hosts->front()->address(), host->address());
    }
  }
};

} // namespace

BOOST_AUTO_TEST_SUITE(token_map)

BOOST_AUTO_TEST_CASE(murmur3)
{
  TestTokenMap<cass::Murmur3Partitioner> test_murmur3;

  test_murmur3.tokens[CASS_INT64_MIN / 2] = create_host("1.0.0.1");
  test_murmur3.tokens[0]                  = create_host("1.0.0.2");
  test_murmur3.tokens[CASS_INT64_MAX / 2] = create_host("1.0.0.3");

  test_murmur3.build();
  test_murmur3.verify();
}

BOOST_AUTO_TEST_CASE(murmur3_multiple_tokens_per_host)
{
  TestTokenMap<cass::Murmur3Partitioner> test_murmur3;

  const size_t tokens_per_host = 256;

  cass::HostVec hosts;
  hosts.push_back(create_host("1.0.0.1"));
  hosts.push_back(create_host("1.0.0.2"));
  hosts.push_back(create_host("1.0.0.3"));
  hosts.push_back(create_host("1.0.0.4"));

  MT19937_64 rng;

  for (cass::HostVec::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    for (size_t j = 0; j < tokens_per_host; ++j) {
      test_murmur3.tokens[rng()] = *i;
    }
  }

  test_murmur3.build();
  test_murmur3.verify();
}

BOOST_AUTO_TEST_CASE(murmur3_large_number_of_vnodes)
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
  TestTokenMap<cass::Murmur3Partitioner>::TokenHostMap& tokens = test_murmur3.tokens;

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

        cass::Host::Ptr host(create_host(ip, rack, dc));

        TokenCollectionBuilder builder;
        for (size_t i = 0;  i < num_vnodes; ++i) {
          cass::Murmur3Partitioner::Token token = rng();
          builder.append_token(token);
          tokens[token] = host;
        }
        token_map->add_host(host, builder.finish());
      }
    }
  }

  // Build token map
  add_keyspace_network_topology("ks1", replication, token_map);
  token_map->build();

  const std::string keys[] = { "test", "abc", "def", "a", "b", "c", "d" };

  for (size_t i = 0; i < sizeof(keys)/sizeof(keys[0]); ++i) {
    const std::string& key = keys[i];

    const cass::CopyOnWriteHostVec& hosts = token_map->get_replicas("ks1", key);
    BOOST_REQUIRE(hosts && hosts->size() == replication_factor * num_dcs);

    typedef std::map<std::string, std::set<std::string> > DcRackMap;

    // Verify rack counts
    DcRackMap dc_racks;
    for (cass::HostVec::const_iterator i = hosts->begin(),
         end = hosts->end(); i != end; ++i) {
      const cass::Host::Ptr& host = (*i);
      dc_racks[host->dc()].insert(host->rack());
    }
    BOOST_CHECK(dc_racks.size() == num_dcs);

    for (DcRackMap::const_iterator i = dc_racks.begin(),
         end = dc_racks.end(); i != end; ++i) {
      BOOST_CHECK(i->second.size() >= std::min(num_racks, replication_factor));
    }

    // Verify replica
    cass::Host::Ptr host = test_murmur3.get_replica(key);
    BOOST_REQUIRE(host);

    BOOST_CHECK_EQUAL((*hosts)[0]->address(), host->address());
  }
}

BOOST_AUTO_TEST_CASE(random)
{
  cass::ScopedPtr<cass::TokenMap> token_map(cass::TokenMap::from_partitioner(cass::RandomPartitioner::name()));

  TestTokenMap<cass::RandomPartitioner> test_random;

  test_random.tokens[create_random_token("42535295865117307932921825928971026432")] = create_host("1.0.0.1");  // 2^127 / 4
  test_random.tokens[create_random_token("85070591730234615865843651857942052864")] = create_host("1.0.0.2");  // 2^127 / 2
  test_random.tokens[create_random_token("1605887595351923798765477786913079296")]  = create_host("1.0.0.3"); // 2^127 * 3 / 4

  test_random.build();
  test_random.verify();
}

BOOST_AUTO_TEST_CASE(byte_ordered)
{
  cass::ScopedPtr<cass::TokenMap> token_map(cass::TokenMap::from_partitioner(cass::ByteOrderedPartitioner::name()));

  TestTokenMap<cass::ByteOrderedPartitioner> test_byte_ordered;

  test_byte_ordered.tokens[create_byte_ordered_token("g")] = create_host("1.0.0.1");
  test_byte_ordered.tokens[create_byte_ordered_token("m")] = create_host("1.0.0.2");
  test_byte_ordered.tokens[create_byte_ordered_token("s")] = create_host("1.0.0.3");

  test_byte_ordered.build();
  test_byte_ordered.verify();
}

BOOST_AUTO_TEST_CASE(remove_host)
{
  TestTokenMap<cass::Murmur3Partitioner> test_remove_host;

  test_remove_host.tokens[CASS_INT64_MIN / 2] = create_host("1.0.0.1");
  test_remove_host.tokens[0]                  = create_host("1.0.0.2");
  test_remove_host.tokens[CASS_INT64_MAX / 2] = create_host("1.0.0.3");

  test_remove_host.build("ks", 2);
  test_remove_host.verify();

  cass::TokenMap* token_map = test_remove_host.token_map.get();

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 2);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    BOOST_CHECK_EQUAL((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
  }

  TestTokenMap<cass::Murmur3Partitioner>::TokenHostMap::iterator host_to_remove_it = test_remove_host.tokens.begin();

  token_map->remove_host_and_build(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 2);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.2", 9042));
    BOOST_CHECK_EQUAL((*replicas)[1]->address(), cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map->remove_host_and_build(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 1);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map->remove_host_and_build(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("test", "abc");

    BOOST_CHECK(!replicas);
  }
}

BOOST_AUTO_TEST_CASE(update_host)
{
  TestTokenMap<cass::Murmur3Partitioner> test_update_host;

  test_update_host.tokens[CASS_INT64_MIN / 2] = create_host("1.0.0.1");
  test_update_host.tokens[CASS_INT64_MIN / 4] = create_host("1.0.0.2");

  test_update_host.build("ks", 4);
  test_update_host.verify();

  cass::TokenMap* token_map = test_update_host.token_map.get();

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 2);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    BOOST_CHECK_EQUAL((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
  }

  {
    cass::Murmur3Partitioner::Token token = 0;
    cass::Host::Ptr host(create_host("1.0.0.3"));

    test_update_host.tokens[token] = host;

    TokenCollectionBuilder builder;
    builder.append_token(token);
    token_map->update_host_and_build(host, builder.finish());
  }

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 3);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    BOOST_CHECK_EQUAL((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
    BOOST_CHECK_EQUAL((*replicas)[2]->address(), cass::Address("1.0.0.3", 9042));
  }

  {
    cass::Murmur3Partitioner::Token token = CASS_INT64_MAX / 2;
    cass::Host::Ptr host(create_host("1.0.0.4"));

    test_update_host.tokens[token] = host;

    TokenCollectionBuilder builder;
    builder.append_token(token);
    token_map->update_host_and_build(host, builder.finish());
  }

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 4);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    BOOST_CHECK_EQUAL((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
    BOOST_CHECK_EQUAL((*replicas)[2]->address(), cass::Address("1.0.0.3", 9042));
    BOOST_CHECK_EQUAL((*replicas)[3]->address(), cass::Address("1.0.0.4", 9042));
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
BOOST_AUTO_TEST_CASE(update_remove_hosts_murmur3)
{
  cass::TokenMapImpl<cass::Murmur3Partitioner> token_map;

  // Add hosts and build token map
  cass::Host::Ptr host1(create_host("1.0.0.1", "rack1", "dc1"));

  TokenCollectionBuilder builder1;
  builder1.append_token(-3LL);
  builder1.append_token(-1LL);
  builder1.append_token(1LL);
  builder1.append_token(3LL);

  token_map.add_host(host1, builder1.finish());

  cass::Host::Ptr host2(create_host("1.0.0.2", "rack1", "dc2"));

  TokenCollectionBuilder builder2;
  builder2.append_token(-4LL);
  builder2.append_token(-2LL);
  builder2.append_token(2LL);
  builder2.append_token(4LL);

  token_map.add_host(host2, builder2.finish());

  ReplicationMap replication;

  replication["dc1"] = "1";
  replication["dc2"] = "1";

  add_keyspace_network_topology("ks1", replication, &token_map);

  token_map.build();

  // Verify all tokens are added to the array
  BOOST_CHECK(token_map.contains(-3LL));
  BOOST_CHECK(token_map.contains(-1LL));
  BOOST_CHECK(token_map.contains(1LL));
  BOOST_CHECK(token_map.contains(3LL));

  BOOST_CHECK(token_map.contains(-4LL));
  BOOST_CHECK(token_map.contains(-2LL));
  BOOST_CHECK(token_map.contains(2LL));
  BOOST_CHECK(token_map.contains(4LL));

  // Remove host1 and check that its tokens have been removed
  token_map.remove_host_and_build(host1);

  BOOST_CHECK(!token_map.contains(-3LL));
  BOOST_CHECK(!token_map.contains(-1LL));
  BOOST_CHECK(!token_map.contains(1LL));
  BOOST_CHECK(!token_map.contains(3LL));

  BOOST_CHECK(token_map.contains(-4LL));
  BOOST_CHECK(token_map.contains(-2LL));
  BOOST_CHECK(token_map.contains(2LL));
  BOOST_CHECK(token_map.contains(4LL));

  // Add host1 and check that its tokens have been added (same as the initial state)
  token_map.update_host_and_build(host1, builder1.finish());

  BOOST_CHECK(token_map.contains(-3LL));
  BOOST_CHECK(token_map.contains(-1LL));
  BOOST_CHECK(token_map.contains(1LL));
  BOOST_CHECK(token_map.contains(3LL));

  BOOST_CHECK(token_map.contains(-4LL));
  BOOST_CHECK(token_map.contains(-2LL));
  BOOST_CHECK(token_map.contains(2LL));
  BOOST_CHECK(token_map.contains(4LL));

  // Remove host2 and check that its tokens have been removed
  token_map.remove_host_and_build(host2);

  BOOST_CHECK(token_map.contains(-3LL));
  BOOST_CHECK(token_map.contains(-1LL));
  BOOST_CHECK(token_map.contains(1LL));
  BOOST_CHECK(token_map.contains(3LL));

  BOOST_CHECK(!token_map.contains(-4LL));
  BOOST_CHECK(!token_map.contains(-2LL));
  BOOST_CHECK(!token_map.contains(2LL));
  BOOST_CHECK(!token_map.contains(4LL));

  // Add host2 and check that its tokens have been added (same as the initial state)
  token_map.update_host_and_build(host2, builder2.finish());

  BOOST_CHECK(token_map.contains(-3LL));
  BOOST_CHECK(token_map.contains(-1LL));
  BOOST_CHECK(token_map.contains(1LL));
  BOOST_CHECK(token_map.contains(3LL));

  BOOST_CHECK(token_map.contains(-4LL));
  BOOST_CHECK(token_map.contains(-2LL));
  BOOST_CHECK(token_map.contains(2LL));
  BOOST_CHECK(token_map.contains(4LL));
}

BOOST_AUTO_TEST_CASE(drop_keyspace)
{
  TestTokenMap<cass::Murmur3Partitioner> test_drop_keyspace;

  test_drop_keyspace.tokens[CASS_INT64_MIN / 2] = create_host("1.0.0.1");
  test_drop_keyspace.tokens[0]                  = create_host("1.0.0.2");
  test_drop_keyspace.tokens[CASS_INT64_MAX / 2] = create_host("1.0.0.3");

  test_drop_keyspace.build("ks", 2);
  test_drop_keyspace.verify();

  cass::TokenMap* token_map = test_drop_keyspace.token_map.get();

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 2);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    BOOST_CHECK_EQUAL((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
  }

  token_map->drop_keyspace("ks");

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_CHECK(!replicas);
  }
}

BOOST_AUTO_TEST_SUITE_END()
