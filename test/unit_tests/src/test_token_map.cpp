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
#include "md5.hpp"
#include "murmur3.hpp"
#include "token_map.hpp"

#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/random/mersenne_twister.hpp>

#include <limits>

cass::SharedRefPtr<cass::Host> create_host(const std::string& ip) {
  return cass::SharedRefPtr<cass::Host>(new cass::Host(cass::Address(ip, 4092), false));
}

template <class HashType>
struct TestTokenMap {
  typedef HashType(HashFunc)(const std::string& s);
  typedef std::map<HashType, cass::SharedRefPtr<cass::Host> > TokenHostMap;

  TokenHostMap tokens;
  cass::TokenMap token_map;
  cass::SharedRefPtr<cass::ReplicationStrategy> strategy;

  void build(const std::string& partitioner,
             const std::string& ks_name) {
    token_map.set_partitioner(partitioner);

    if (!strategy) {
      strategy =  cass::SharedRefPtr<cass::ReplicationStrategy>(
                    new cass::NonReplicatedStrategy(""));
    }

    token_map.set_replication_strategy(ks_name, strategy);

    for (typename TokenHostMap::iterator i = tokens.begin(); i != tokens.end(); ++i) {
      cass::TokenStringList tokens;
      std::string token(boost::lexical_cast<std::string>(i->first));
      tokens.push_back(token);
      token_map.update_host(i->second, tokens);
    }

    token_map.build();
  }

  void verify(HashFunc hash_func, const std::string& ks_name) {
    for (int i = 0; i < 24; ++i) {
      std::string value(1, 'a' + i);
      const cass::CopyOnWriteHostVec& replicas
          = token_map.get_replicas(ks_name, value);

      HashType hash = hash_func(value);
      typename TokenHostMap::iterator token = tokens.upper_bound(hash);

      if (token != tokens.end()) {
        BOOST_CHECK(replicas->front() == token->second);
      } else {
        BOOST_CHECK(replicas->front() == tokens.begin()->second);
      }
    }
  }
};

BOOST_AUTO_TEST_SUITE(token_map)

int64_t murmur3_hash(const std::string& s) {
  return cass::MurmurHash3_x64_128(s.data(), s.size(), 0);
}

BOOST_AUTO_TEST_CASE(murmur3)
{
  TestTokenMap<int64_t> test_murmur3;

  test_murmur3.tokens[std::numeric_limits<int64_t>::min() / 2] = create_host("1.0.0.1");
  test_murmur3.tokens[0] = create_host("1.0.0.2");
  test_murmur3.tokens[std::numeric_limits<int64_t>::max() / 2] = create_host("1.0.0.3");
  // Anything greater than the last host should be wrapped around to host1

  test_murmur3.build(cass::Murmur3Partitioner::PARTITIONER_CLASS, "test");
  test_murmur3.verify(murmur3_hash, "test");
}

BOOST_AUTO_TEST_CASE(murmur3_multiple_tokens_per_host)
{
  TestTokenMap<int64_t> test_murmur3;

  const size_t tokens_per_host = 256;

  cass::HostVec hosts;
  hosts.push_back(create_host("1.0.0.1"));
  hosts.push_back(create_host("1.0.0.2"));
  hosts.push_back(create_host("1.0.0.3"));
  hosts.push_back(create_host("1.0.0.4"));

  boost::mt19937_64 ng;

  for (cass::HostVec::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    for (size_t j = 0; j < tokens_per_host; ++j) {
      int64_t t = static_cast<int64_t>(ng());
      test_murmur3.tokens[t] = *i;
    }
  }

  test_murmur3.build(cass::Murmur3Partitioner::PARTITIONER_CLASS, "test");
  test_murmur3.verify(murmur3_hash, "test");
}

boost::multiprecision::int128_t random_hash(const std::string& s) {
  cass::Md5 m;
  m.update(reinterpret_cast<const uint8_t*>(s.data()), s.size());
  uint8_t h[16];
  m.final(h);
  std::string hex("0x");
  for (int i = 0; i < 16; ++i) {
    char buf[4];
    sprintf(buf, "%02X", h[i]);
    hex.append(buf);
  }
  return boost::multiprecision::int128_t(hex);
}

BOOST_AUTO_TEST_CASE(random)
{
  TestTokenMap<boost::multiprecision::int128_t> test_random;

  test_random.tokens[boost::multiprecision::int128_t("42535295865117307932921825928971026432")] = create_host("1.0.0.1");  // 2^127 / 4
  test_random.tokens[boost::multiprecision::int128_t("85070591730234615865843651857942052864")] = create_host("1.0.0.2");  // 2^127 / 2
  test_random.tokens[boost::multiprecision::int128_t("1605887595351923798765477786913079296")] = create_host("1.0.0.3"); // 2^127 * 3 / 4
  // Anything greater than the last host should be wrapped around to host1

  test_random.build(cass::RandomPartitioner::PARTITIONER_CLASS, "test");
  test_random.verify(random_hash, "test");
}

std::string byte_ordered_hash(const std::string& s) {
  return s;
}

BOOST_AUTO_TEST_CASE(byte_ordered)
{
  TestTokenMap<std::string> test_byte_ordered;

  test_byte_ordered.tokens["g"] = create_host("1.0.0.1");
  test_byte_ordered.tokens["m"] = create_host("1.0.0.2");
  test_byte_ordered.tokens["s"] = create_host("1.0.0.3");
  // Anything greater than the last host should be wrapped around to host1

  test_byte_ordered.build(cass::ByteOrderedPartitioner::PARTITIONER_CLASS, "test");
  test_byte_ordered.verify(byte_ordered_hash, "test");
}

BOOST_AUTO_TEST_CASE(remove_host)
{
  TestTokenMap<int64_t> test_remove_host;

  test_remove_host.strategy =
      cass::SharedRefPtr<cass::ReplicationStrategy>(new cass::SimpleStrategy("", 2));

  test_remove_host.tokens[std::numeric_limits<int64_t>::min() / 2] = create_host("1.0.0.1");
  test_remove_host.tokens[0] = create_host("1.0.0.2");
  test_remove_host.tokens[std::numeric_limits<int64_t>::max() / 2] = create_host("1.0.0.3");

  test_remove_host.build(cass::Murmur3Partitioner::PARTITIONER_CLASS, "test");

  cass::TokenMap& token_map = test_remove_host.token_map;

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 2);
    BOOST_CHECK((*replicas)[0]->address() == cass::Address("1.0.0.1", 9042));
    BOOST_CHECK((*replicas)[1]->address() == cass::Address("1.0.0.2", 9042));
  }

  TestTokenMap<int64_t>::TokenHostMap::iterator host_to_remove_it = test_remove_host.tokens.begin();

  token_map.remove_host(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 2);
    BOOST_CHECK((*replicas)[0]->address() == cass::Address("1.0.0.2", 9042));
    BOOST_CHECK((*replicas)[1]->address() == cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map.remove_host(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 1);
    BOOST_CHECK((*replicas)[0]->address() == cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map.remove_host(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 0);
  }
}

BOOST_AUTO_TEST_CASE(drop_keyspace)
{
  TestTokenMap<int64_t> test_drop_keyspace;

  test_drop_keyspace.strategy =
      cass::SharedRefPtr<cass::ReplicationStrategy>(new cass::SimpleStrategy("", 2));

  test_drop_keyspace.tokens[std::numeric_limits<int64_t>::min() / 2] = create_host("1.0.0.1");
  test_drop_keyspace.tokens[0] = create_host("1.0.0.2");
  test_drop_keyspace.tokens[std::numeric_limits<int64_t>::max() / 2] = create_host("1.0.0.3");

  test_drop_keyspace.build(cass::Murmur3Partitioner::PARTITIONER_CLASS, "test");

  cass::TokenMap& token_map = test_drop_keyspace.token_map;

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 2);
    BOOST_CHECK((*replicas)[0]->address() == cass::Address("1.0.0.1", 9042));
    BOOST_CHECK((*replicas)[1]->address() == cass::Address("1.0.0.2", 9042));
  }

  token_map.drop_keyspace("test");

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 0);
  }
}



BOOST_AUTO_TEST_SUITE_END()
