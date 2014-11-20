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

#include "query_request.hpp"
#include "token_map.hpp"
#include "murmur3.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/cstdint.hpp>

#include <string>

bool uuid_from_string(const std::string& str, CassUuid out) {
  if (str.size() != 36) return false;
  const char* pos = str.data();
  for (size_t i = 0; i < 16; ++i) {
    if (*pos == '-') pos++;
    unsigned int byte;
    sscanf(pos, "%2x", &byte);
    out[i] = byte;
    pos += 2;
  }
  return true;
}

// The java-driver was used as a reference for the hash value
// below.

BOOST_AUTO_TEST_SUITE(routing_key)

BOOST_AUTO_TEST_CASE(single)
{
  {
    cass::QueryRequest query(1);

    CassUuid uuid;
    BOOST_REQUIRE(uuid_from_string("d8775a70-6ea4-11e4-9fa7-0db22d2a6140", uuid));

    query.bind(0, uuid);
    query.add_key_index(0);

    std::string routing_key;
    query.get_routing_key(&routing_key);

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 6739078495667776670);
  }

  {
    cass::QueryRequest query(1);

    cass_int32_t value = 123456789;
    query.bind(0, value);
    query.add_key_index(0);

    std::string routing_key;
    query.get_routing_key(&routing_key);

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == -567416363967733925);
  }

  {
    cass::QueryRequest query(1);

    cass_int64_t value = 123456789;
    query.bind(0, value);
    query.add_key_index(0);

    std::string routing_key;
    query.get_routing_key(&routing_key);

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 5616923877423390342);
  }

  {
    cass::QueryRequest query(1);

    query.bind(0, true);
    query.add_key_index(0);

    std::string routing_key;
    query.get_routing_key(&routing_key);

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 8849112093580131862);
  }

  {
    cass::QueryRequest query(1);

    query.bind(0, cass_string_init("abcdefghijklmnop"));
    query.add_key_index(0);

    std::string routing_key;
    query.get_routing_key(&routing_key);

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == -4266531025627334877);
  }
}

BOOST_AUTO_TEST_CASE(composite)
{
  {
    cass::QueryRequest query(3);

    CassUuid uuid;
    BOOST_REQUIRE(uuid_from_string("d8775a70-6ea4-11e4-9fa7-0db22d2a6140", uuid));

    query.bind(0, uuid);
    query.add_key_index(0);

    query.bind(1, static_cast<cass_int64_t>(123456789));
    query.add_key_index(1);

    query.bind(2, cass_string_init("abcdefghijklmnop"));
    query.add_key_index(2);


    std::string routing_key;
    query.get_routing_key(&routing_key);

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 3838437721532426513);
  }

  {
    cass::QueryRequest query(3);

    query.bind(0, false);
    query.add_key_index(0);

    query.bind(1, static_cast<cass_int32_t>(123456789));
    query.add_key_index(1);

    query.bind(2, cass_string_init("xyz"));
    query.add_key_index(2);

    std::string routing_key;
    query.get_routing_key(&routing_key);

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 4466051201071860026);
  }
}

BOOST_AUTO_TEST_SUITE_END()
