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

#include "query_request.hpp"
#include "token_map.hpp"
#include "murmur3.hpp"
#include "logger.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/cstdint.hpp>

#include <string>

// The java-driver was used as a reference for the hash value
// below.

struct RoutingKeyTest {
  RoutingKeyTest() {
    cass::Logger::init();
    cass::Logger::set_log_level(CASS_LOG_DISABLED);
  }
};

BOOST_FIXTURE_TEST_SUITE(routing_key, RoutingKeyTest)

BOOST_AUTO_TEST_CASE(single)
{
  {
    cass::QueryRequest query(1);

    CassUuid uuid;
    BOOST_REQUIRE(cass_uuid_from_string("d8775a70-6ea4-11e4-9fa7-0db22d2a6140", &uuid) == CASS_OK);

    query.bind(0, uuid);
    query.add_key_index(0);

    std::string routing_key;
    BOOST_CHECK(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 6739078495667776670);
  }

  {
    cass::QueryRequest query(1);

    cass_int32_t value = 123456789;
    query.bind(0, value);
    query.add_key_index(0);

    std::string routing_key;
    BOOST_CHECK(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == -567416363967733925);
  }

  {
    cass::QueryRequest query(1);

    cass_int64_t value = 123456789;
    query.bind(0, value);
    query.add_key_index(0);

    std::string routing_key;
    BOOST_CHECK(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 5616923877423390342);
  }

  {
    cass::QueryRequest query(1);

    query.bind(0, true);
    query.add_key_index(0);

    std::string routing_key;
    BOOST_CHECK(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 8849112093580131862);
  }

  {
    cass::QueryRequest query(1);

    const char* value = "abcdefghijklmnop";
    query.bind(0, value, strlen(value));
    query.add_key_index(0);

    std::string routing_key;
    BOOST_CHECK(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == -4266531025627334877);
  }
}

BOOST_AUTO_TEST_CASE(empty_and_null)
{
  cass::QueryRequest query(1);

  std::string routing_key;
  BOOST_CHECK_EQUAL(query.get_routing_key(&routing_key), false);

  query.bind(0, cass::CassNull());
  query.add_key_index(0);

  BOOST_CHECK_EQUAL(query.get_routing_key(&routing_key), false);
}

BOOST_AUTO_TEST_CASE(composite)
{
  {
    cass::QueryRequest query(3);

    CassUuid uuid;
    BOOST_REQUIRE(cass_uuid_from_string("d8775a70-6ea4-11e4-9fa7-0db22d2a6140", &uuid) == CASS_OK);

    query.bind(0, uuid);
    query.add_key_index(0);

    query.bind(1, static_cast<cass_int64_t>(123456789));
    query.add_key_index(1);

    const char* value = "abcdefghijklmnop";
    query.bind(2, value, strlen(value));
    query.add_key_index(2);

    std::string routing_key;
    BOOST_CHECK(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 3838437721532426513);
  }

  {
    cass::QueryRequest query(3);

    query.bind(0, false);
    query.add_key_index(0);

    query.bind(1, static_cast<cass_int32_t>(123456789));
    query.add_key_index(1);

    const char* value = "xyz";
    query.bind(2, value, strlen(value));
    query.add_key_index(2);

    std::string routing_key;
    BOOST_CHECK(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    BOOST_CHECK(hash == 4466051201071860026);
  }
}

BOOST_AUTO_TEST_SUITE_END()
