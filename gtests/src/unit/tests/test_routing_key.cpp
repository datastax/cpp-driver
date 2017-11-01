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

#include "logger.hpp"
#include "murmur3.hpp"
#include "query_request.hpp"
#include "token_map.hpp"

// The java-driver was used as a reference for the hash value
// below.

struct RoutingKeyUnitTest : public ::testing::Test  {
  static void SetUpTestCase() {
    cass::Logger::set_log_level(CASS_LOG_DISABLED);
  }
};

TEST_F(RoutingKeyUnitTest, Single)
{
  {
    cass::QueryRequest query("", 1);

    CassUuid uuid;
    ASSERT_EQ(cass_uuid_from_string("d8775a70-6ea4-11e4-9fa7-0db22d2a6140", &uuid), CASS_OK);

    query.set(0, uuid);
    query.add_key_index(0);

    std::string routing_key;
    EXPECT_TRUE(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    EXPECT_EQ(hash, 6739078495667776670);
  }

  {
    cass::QueryRequest query("", 1);

    cass_int32_t value = 123456789;
    query.set(0, value);
    query.add_key_index(0);

    std::string routing_key;
    EXPECT_TRUE(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    EXPECT_EQ(hash, -567416363967733925);
  }

  {
    cass::QueryRequest query("", 1);

    cass_int64_t value = 123456789;
    query.set(0, value);
    query.add_key_index(0);

    std::string routing_key;
    EXPECT_TRUE(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    EXPECT_EQ(hash, 5616923877423390342);
  }

  {
    cass::QueryRequest query("", 1);

    query.set(0, cass_true);
    query.add_key_index(0);

    std::string routing_key;
    EXPECT_TRUE(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    EXPECT_EQ(hash, 8849112093580131862);
  }

  {
    cass::QueryRequest query("", 1);

    const char* value = "abcdefghijklmnop";
    query.set(0, cass::CassString(value, strlen(value)));
    query.add_key_index(0);

    std::string routing_key;
    EXPECT_TRUE(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    EXPECT_EQ(hash, -4266531025627334877);
  }
}

TEST_F(RoutingKeyUnitTest, EmptyAndNull)
{
  cass::QueryRequest query("", 1);

  std::string routing_key;
  EXPECT_FALSE(query.get_routing_key(&routing_key));

  query.set(0, cass::CassNull());
  query.add_key_index(0);

  EXPECT_FALSE(query.get_routing_key(&routing_key));
}

TEST_F(RoutingKeyUnitTest, Composite)
{
  {
    cass::QueryRequest query("", 3);

    CassUuid uuid;
    ASSERT_EQ(cass_uuid_from_string("d8775a70-6ea4-11e4-9fa7-0db22d2a6140", &uuid), CASS_OK);

    query.set(0, uuid);
    query.add_key_index(0);

    query.set(1, static_cast<cass_int64_t>(123456789));
    query.add_key_index(1);

    const char* value = "abcdefghijklmnop";
    query.set(2, cass::CassString(value, strlen(value)));
    query.add_key_index(2);

    std::string routing_key;
    EXPECT_TRUE(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    EXPECT_EQ(hash, 3838437721532426513);
  }

  {
    cass::QueryRequest query("", 3);

    query.set(0, cass_false);
    query.add_key_index(0);

    query.set(1, static_cast<cass_int32_t>(123456789));
    query.add_key_index(1);

    const char* value = "xyz";
    query.set(2, cass::CassString(value, strlen(value)));
    query.add_key_index(2);

    std::string routing_key;
    EXPECT_TRUE(query.get_routing_key(&routing_key));

    int64_t hash = cass::MurmurHash3_x64_128(routing_key.data(), routing_key.size(), 0);
    EXPECT_EQ(hash, 4466051201071860026);
  }
}
