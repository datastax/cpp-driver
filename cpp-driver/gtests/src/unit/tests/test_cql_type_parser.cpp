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

#include "data_type_parser.hpp"

TEST(CqlTypeParserUnitTest, Simple) {
  cass::DataType::ConstPtr data_type;

  cass::SimpleDataTypeCache cache;

  cass::KeyspaceMetadata keyspace("keyspace1");

  data_type = cass::DataTypeCqlNameParser::parse("ascii", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_ASCII);

  data_type = cass::DataTypeCqlNameParser::parse("bigint", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_BIGINT);

  data_type = cass::DataTypeCqlNameParser::parse("blob", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_BLOB);

  data_type = cass::DataTypeCqlNameParser::parse("boolean", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_BOOLEAN);

  data_type = cass::DataTypeCqlNameParser::parse("counter", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_COUNTER);

  data_type = cass::DataTypeCqlNameParser::parse("date", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_DATE);

  data_type = cass::DataTypeCqlNameParser::parse("decimal", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_DECIMAL);

  data_type = cass::DataTypeCqlNameParser::parse("double", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_DOUBLE);

  data_type = cass::DataTypeCqlNameParser::parse("float", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_FLOAT);

  data_type = cass::DataTypeCqlNameParser::parse("inet", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_INET);

  data_type = cass::DataTypeCqlNameParser::parse("int", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_INT);

  data_type = cass::DataTypeCqlNameParser::parse("smallint", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_SMALL_INT);

  data_type = cass::DataTypeCqlNameParser::parse("time", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TIME);

  data_type = cass::DataTypeCqlNameParser::parse("timestamp", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TIMESTAMP);

  data_type = cass::DataTypeCqlNameParser::parse("timeuuid", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TIMEUUID);

  data_type = cass::DataTypeCqlNameParser::parse("tinyint", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TINY_INT);

  data_type = cass::DataTypeCqlNameParser::parse("text", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TEXT);

  data_type = cass::DataTypeCqlNameParser::parse("uuid", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_UUID);

  data_type = cass::DataTypeCqlNameParser::parse("varchar", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_VARCHAR);

  data_type = cass::DataTypeCqlNameParser::parse("varint", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_VARINT);
}

TEST(CqlTypeParserUnitTest, Collections) {
  cass::DataType::ConstPtr data_type;

  cass::SimpleDataTypeCache cache;

  cass::KeyspaceMetadata keyspace("keyspace1");

  data_type = cass::DataTypeCqlNameParser::parse("list<int>", cache, &keyspace);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_LIST);
  cass::CollectionType::ConstPtr list = static_cast<cass::CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(list->types().size(), 1u);
  EXPECT_EQ(list->types()[0]->value_type(), CASS_VALUE_TYPE_INT);

  data_type = cass::DataTypeCqlNameParser::parse("set<int>", cache, &keyspace);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_SET);
  cass::CollectionType::ConstPtr set = static_cast<cass::CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(set->types().size(), 1u);
  EXPECT_EQ(set->types()[0]->value_type(), CASS_VALUE_TYPE_INT);

  data_type = cass::DataTypeCqlNameParser::parse("map<int, text>", cache, &keyspace);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_MAP);
  cass::CollectionType::ConstPtr map = static_cast<cass::CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(map->types().size(), 2u);
  EXPECT_EQ(map->types()[0]->value_type(), CASS_VALUE_TYPE_INT);
  EXPECT_EQ(map->types()[1]->value_type(), CASS_VALUE_TYPE_TEXT);
}

TEST(CqlTypeParserUnitTest, Tuple) {
  cass::DataType::ConstPtr data_type;

  cass::SimpleDataTypeCache cache;

  cass::KeyspaceMetadata keyspace("keyspace1");

  data_type = cass::DataTypeCqlNameParser::parse("tuple<int, bigint, text>", cache, &keyspace);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TUPLE);
  cass::CollectionType::ConstPtr tuple = static_cast<cass::CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(tuple->types().size(), 3u);
  EXPECT_EQ(tuple->types()[0]->value_type(), CASS_VALUE_TYPE_INT);
  EXPECT_EQ(tuple->types()[1]->value_type(), CASS_VALUE_TYPE_BIGINT);
  EXPECT_EQ(tuple->types()[2]->value_type(), CASS_VALUE_TYPE_TEXT);
}

TEST(CqlTypeParserUnitTest, UserDefinedType) {
  cass::DataType::ConstPtr data_type;

  cass::SimpleDataTypeCache cache;

  cass::KeyspaceMetadata keyspace("keyspace1");

  EXPECT_TRUE(keyspace.user_types().empty());

  data_type = cass::DataTypeCqlNameParser::parse("type1", cache, &keyspace);

  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_UDT);
  cass::UserType::ConstPtr udt = static_cast<cass::UserType::ConstPtr>(data_type);
  EXPECT_EQ(udt->type_name(), "type1");
  EXPECT_EQ(udt->keyspace(), "keyspace1");

  EXPECT_FALSE(keyspace.user_types().empty());
}

TEST(CqlTypeParserUnitTest, Frozen) {
  cass::DataType::ConstPtr data_type;

  cass::SimpleDataTypeCache cache;

  cass::KeyspaceMetadata keyspace("keyspace1");

  {
    data_type = cass::DataTypeCqlNameParser::parse("frozen<list<int>>", cache, &keyspace);
    ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_LIST);
    cass::CollectionType::ConstPtr list = static_cast<cass::CollectionType::ConstPtr>(data_type);
    ASSERT_EQ(list->types().size(), 1u);
    EXPECT_TRUE(list->is_frozen());
    EXPECT_EQ(list->types()[0]->value_type(), CASS_VALUE_TYPE_INT);
  }

  {
    data_type = cass::DataTypeCqlNameParser::parse("list<frozen<list<int>>>", cache, &keyspace);
    ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_LIST);
    cass::CollectionType::ConstPtr list = static_cast<cass::CollectionType::ConstPtr>(data_type);
    ASSERT_EQ(list->types().size(), 1u);
    EXPECT_FALSE(list->is_frozen());

    EXPECT_EQ(list->types()[0]->value_type(), CASS_VALUE_TYPE_LIST);
    EXPECT_TRUE(list->types()[0]->is_frozen());
  }
}

TEST(CqlTypeParserUnitTest, Invalid) {
  cass::DataType::ConstPtr data_type;

  cass::SimpleDataTypeCache cache;

  cass::KeyspaceMetadata keyspace("keyspace1");

  // Invalid number of parameters
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("list<>", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("set<>", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("map<>", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("tuple<>", cache, &keyspace));

  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("list<int, int>", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("set<int, int>", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("map<int>", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("map<int, int, int>", cache, &keyspace));

  // Invalid brackets
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("list<", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("list>", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("<>", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("<", cache, &keyspace));
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse(">", cache, &keyspace));

  // Empty
  EXPECT_FALSE(cass::DataTypeCqlNameParser::parse("", cache, &keyspace));
}
