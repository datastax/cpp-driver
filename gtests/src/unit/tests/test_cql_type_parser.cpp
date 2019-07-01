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

using namespace datastax::internal::core;

TEST(CqlTypeParserUnitTest, Simple) {
  DataType::ConstPtr data_type;

  SimpleDataTypeCache cache;

  KeyspaceMetadata keyspace("keyspace1");

  data_type = DataTypeCqlNameParser::parse("ascii", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_ASCII);

  data_type = DataTypeCqlNameParser::parse("bigint", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_BIGINT);

  data_type = DataTypeCqlNameParser::parse("blob", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_BLOB);

  data_type = DataTypeCqlNameParser::parse("boolean", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_BOOLEAN);

  data_type = DataTypeCqlNameParser::parse("counter", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_COUNTER);

  data_type = DataTypeCqlNameParser::parse("date", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_DATE);

  data_type = DataTypeCqlNameParser::parse("decimal", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_DECIMAL);

  data_type = DataTypeCqlNameParser::parse("double", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_DOUBLE);

  data_type = DataTypeCqlNameParser::parse("float", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_FLOAT);

  data_type = DataTypeCqlNameParser::parse("inet", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_INET);

  data_type = DataTypeCqlNameParser::parse("int", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_INT);

  data_type = DataTypeCqlNameParser::parse("smallint", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_SMALL_INT);

  data_type = DataTypeCqlNameParser::parse("time", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TIME);

  data_type = DataTypeCqlNameParser::parse("timestamp", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TIMESTAMP);

  data_type = DataTypeCqlNameParser::parse("timeuuid", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TIMEUUID);

  data_type = DataTypeCqlNameParser::parse("tinyint", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TINY_INT);

  data_type = DataTypeCqlNameParser::parse("text", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TEXT);

  data_type = DataTypeCqlNameParser::parse("uuid", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_UUID);

  data_type = DataTypeCqlNameParser::parse("varchar", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_VARCHAR);

  data_type = DataTypeCqlNameParser::parse("varint", cache, &keyspace);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_VARINT);
}

TEST(CqlTypeParserUnitTest, Collections) {
  DataType::ConstPtr data_type;

  SimpleDataTypeCache cache;

  KeyspaceMetadata keyspace("keyspace1");

  data_type = DataTypeCqlNameParser::parse("list<int>", cache, &keyspace);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_LIST);
  CollectionType::ConstPtr list = static_cast<CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(list->types().size(), 1u);
  EXPECT_EQ(list->types()[0]->value_type(), CASS_VALUE_TYPE_INT);

  data_type = DataTypeCqlNameParser::parse("set<int>", cache, &keyspace);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_SET);
  CollectionType::ConstPtr set = static_cast<CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(set->types().size(), 1u);
  EXPECT_EQ(set->types()[0]->value_type(), CASS_VALUE_TYPE_INT);

  data_type = DataTypeCqlNameParser::parse("map<int, text>", cache, &keyspace);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_MAP);
  CollectionType::ConstPtr map = static_cast<CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(map->types().size(), 2u);
  EXPECT_EQ(map->types()[0]->value_type(), CASS_VALUE_TYPE_INT);
  EXPECT_EQ(map->types()[1]->value_type(), CASS_VALUE_TYPE_TEXT);
}

TEST(CqlTypeParserUnitTest, Tuple) {
  DataType::ConstPtr data_type;

  SimpleDataTypeCache cache;

  KeyspaceMetadata keyspace("keyspace1");

  data_type = DataTypeCqlNameParser::parse("tuple<int, bigint, text>", cache, &keyspace);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TUPLE);
  CollectionType::ConstPtr tuple = static_cast<CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(tuple->types().size(), 3u);
  EXPECT_EQ(tuple->types()[0]->value_type(), CASS_VALUE_TYPE_INT);
  EXPECT_EQ(tuple->types()[1]->value_type(), CASS_VALUE_TYPE_BIGINT);
  EXPECT_EQ(tuple->types()[2]->value_type(), CASS_VALUE_TYPE_TEXT);
}

TEST(CqlTypeParserUnitTest, UserDefinedType) {
  DataType::ConstPtr data_type;

  SimpleDataTypeCache cache;

  KeyspaceMetadata keyspace("keyspace1");

  EXPECT_TRUE(keyspace.user_types().empty());

  data_type = DataTypeCqlNameParser::parse("type1", cache, &keyspace);

  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_UDT);
  UserType::ConstPtr udt = static_cast<UserType::ConstPtr>(data_type);
  EXPECT_EQ(udt->type_name(), "type1");
  EXPECT_EQ(udt->keyspace(), "keyspace1");

  EXPECT_FALSE(keyspace.user_types().empty());
}

TEST(CqlTypeParserUnitTest, Frozen) {
  DataType::ConstPtr data_type;

  SimpleDataTypeCache cache;

  KeyspaceMetadata keyspace("keyspace1");

  {
    data_type = DataTypeCqlNameParser::parse("frozen<list<int>>", cache, &keyspace);
    ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_LIST);
    CollectionType::ConstPtr list = static_cast<CollectionType::ConstPtr>(data_type);
    ASSERT_EQ(list->types().size(), 1u);
    EXPECT_TRUE(list->is_frozen());
    EXPECT_EQ(list->types()[0]->value_type(), CASS_VALUE_TYPE_INT);
  }

  {
    data_type = DataTypeCqlNameParser::parse("list<frozen<list<int>>>", cache, &keyspace);
    ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_LIST);
    CollectionType::ConstPtr list = static_cast<CollectionType::ConstPtr>(data_type);
    ASSERT_EQ(list->types().size(), 1u);
    EXPECT_FALSE(list->is_frozen());

    EXPECT_EQ(list->types()[0]->value_type(), CASS_VALUE_TYPE_LIST);
    EXPECT_TRUE(list->types()[0]->is_frozen());
  }
}

TEST(CqlTypeParserUnitTest, Invalid) {
  DataType::ConstPtr data_type;

  SimpleDataTypeCache cache;

  KeyspaceMetadata keyspace("keyspace1");

  // Invalid number of parameters
  EXPECT_FALSE(DataTypeCqlNameParser::parse("list<>", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse("set<>", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse("map<>", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse("tuple<>", cache, &keyspace));

  EXPECT_FALSE(DataTypeCqlNameParser::parse("list<int, int>", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse("set<int, int>", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse("map<int>", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse("map<int, int, int>", cache, &keyspace));

  // Invalid brackets
  EXPECT_FALSE(DataTypeCqlNameParser::parse("list<", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse("list>", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse("<>", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse("<", cache, &keyspace));
  EXPECT_FALSE(DataTypeCqlNameParser::parse(">", cache, &keyspace));

  // Empty
  EXPECT_FALSE(DataTypeCqlNameParser::parse("", cache, &keyspace));
}
