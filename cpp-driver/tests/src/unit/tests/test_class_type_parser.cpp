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

using namespace datastax::internal;
using namespace datastax::internal::core;

TEST(ClassTypeParserUnitTest, Simple) {
  DataType::ConstPtr data_type;

  SimpleDataTypeCache cache;

  data_type =
      DataTypeClassNameParser::parse_one("org.apache.cassandra.db.marshal.InetAddressType", cache);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_INET);

  data_type = DataTypeClassNameParser::parse_one(
      "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type)",
      cache);
  EXPECT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TEXT);

  data_type = DataTypeClassNameParser::parse_one(
      "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)", cache);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_LIST);

  CollectionType::ConstPtr collection = static_cast<CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(collection->types().size(), 1u);
  EXPECT_EQ(collection->types()[0]->value_type(), CASS_VALUE_TYPE_TEXT);
}

TEST(ClassTypeParserUnitTest, Invalid) {
  cass_log_set_level(CASS_LOG_DISABLED);

  SimpleDataTypeCache cache;

  // Premature end of string
  EXPECT_FALSE(
      DataTypeClassNameParser::parse_one("org.apache.cassandra.db.marshal.UserType", cache));
  EXPECT_FALSE(
      DataTypeClassNameParser::parse_one("org.apache.cassandra.db.marshal.UserType(", cache));
  EXPECT_FALSE(
      DataTypeClassNameParser::parse_one("org.apache.cassandra.db.marshal.UserType(blah", cache));
  EXPECT_FALSE(
      DataTypeClassNameParser::parse_one("org.apache.cassandra.db.marshal.UserType(blah,", cache));

  // Empty
  EXPECT_FALSE(
      DataTypeClassNameParser::parse_one("org.apache.cassandra.db.marshal.UserType()", cache));

  // Invalid hex
  EXPECT_FALSE(DataTypeClassNameParser::parse_one(
      "org.apache.cassandra.db.marshal.UserType(blah,ZZZZ", cache));

  // Missing ':'
  EXPECT_FALSE(
      DataTypeClassNameParser::parse_one("org.apache.cassandra.db.marshal.UserType("
                                         "foo,61646472657373,"
                                         "737472656574org.apache.cassandra.db.marshal.UTF8Type)",
                                         cache));

  // Premature end of string
  EXPECT_FALSE(DataTypeClassNameParser::parse_with_composite(
      "org.apache.cassandra.db.marshal.CompositeType", cache));
  EXPECT_FALSE(DataTypeClassNameParser::parse_with_composite(
      "org.apache.cassandra.db.marshal.CompositeType(", cache));
  EXPECT_FALSE(DataTypeClassNameParser::parse_with_composite(
      "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type",
      cache));
  EXPECT_FALSE(DataTypeClassNameParser::parse_with_composite(
      "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,",
      cache));

  // Empty
  EXPECT_FALSE(DataTypeClassNameParser::parse_with_composite(
      "org.apache.cassandra.db.marshal.CompositeType()", cache));
}

TEST(ClassTypeParserUnitTest, UserDefinedType) {
  SimpleDataTypeCache cache;

  DataType::ConstPtr data_type = DataTypeClassNameParser::parse_one(
      "org.apache.cassandra.db.marshal.UserType("
      "foo,61646472657373,"
      "737472656574:org.apache.cassandra.db.marshal.UTF8Type,"
      "7a6970636f6465:org.apache.cassandra.db.marshal.Int32Type,"
      "70686f6e6573:org.apache.cassandra.db.marshal.SetType("
      "org.apache.cassandra.db.marshal.UserType(foo,70686f6e65,6e616d65:org.apache.cassandra.db."
      "marshal.UTF8Type,6e756d626572:org.apache.cassandra.db.marshal.UTF8Type)))",
      cache);

  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_UDT);

  // Check external UDT

  UserType::ConstPtr udt(data_type);

  EXPECT_EQ(udt->keyspace(), "foo");
  EXPECT_EQ(udt->type_name(), "address");
  ASSERT_EQ(udt->fields().size(), 3u);

  UserType::FieldVec::const_iterator i;

  i = udt->fields().begin();

  EXPECT_EQ(i->name, "street");
  EXPECT_EQ(i->type->value_type(), CASS_VALUE_TYPE_TEXT);

  ++i;

  EXPECT_EQ(i->name, "zipcode");
  EXPECT_EQ(i->type->value_type(), CASS_VALUE_TYPE_INT);

  ++i;

  EXPECT_EQ(i->name, "phones");
  ASSERT_EQ(i->type->value_type(), CASS_VALUE_TYPE_SET);

  CollectionType::ConstPtr collection = static_cast<CollectionType::ConstPtr>(i->type);

  ASSERT_EQ(collection->types().size(), 1u);
  ASSERT_EQ(collection->types()[0]->value_type(), CASS_VALUE_TYPE_UDT);

  // Check internal UDT

  udt = static_cast<UserType::ConstPtr>(collection->types()[0]);

  EXPECT_EQ(udt->keyspace(), "foo");
  EXPECT_EQ(udt->type_name(), "phone");
  ASSERT_EQ(udt->fields().size(), 2u);

  i = udt->fields().begin();

  EXPECT_EQ(i->name, "name");
  EXPECT_EQ(i->type->value_type(), CASS_VALUE_TYPE_TEXT);

  ++i;

  EXPECT_EQ(i->name, "number");
  EXPECT_EQ(i->type->value_type(), CASS_VALUE_TYPE_TEXT);
}

TEST(ClassTypeParserUnitTest, Tuple) {
  SimpleDataTypeCache cache;

  DataType::ConstPtr data_type =
      DataTypeClassNameParser::parse_one("org.apache.cassandra.db.marshal.TupleType("
                                         "org.apache.cassandra.db.marshal.Int32Type,"
                                         "org.apache.cassandra.db.marshal.UTF8Type,"
                                         "org.apache.cassandra.db.marshal.FloatType)",
                                         cache);

  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_TUPLE);

  TupleType::ConstPtr tuple = static_cast<TupleType::ConstPtr>(data_type);

  ASSERT_EQ(tuple->types().size(), 3u);

  ASSERT_EQ(tuple->types()[0]->value_type(), CASS_VALUE_TYPE_INT);
  ASSERT_EQ(tuple->types()[1]->value_type(), CASS_VALUE_TYPE_TEXT);
  ASSERT_EQ(tuple->types()[2]->value_type(), CASS_VALUE_TYPE_FLOAT);
}

TEST(ClassTypeParserUnitTest, NestedCollections) {
  SimpleDataTypeCache cache;

  DataType::ConstPtr data_type = DataTypeClassNameParser::parse_one(
      "org.apache.cassandra.db.marshal.MapType("
      "org.apache.cassandra.db.marshal.UTF8Type,"
      "org.apache.cassandra.db.marshal.FrozenType("
      "org.apache.cassandra.db.marshal.MapType("
      "org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)))",
      cache);

  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_MAP);

  CollectionType::ConstPtr collection = static_cast<CollectionType::ConstPtr>(data_type);

  ASSERT_EQ(collection->types().size(), 2u);

  EXPECT_EQ(collection->types()[0]->value_type(), CASS_VALUE_TYPE_TEXT);

  ASSERT_EQ(collection->types()[1]->value_type(), CASS_VALUE_TYPE_MAP);

  CollectionType::ConstPtr nested_collection =
      static_cast<CollectionType::ConstPtr>(collection->types()[1]);

  ASSERT_EQ(nested_collection->types().size(), 2u);
  EXPECT_EQ(nested_collection->types()[0]->value_type(), CASS_VALUE_TYPE_INT);
  EXPECT_EQ(nested_collection->types()[1]->value_type(), CASS_VALUE_TYPE_INT);
}

TEST(ClassTypeParserUnitTest, Composite) {
  SimpleDataTypeCache cache;

  SharedRefPtr<ParseResult> result =
      DataTypeClassNameParser::parse_with_composite("org.apache.cassandra.db.marshal.CompositeType("
                                                    "org.apache.cassandra.db.marshal.AsciiType,"
                                                    "org.apache.cassandra.db.marshal.Int32Type)",
                                                    cache);

  EXPECT_TRUE(result->is_composite());

  ASSERT_EQ(result->types().size(), 2u);
  EXPECT_EQ(result->types()[0]->value_type(), CASS_VALUE_TYPE_ASCII);
  EXPECT_EQ(result->types()[1]->value_type(), CASS_VALUE_TYPE_INT);

  ASSERT_EQ(result->reversed().size(), 2u);
  EXPECT_EQ(result->reversed()[0], false);
  EXPECT_EQ(result->reversed()[1], false);

  EXPECT_TRUE(result->collections().empty());
}

TEST(ClassTypeParserUnitTest, NotComposite) {
  SimpleDataTypeCache cache;

  SharedRefPtr<ParseResult> result = DataTypeClassNameParser::parse_with_composite(
      "org.apache.cassandra.db.marshal.InetAddressType", cache);

  ASSERT_EQ(result->types().size(), 1u);
  EXPECT_EQ(result->types()[0]->value_type(), CASS_VALUE_TYPE_INET);

  ASSERT_EQ(result->reversed().size(), 1u);
  EXPECT_EQ(result->reversed()[0], false);
}

TEST(ClassTypeParserUnitTest, CompositeWithReversedType) {
  SimpleDataTypeCache cache;

  SharedRefPtr<ParseResult> result = DataTypeClassNameParser::parse_with_composite(
      "org.apache.cassandra.db.marshal.CompositeType("
      "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.AsciiType),"
      "org.apache.cassandra.db.marshal.Int32Type)",
      cache);

  EXPECT_TRUE(result->is_composite());

  ASSERT_EQ(result->types().size(), 2u);
  EXPECT_EQ(result->types()[0]->value_type(), CASS_VALUE_TYPE_ASCII);
  EXPECT_EQ(result->types()[1]->value_type(), CASS_VALUE_TYPE_INT);

  ASSERT_EQ(result->reversed().size(), 2u);
  EXPECT_EQ(result->reversed()[0], true);
  EXPECT_EQ(result->reversed()[1], false);

  EXPECT_TRUE(result->collections().empty());
}

TEST(ClassTypeParserUnitTest, CompositeWithCollections) {
  SimpleDataTypeCache cache;

  SharedRefPtr<ParseResult> result = DataTypeClassNameParser::parse_with_composite(
      "org.apache.cassandra.db.marshal.CompositeType("
      "org.apache.cassandra.db.marshal.Int32Type, "
      "org.apache.cassandra.db.marshal.UTF8Type,"
      "org.apache.cassandra.db.marshal.ColumnToCollectionType("
      "6162:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type),"
      "4A4b4C4D4e4F:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal."
      "UTF8Type),"
      "6A6b6C6D6e6F:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal."
      "UTF8Type, org.apache.cassandra.db.marshal.LongType)"
      "))",
      cache);

  EXPECT_TRUE(result->is_composite());

  ASSERT_EQ(result->types().size(), 2u);
  EXPECT_EQ(result->types()[0]->value_type(), CASS_VALUE_TYPE_INT);
  EXPECT_EQ(result->types()[1]->value_type(), CASS_VALUE_TYPE_TEXT);

  ASSERT_EQ(result->reversed().size(), 2u);
  EXPECT_EQ(result->reversed()[0], false);
  EXPECT_EQ(result->reversed()[1], false);

  ASSERT_EQ(result->collections().size(), 3u);

  ParseResult::CollectionMap::const_iterator i;

  i = result->collections().find("ab");
  CollectionType::ConstPtr collection;
  ASSERT_NE(i, result->collections().end());
  ASSERT_EQ(i->second->value_type(), CASS_VALUE_TYPE_LIST);
  collection = static_cast<CollectionType::ConstPtr>(i->second);
  ASSERT_EQ(collection->types().size(), 1u);
  EXPECT_EQ(collection->types()[0]->value_type(), CASS_VALUE_TYPE_INT);

  i = result->collections().find("JKLMNO");
  ASSERT_NE(i, result->collections().end());
  EXPECT_EQ(i->second->value_type(), CASS_VALUE_TYPE_SET);
  collection = static_cast<CollectionType::ConstPtr>(i->second);
  ASSERT_EQ(collection->types().size(), 1u);
  EXPECT_EQ(collection->types()[0]->value_type(), CASS_VALUE_TYPE_TEXT);

  i = result->collections().find("jklmno");
  ASSERT_NE(i, result->collections().end());
  EXPECT_EQ(i->second->value_type(), CASS_VALUE_TYPE_MAP);
  collection = static_cast<CollectionType::ConstPtr>(i->second);
  ASSERT_EQ(collection->types().size(), 2u);
  EXPECT_EQ(collection->types()[0]->value_type(), CASS_VALUE_TYPE_TEXT);
  EXPECT_EQ(collection->types()[1]->value_type(), CASS_VALUE_TYPE_BIGINT);
}

TEST(ClassTypeParserUnitTest, Frozen) {
  DataType::ConstPtr data_type;

  SimpleDataTypeCache cache;

  data_type = DataTypeClassNameParser::parse_one(
      "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org."
      "apache.cassandra.db.marshal.UTF8Type))",
      cache);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_LIST);
  EXPECT_TRUE(data_type->is_frozen());

  data_type = DataTypeClassNameParser::parse_one(
      "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.FrozenType(org."
      "apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)))",
      cache);
  ASSERT_EQ(data_type->value_type(), CASS_VALUE_TYPE_LIST);
  EXPECT_FALSE(data_type->is_frozen());

  CollectionType::ConstPtr collection = static_cast<CollectionType::ConstPtr>(data_type);
  ASSERT_EQ(collection->types().size(), 1u);
  EXPECT_EQ(collection->types()[0]->value_type(), CASS_VALUE_TYPE_LIST);
  EXPECT_TRUE(collection->types()[0]->is_frozen());
}
