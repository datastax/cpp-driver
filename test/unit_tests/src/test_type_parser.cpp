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

#include "type_parser.hpp"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(type_parser)

BOOST_AUTO_TEST_CASE(simple)
{
  cass::SharedRefPtr<cass::DataType> data_type;

  data_type = cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.InetAddressType");
  BOOST_CHECK(data_type->value_type() == CASS_VALUE_TYPE_INET);

  data_type = cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type)");
  BOOST_CHECK(data_type->value_type() == CASS_VALUE_TYPE_TEXT);

  data_type = cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)");
  BOOST_REQUIRE(data_type->value_type() == CASS_VALUE_TYPE_LIST);

  cass::SharedRefPtr<cass::CollectionType> collection
      = static_cast<cass::SharedRefPtr<cass::CollectionType> >(data_type);
  BOOST_REQUIRE(collection->types().size() == 1);
  BOOST_CHECK(collection->types()[0]->value_type() == CASS_VALUE_TYPE_TEXT);
}

BOOST_AUTO_TEST_CASE(invalid)
{
  cass_log_set_level(CASS_LOG_DISABLED);

  // Premature end of string
  BOOST_CHECK(!cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.UserType"));
  BOOST_CHECK(!cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.UserType("));
  BOOST_CHECK(!cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.UserType(blah"));
  BOOST_CHECK(!cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.UserType(blah,"));

  // Empty
  BOOST_CHECK(!cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.UserType()"));

  // Invalid hex
  BOOST_CHECK(!cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.UserType(blah,ZZZZ"));

  // Missing ':'
  BOOST_CHECK(!cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.UserType("
                                           "foo,61646472657373,"
                                           "737472656574org.apache.cassandra.db.marshal.UTF8Type)"));

  // Premature end of string
  BOOST_CHECK(!cass::TypeParser::parse_with_composite("org.apache.cassandra.db.marshal.CompositeType"));
  BOOST_CHECK(!cass::TypeParser::parse_with_composite("org.apache.cassandra.db.marshal.CompositeType("));
  BOOST_CHECK(!cass::TypeParser::parse_with_composite("org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type"));
  BOOST_CHECK(!cass::TypeParser::parse_with_composite("org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,"));

  // Empty
  BOOST_CHECK(!cass::TypeParser::parse_with_composite("org.apache.cassandra.db.marshal.CompositeType()"));
}

BOOST_AUTO_TEST_CASE(udt)
{
  cass::SharedRefPtr<cass::DataType> data_type
      = cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.UserType("
                                    "foo,61646472657373,"
                                    "737472656574:org.apache.cassandra.db.marshal.UTF8Type,"
                                    "7a6970636f6465:org.apache.cassandra.db.marshal.Int32Type,"
                                    "70686f6e6573:org.apache.cassandra.db.marshal.SetType("
                                    "org.apache.cassandra.db.marshal.UserType(foo,70686f6e65,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,6e756d626572:org.apache.cassandra.db.marshal.UTF8Type)))");

  BOOST_REQUIRE(data_type->value_type() == CASS_VALUE_TYPE_UDT);

  // Check external UDT

  cass::SharedRefPtr<cass::UserType> udt = static_cast<cass::SharedRefPtr<cass::UserType> >(data_type);

  BOOST_CHECK(udt->keyspace() == "foo");
  BOOST_CHECK(udt->type_name() == "address");
  BOOST_REQUIRE(udt->fields().size() == 3);

  cass::UserType::FieldVec::const_iterator i;

  i = udt->fields().begin();

  BOOST_CHECK(i->name == "street");
  BOOST_CHECK(i->type->value_type() == CASS_VALUE_TYPE_TEXT);

  ++i;

  BOOST_CHECK(i->name == "zipcode");
  BOOST_CHECK(i->type->value_type() == CASS_VALUE_TYPE_INT);

  ++i;

  BOOST_CHECK(i->name == "phones");
  BOOST_REQUIRE(i->type->value_type() == CASS_VALUE_TYPE_SET);

  cass::SharedRefPtr<cass::CollectionType> collection
      = static_cast<cass::SharedRefPtr<cass::CollectionType> >(i->type);

  BOOST_REQUIRE(collection->types().size() == 1);
  BOOST_REQUIRE(collection->types()[0]->value_type() == CASS_VALUE_TYPE_UDT);

  // Check internal UDT

  udt = static_cast<cass::SharedRefPtr<cass::UserType> >(collection->types()[0]);

  BOOST_CHECK(udt->keyspace() == "foo");
  BOOST_CHECK(udt->type_name() == "phone");
  BOOST_REQUIRE(udt->fields().size() == 2);

  i = udt->fields().begin();

  BOOST_CHECK(i->name == "name");
  BOOST_CHECK(i->type->value_type() == CASS_VALUE_TYPE_TEXT);

  ++i;

  BOOST_CHECK(i->name == "number");
  BOOST_CHECK(i->type->value_type() == CASS_VALUE_TYPE_TEXT);
}

BOOST_AUTO_TEST_CASE(tuple)
{
  cass::SharedRefPtr<cass::DataType> data_type
      = cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.TupleType("
                                    "org.apache.cassandra.db.marshal.Int32Type,"
                                    "org.apache.cassandra.db.marshal.UTF8Type,"
                                    "org.apache.cassandra.db.marshal.FloatType)");

  BOOST_REQUIRE(data_type->value_type() == CASS_VALUE_TYPE_TUPLE);

  cass::SharedRefPtr<cass::CollectionType> tuple = static_cast<cass::SharedRefPtr<cass::CollectionType> >(data_type);

  BOOST_REQUIRE(tuple->types().size() == 3);

  BOOST_REQUIRE(tuple->types()[0]->value_type() == CASS_VALUE_TYPE_INT);
  BOOST_REQUIRE(tuple->types()[1]->value_type() == CASS_VALUE_TYPE_TEXT);
  BOOST_REQUIRE(tuple->types()[2]->value_type() == CASS_VALUE_TYPE_FLOAT);
}

BOOST_AUTO_TEST_CASE(nested_collections)
{
  cass::SharedRefPtr<cass::DataType> data_type
      = cass::TypeParser::parse_one("org.apache.cassandra.db.marshal.MapType("
                                    "org.apache.cassandra.db.marshal.UTF8Type,"
                                    "org.apache.cassandra.db.marshal.FrozenType("
                                    "org.apache.cassandra.db.marshal.MapType("
                                    "org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)))");

  BOOST_REQUIRE(data_type->value_type() == CASS_VALUE_TYPE_MAP);

  cass::SharedRefPtr<cass::CollectionType> collection
      = static_cast<cass::SharedRefPtr<cass::CollectionType> >(data_type);

  BOOST_REQUIRE(collection->types().size() == 2);

  BOOST_CHECK(collection->types()[0]->value_type() == CASS_VALUE_TYPE_TEXT);

  BOOST_REQUIRE(collection->types()[1]->value_type() == CASS_VALUE_TYPE_MAP);

  cass::SharedRefPtr<cass::CollectionType> nested_collection
      = static_cast<cass::SharedRefPtr<cass::CollectionType> >(collection->types()[1]);

  BOOST_REQUIRE(nested_collection->types().size() == 2);
  BOOST_CHECK(nested_collection->types()[0]->value_type() == CASS_VALUE_TYPE_INT);
  BOOST_CHECK(nested_collection->types()[1]->value_type() == CASS_VALUE_TYPE_INT);
}

BOOST_AUTO_TEST_CASE(composite)
{
  cass::SharedRefPtr<cass::ParseResult> result
      = cass::TypeParser::parse_with_composite("org.apache.cassandra.db.marshal.CompositeType("
                                               "org.apache.cassandra.db.marshal.AsciiType,"
                                               "org.apache.cassandra.db.marshal.Int32Type)");

  BOOST_CHECK(result->is_composite());

  BOOST_REQUIRE(result->types().size() == 2);
  BOOST_CHECK(result->types()[0]->value_type() == CASS_VALUE_TYPE_ASCII);
  BOOST_CHECK(result->types()[1]->value_type() == CASS_VALUE_TYPE_INT);

  BOOST_REQUIRE(result->reversed().size() == 2);
  BOOST_CHECK(result->reversed()[0] == false);
  BOOST_CHECK(result->reversed()[1] == false);

  BOOST_CHECK(result->collections().empty());
}

BOOST_AUTO_TEST_CASE(not_composite)
{
  cass::SharedRefPtr<cass::ParseResult> result
      = cass::TypeParser::parse_with_composite("org.apache.cassandra.db.marshal.InetAddressType");

  BOOST_REQUIRE(result->types().size() == 1);
  BOOST_CHECK(result->types()[0]->value_type() == CASS_VALUE_TYPE_INET);

  BOOST_REQUIRE(result->reversed().size() == 1);
  BOOST_CHECK(result->reversed()[0] == false);
}

BOOST_AUTO_TEST_CASE(composite_with_reversed)
{
  cass::SharedRefPtr<cass::ParseResult> result
      = cass::TypeParser::parse_with_composite("org.apache.cassandra.db.marshal.CompositeType("
                                               "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.AsciiType),"
                                               "org.apache.cassandra.db.marshal.Int32Type)");

  BOOST_CHECK(result->is_composite());

  BOOST_REQUIRE(result->types().size() == 2);
  BOOST_CHECK(result->types()[0]->value_type() == CASS_VALUE_TYPE_ASCII);
  BOOST_CHECK(result->types()[1]->value_type() == CASS_VALUE_TYPE_INT);

  BOOST_REQUIRE(result->reversed().size() == 2);
  BOOST_CHECK(result->reversed()[0] == true);
  BOOST_CHECK(result->reversed()[1] == false);

  BOOST_CHECK(result->collections().empty());
}

BOOST_AUTO_TEST_CASE(composite_with_collections)
{
  cass::SharedRefPtr<cass::ParseResult> result
      = cass::TypeParser::parse_with_composite("org.apache.cassandra.db.marshal.CompositeType("
                                               "org.apache.cassandra.db.marshal.Int32Type, "
                                               "org.apache.cassandra.db.marshal.UTF8Type,"
                                               "org.apache.cassandra.db.marshal.ColumnToCollectionType("
                                               "6162:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type),"
                                               "4A4b4C4D4e4F:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type),"
                                               "6A6b6C6D6e6F:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.LongType)"
                                               "))");

  BOOST_CHECK(result->is_composite());

  BOOST_REQUIRE(result->types().size() == 2);
  BOOST_CHECK(result->types()[0]->value_type() == CASS_VALUE_TYPE_INT);
  BOOST_CHECK(result->types()[1]->value_type() == CASS_VALUE_TYPE_TEXT);

  BOOST_REQUIRE(result->reversed().size() == 2);
  BOOST_CHECK(result->reversed()[0] == false);
  BOOST_CHECK(result->reversed()[1] == false);

  BOOST_REQUIRE(result->collections().size() == 3);

  cass::ParseResult::CollectionMap::const_iterator i;

  i = result->collections().find("ab");
  cass::SharedRefPtr<cass::CollectionType> collection;
  BOOST_REQUIRE(i != result->collections().end());
  BOOST_REQUIRE(i->second->value_type() == CASS_VALUE_TYPE_LIST);
  collection = static_cast<cass::SharedRefPtr<cass::CollectionType> >(i->second);
  BOOST_REQUIRE(collection->types().size() == 1);
  BOOST_CHECK(collection->types()[0]->value_type() == CASS_VALUE_TYPE_INT);

  i = result->collections().find("JKLMNO");
  BOOST_REQUIRE(i != result->collections().end());
  BOOST_CHECK(i->second->value_type() == CASS_VALUE_TYPE_SET);
  collection = static_cast<cass::SharedRefPtr<cass::CollectionType> >(i->second);
  BOOST_REQUIRE(collection->types().size() == 1);
  BOOST_CHECK(collection->types()[0]->value_type() == CASS_VALUE_TYPE_TEXT);

  i = result->collections().find("jklmno");
  BOOST_REQUIRE(i != result->collections().end());
  BOOST_CHECK(i->second->value_type() == CASS_VALUE_TYPE_MAP);
  collection = static_cast<cass::SharedRefPtr<cass::CollectionType> >(i->second);
  BOOST_REQUIRE(collection->types().size() == 2);
  BOOST_CHECK(collection->types()[0]->value_type() == CASS_VALUE_TYPE_TEXT);
  BOOST_CHECK(collection->types()[1]->value_type() == CASS_VALUE_TYPE_BIGINT);
}

BOOST_AUTO_TEST_SUITE_END()
