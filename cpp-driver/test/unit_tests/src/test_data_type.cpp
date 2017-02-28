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

#include "cassandra.h"
#include "data_type.hpp"

#include <boost/test/unit_test.hpp>

class DataTypeWrapper {
public:
  DataTypeWrapper(CassDataType* data_type)
    : data_type_(data_type) { }

  ~DataTypeWrapper() {
    cass_data_type_free(data_type_);
  }

  operator CassDataType*() {
    return data_type_;
  }

private:
  CassDataType* data_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(DataTypeWrapper);
};

BOOST_AUTO_TEST_SUITE(data_type)

BOOST_AUTO_TEST_CASE(keyspace_and_type_name)
{
  // Verify names
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_UDT));

    BOOST_CHECK_EQUAL(cass_data_type_set_keyspace(data_type, "keyspace1"), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_set_type_name(data_type, "type_name1"), CASS_OK);

    const char* name;
    size_t name_length;

    BOOST_REQUIRE_EQUAL(cass_data_type_keyspace(data_type, &name, &name_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(name, name_length), "keyspace1");

    BOOST_REQUIRE_EQUAL(cass_data_type_type_name(data_type, &name, &name_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(name, name_length), "type_name1");
  }

  // Invalid type
  {
    // Only UDT data types support keyspace and type name
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_LIST));

    BOOST_CHECK_EQUAL(cass_data_type_set_keyspace(data_type, "keyspace1"),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    BOOST_CHECK_EQUAL(cass_data_type_set_type_name(data_type, "type_name1"),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const char* name;
    size_t name_length;

    BOOST_CHECK_EQUAL(cass_data_type_keyspace(data_type, &name, &name_length),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    BOOST_CHECK_EQUAL(cass_data_type_type_name(data_type, &name, &name_length),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);
  }
}

BOOST_AUTO_TEST_CASE(class_name)
{
  // Verify names
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_CUSTOM));

    BOOST_CHECK_EQUAL(cass_data_type_set_class_name(data_type, "class_name1"), CASS_OK);

    const char* name;
    size_t name_length;

    BOOST_REQUIRE_EQUAL(cass_data_type_class_name(data_type, &name, &name_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(name, name_length), "class_name1");
  }

  // Invalid type
  {
    // Only custom data types support class name
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_UDT));

    BOOST_CHECK_EQUAL(cass_data_type_set_class_name(data_type, "class_name1"),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const char* name;
    size_t name_length;

    BOOST_CHECK_EQUAL(cass_data_type_class_name(data_type, &name, &name_length),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);
  }
}

BOOST_AUTO_TEST_CASE(from_existing)
{
  // From an existing custom type
  {
    DataTypeWrapper data_type_existing(cass_data_type_new(CASS_VALUE_TYPE_CUSTOM));

    BOOST_CHECK_EQUAL(cass_data_type_set_class_name(data_type_existing, "class_name1"), CASS_OK);

    // Copy custom type and verify values
    DataTypeWrapper data_type_copy(cass_data_type_new_from_existing(data_type_existing));
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type_copy), CASS_VALUE_TYPE_CUSTOM);

    const char* name;
    size_t name_length;

    BOOST_REQUIRE_EQUAL(cass_data_type_class_name(data_type_copy, &name, &name_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(name, name_length), "class_name1");
  }

  // From an existing tuple
  {
    DataTypeWrapper data_type_existing(cass_data_type_new(CASS_VALUE_TYPE_TUPLE));

    // Tuples support an arbitrary number of parameterized types
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type_existing, CASS_VALUE_TYPE_TEXT), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type_existing, CASS_VALUE_TYPE_INT), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type_existing, CASS_VALUE_TYPE_BIGINT), CASS_OK);

    // Copy tuple and verify values
    DataTypeWrapper data_type_copy(cass_data_type_new_from_existing(data_type_existing));
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type_copy), CASS_VALUE_TYPE_TUPLE);

    const CassDataType* sub_data_type;

    sub_data_type = cass_data_type_sub_data_type(data_type_copy, 0);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type(data_type_copy, 1);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type(data_type_copy, 2);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);
  }

  // From an existing UDT
  {
    DataTypeWrapper data_type_existing(cass_data_type_new_udt(3));

    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type_existing, "field1", CASS_VALUE_TYPE_TEXT), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type_existing, "field2", CASS_VALUE_TYPE_INT), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type_existing, "field3", CASS_VALUE_TYPE_BIGINT), CASS_OK);

    BOOST_CHECK_EQUAL(cass_data_type_set_keyspace(data_type_existing, "keyspace1"), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_set_type_name(data_type_existing, "type_name1"), CASS_OK);

    // Copy UDT and verify values
    DataTypeWrapper data_type_copy(cass_data_type_new_from_existing(data_type_existing));
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type_copy), CASS_VALUE_TYPE_UDT);

    const CassDataType* sub_data_type;

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type_copy, "field1");
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type_copy, "field2");
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type_copy, "field3");
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);

    const char* name;
    size_t name_length;

    BOOST_REQUIRE_EQUAL(cass_data_type_keyspace(data_type_copy, &name, &name_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(name, name_length), "keyspace1");

    BOOST_REQUIRE_EQUAL(cass_data_type_type_name(data_type_copy, &name, &name_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(name, name_length), "type_name1");
  }
}

BOOST_AUTO_TEST_CASE(type)
{
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_INT));
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_INT);
  }

  {
    DataTypeWrapper data_type(cass_data_type_new_udt(0));
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_UDT);
  }

  {
    DataTypeWrapper data_type(cass_data_type_new_tuple(0));
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_TUPLE);
  }
}

BOOST_AUTO_TEST_CASE(subtypes)
{
  // List
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_LIST));
    BOOST_CHECK(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_INT) == CASS_OK);

    // Lists only support a single parameterized type
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT),
                      CASS_ERROR_LIB_BAD_PARAMS);

    // List don't support named parameterized types
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_INT),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type),
                      CASS_VALUE_TYPE_INT);
  }

  // Set
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_SET));
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_INT), CASS_OK);

    // Sets only support a single parameterized type
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT),
                      CASS_ERROR_LIB_BAD_PARAMS);

    // Sets don't support named parameterized types
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_INT),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);
  }


  // Map
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_MAP));
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_INT), CASS_OK);

    // Maps only support two parameterized types
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_BIGINT),
                      CASS_ERROR_LIB_BAD_PARAMS);

    // Maps don't support named parameterized types
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_INT),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type;

    sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 1);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);
  }

  // Tuple
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_TUPLE));

    // Tuples support an arbitrary number of parameterized types
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_INT), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_BIGINT), CASS_OK);

    // Tuples don't support named parameterized types
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_INT),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type;

    sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 1);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 2);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);

    const char* name;
    size_t name_length;

    // Tuples don't support named fields
    BOOST_CHECK_EQUAL(cass_data_type_sub_type_name(data_type, 0, &name, &name_length),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);
  }

  // UDT
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_UDT));

    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_TEXT), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type, "field2", CASS_VALUE_TYPE_INT), CASS_OK);
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type_by_name(data_type, "field3", CASS_VALUE_TYPE_BIGINT), CASS_OK);

    // UDTs don't support adding fields without a name
    BOOST_CHECK_EQUAL(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT),
                      CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type;

    // By index
    sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 1);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 2);
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);

    // Invalid index
    BOOST_CHECK(cass_data_type_sub_data_type(data_type, 3) == NULL);

    // By name
    sub_data_type = cass_data_type_sub_data_type_by_name(data_type, "field1");
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type, "field2");
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type, "field3");
    BOOST_REQUIRE(sub_data_type != NULL);
    BOOST_CHECK_EQUAL(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);

    // Invalid name
    BOOST_CHECK(cass_data_type_sub_data_type_by_name(data_type, "field4") == NULL);

    // Field names
    const char* name;
    size_t name_length;

    BOOST_REQUIRE_EQUAL(cass_data_type_sub_type_name(data_type, 0, &name, &name_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(name, name_length), "field1");

    BOOST_REQUIRE_EQUAL(cass_data_type_sub_type_name(data_type, 1, &name, &name_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(name, name_length), "field2");

    BOOST_REQUIRE_EQUAL(cass_data_type_sub_type_name(data_type, 2, &name, &name_length), CASS_OK);
    BOOST_CHECK_EQUAL(std::string(name, name_length), "field3");
  }
}

BOOST_AUTO_TEST_CASE(value_types_by_class)
{
#define XX_VALUE_TYPE(name, type, cql, klass)             \
  BOOST_CHECK(strlen(klass) == 0 ||                       \
              cass::ValueTypes::by_class(klass) == name); \

  CASS_VALUE_TYPE_MAPPING(XX_VALUE_TYPE)
#undef XX_VALUE_TYPE
}

BOOST_AUTO_TEST_CASE(value_types_by_class_case_insensitive)
{
#define XX_VALUE_TYPE(name, type, cql, klass) {                              \
    std::string upper(klass);                                                \
    std::transform(upper.begin(), upper.end(), upper.begin(), toupper);      \
    BOOST_CHECK(upper.empty() || cass::ValueTypes::by_class(upper) == name); \
  }

  CASS_VALUE_TYPE_MAPPING(XX_VALUE_TYPE)
#undef XX_VALUE_TYPE
}

BOOST_AUTO_TEST_CASE(value_types_by_cql)
{
#define XX_VALUE_TYPE(name, type, cql, klass)         \
  BOOST_CHECK(strlen(cql) == 0 ||                     \
              cass::ValueTypes::by_cql(cql) == name); \

  CASS_VALUE_TYPE_MAPPING(XX_VALUE_TYPE)
#undef XX_VALUE_TYPE
}

BOOST_AUTO_TEST_CASE(value_types_by_cql_case_insensitive)
{
#define XX_VALUE_TYPE(name, type, cql, klass) {                            \
    std::string upper(cql);                                                \
    std::transform(upper.begin(), upper.end(), upper.begin(), toupper);    \
    BOOST_CHECK(upper.empty() || cass::ValueTypes::by_cql(upper) == name); \
  }

  CASS_VALUE_TYPE_MAPPING(XX_VALUE_TYPE)
#undef XX_VALUE_TYPE
}

BOOST_AUTO_TEST_CASE(simple_data_type_cache)
{
  cass::SimpleDataTypeCache cache;

  cass::DataType::ConstPtr by_class = cache.by_class("org.apache.cassandra.db.marshal.AsciiType");
  cass::DataType::ConstPtr by_cql = cache.by_cql("ascii");
  cass::DataType::ConstPtr by_value_type = cache.by_value_type(CASS_VALUE_TYPE_ASCII);

  BOOST_CHECK(by_class->value_type() == CASS_VALUE_TYPE_ASCII);
  BOOST_CHECK(by_cql->value_type() == CASS_VALUE_TYPE_ASCII);
  BOOST_CHECK(by_value_type->value_type() == CASS_VALUE_TYPE_ASCII);

  BOOST_CHECK(by_class.get() == by_cql.get());
  BOOST_CHECK(by_class.get() == by_value_type.get());
}

BOOST_AUTO_TEST_SUITE_END()


