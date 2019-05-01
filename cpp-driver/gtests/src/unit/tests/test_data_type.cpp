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

#include "cassandra.h"
#include "data_type.hpp"

#include <algorithm>
#include <ctype.h>

using namespace datastax;
using namespace datastax::internal::core;

class DataTypeWrapper {
public:
  DataTypeWrapper(CassDataType* data_type)
      : data_type_(data_type) {}

  ~DataTypeWrapper() { cass_data_type_free(data_type_); }

  operator CassDataType*() { return data_type_; }

private:
  CassDataType* data_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(DataTypeWrapper);
};

TEST(DataTypeUnitTest, KeyspaceAndTypeName) {
  // Verify names
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_UDT));

    EXPECT_EQ(cass_data_type_set_keyspace(data_type, "keyspace1"), CASS_OK);
    EXPECT_EQ(cass_data_type_set_type_name(data_type, "type_name1"), CASS_OK);

    const char* name;
    size_t name_length;

    ASSERT_EQ(cass_data_type_keyspace(data_type, &name, &name_length), CASS_OK);
    EXPECT_EQ(String(name, name_length), "keyspace1");

    ASSERT_EQ(cass_data_type_type_name(data_type, &name, &name_length), CASS_OK);
    EXPECT_EQ(String(name, name_length), "type_name1");
  }

  // Invalid type
  {
    // Only UDT data types support keyspace and type name
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_LIST));

    EXPECT_EQ(cass_data_type_set_keyspace(data_type, "keyspace1"),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    EXPECT_EQ(cass_data_type_set_type_name(data_type, "type_name1"),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const char* name;
    size_t name_length;

    EXPECT_EQ(cass_data_type_keyspace(data_type, &name, &name_length),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    EXPECT_EQ(cass_data_type_type_name(data_type, &name, &name_length),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);
  }
}

TEST(DataTypeUnitTest, ClassName) {
  // Verify names
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_CUSTOM));

    EXPECT_EQ(cass_data_type_set_class_name(data_type, "class_name1"), CASS_OK);

    const char* name;
    size_t name_length;

    ASSERT_EQ(cass_data_type_class_name(data_type, &name, &name_length), CASS_OK);
    EXPECT_EQ(String(name, name_length), "class_name1");
  }

  // Invalid type
  {
    // Only custom data types support class name
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_UDT));

    EXPECT_EQ(cass_data_type_set_class_name(data_type, "class_name1"),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const char* name;
    size_t name_length;

    EXPECT_EQ(cass_data_type_class_name(data_type, &name, &name_length),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);
  }
}

TEST(DataTypeUnitTest, FromExisting) {
  // From an existing custom type
  {
    DataTypeWrapper data_type_existing(cass_data_type_new(CASS_VALUE_TYPE_CUSTOM));

    EXPECT_EQ(cass_data_type_set_class_name(data_type_existing, "class_name1"), CASS_OK);

    // Copy custom type and verify values
    DataTypeWrapper data_type_copy(cass_data_type_new_from_existing(data_type_existing));
    EXPECT_EQ(cass_data_type_type(data_type_copy), CASS_VALUE_TYPE_CUSTOM);

    const char* name;
    size_t name_length;

    ASSERT_EQ(cass_data_type_class_name(data_type_copy, &name, &name_length), CASS_OK);
    EXPECT_EQ(String(name, name_length), "class_name1");
  }

  // From an existing tuple
  {
    DataTypeWrapper data_type_existing(cass_data_type_new(CASS_VALUE_TYPE_TUPLE));

    // Tuples support an arbitrary number of parameterized types
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type_existing, CASS_VALUE_TYPE_TEXT), CASS_OK);
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type_existing, CASS_VALUE_TYPE_INT), CASS_OK);
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type_existing, CASS_VALUE_TYPE_BIGINT),
              CASS_OK);

    // Copy tuple and verify values
    DataTypeWrapper data_type_copy(cass_data_type_new_from_existing(data_type_existing));
    EXPECT_EQ(cass_data_type_type(data_type_copy), CASS_VALUE_TYPE_TUPLE);

    const CassDataType* sub_data_type;

    sub_data_type = cass_data_type_sub_data_type(data_type_copy, 0);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type(data_type_copy, 1);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type(data_type_copy, 2);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);
  }

  // From an existing UDT
  {
    DataTypeWrapper data_type_existing(cass_data_type_new_udt(3));

    EXPECT_EQ(cass_data_type_add_sub_value_type_by_name(data_type_existing, "field1",
                                                        CASS_VALUE_TYPE_TEXT),
              CASS_OK);
    EXPECT_EQ(cass_data_type_add_sub_value_type_by_name(data_type_existing, "field2",
                                                        CASS_VALUE_TYPE_INT),
              CASS_OK);
    EXPECT_EQ(cass_data_type_add_sub_value_type_by_name(data_type_existing, "field3",
                                                        CASS_VALUE_TYPE_BIGINT),
              CASS_OK);

    EXPECT_EQ(cass_data_type_set_keyspace(data_type_existing, "keyspace1"), CASS_OK);
    EXPECT_EQ(cass_data_type_set_type_name(data_type_existing, "type_name1"), CASS_OK);

    // Copy UDT and verify values
    DataTypeWrapper data_type_copy(cass_data_type_new_from_existing(data_type_existing));
    EXPECT_EQ(cass_data_type_type(data_type_copy), CASS_VALUE_TYPE_UDT);

    const CassDataType* sub_data_type;

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type_copy, "field1");
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type_copy, "field2");
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type_copy, "field3");
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);

    const char* name;
    size_t name_length;

    ASSERT_EQ(cass_data_type_keyspace(data_type_copy, &name, &name_length), CASS_OK);
    EXPECT_EQ(String(name, name_length), "keyspace1");

    ASSERT_EQ(cass_data_type_type_name(data_type_copy, &name, &name_length), CASS_OK);
    EXPECT_EQ(String(name, name_length), "type_name1");
  }
}

TEST(DataTypeUnitTest, CheckValueType) {
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_INT));
    EXPECT_EQ(cass_data_type_type(data_type), CASS_VALUE_TYPE_INT);
  }

  {
    DataTypeWrapper data_type(cass_data_type_new_udt(0));
    EXPECT_EQ(cass_data_type_type(data_type), CASS_VALUE_TYPE_UDT);
  }

  {
    DataTypeWrapper data_type(cass_data_type_new_tuple(0));
    EXPECT_EQ(cass_data_type_type(data_type), CASS_VALUE_TYPE_TUPLE);
  }
}

TEST(DataTypeUnitTest, CheckSubValueType) {
  // List
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_LIST));
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_INT), CASS_OK);

    // Lists only support a single parameterized type
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT),
              CASS_ERROR_LIB_BAD_PARAMS);

    // List don't support named parameterized types
    EXPECT_EQ(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_INT),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);
  }

  // Set
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_SET));
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_INT), CASS_OK);

    // Sets only support a single parameterized type
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT),
              CASS_ERROR_LIB_BAD_PARAMS);

    // Sets don't support named parameterized types
    EXPECT_EQ(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_INT),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);
  }

  // Map
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_MAP));
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT), CASS_OK);
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_INT), CASS_OK);

    // Maps only support two parameterized types
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_BIGINT),
              CASS_ERROR_LIB_BAD_PARAMS);

    // Maps don't support named parameterized types
    EXPECT_EQ(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_INT),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type;

    sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 1);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);
  }

  // Tuple
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_TUPLE));

    // Tuples support an arbitrary number of parameterized types
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT), CASS_OK);
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_INT), CASS_OK);
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_BIGINT), CASS_OK);

    // Tuples don't support named parameterized types
    EXPECT_EQ(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_INT),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type;

    sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 1);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 2);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);

    const char* name;
    size_t name_length;

    // Tuples don't support named fields
    EXPECT_EQ(cass_data_type_sub_type_name(data_type, 0, &name, &name_length),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);
  }

  // UDT
  {
    DataTypeWrapper data_type(cass_data_type_new(CASS_VALUE_TYPE_UDT));

    EXPECT_EQ(cass_data_type_add_sub_value_type_by_name(data_type, "field1", CASS_VALUE_TYPE_TEXT),
              CASS_OK);
    EXPECT_EQ(cass_data_type_add_sub_value_type_by_name(data_type, "field2", CASS_VALUE_TYPE_INT),
              CASS_OK);
    EXPECT_EQ(
        cass_data_type_add_sub_value_type_by_name(data_type, "field3", CASS_VALUE_TYPE_BIGINT),
        CASS_OK);

    // UDTs don't support adding fields without a name
    EXPECT_EQ(cass_data_type_add_sub_value_type(data_type, CASS_VALUE_TYPE_TEXT),
              CASS_ERROR_LIB_INVALID_VALUE_TYPE);

    const CassDataType* sub_data_type;

    // By index
    sub_data_type = cass_data_type_sub_data_type(data_type, 0);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 1);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type(data_type, 2);
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);

    // Invalid index
    ASSERT_TRUE(cass_data_type_sub_data_type(data_type, 3) == NULL);

    // By name
    sub_data_type = cass_data_type_sub_data_type_by_name(data_type, "field1");
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_TEXT);

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type, "field2");
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_INT);

    sub_data_type = cass_data_type_sub_data_type_by_name(data_type, "field3");
    ASSERT_TRUE(sub_data_type != NULL);
    EXPECT_EQ(cass_data_type_type(sub_data_type), CASS_VALUE_TYPE_BIGINT);

    // Invalid name
    ASSERT_TRUE(cass_data_type_sub_data_type_by_name(data_type, "field4") == NULL);

    // Field names
    const char* name;
    size_t name_length;

    ASSERT_EQ(cass_data_type_sub_type_name(data_type, 0, &name, &name_length), CASS_OK);
    EXPECT_EQ(String(name, name_length), "field1");

    ASSERT_EQ(cass_data_type_sub_type_name(data_type, 1, &name, &name_length), CASS_OK);
    EXPECT_EQ(String(name, name_length), "field2");

    ASSERT_EQ(cass_data_type_sub_type_name(data_type, 2, &name, &name_length), CASS_OK);
    EXPECT_EQ(String(name, name_length), "field3");
  }
}

TEST(DataTypeUnitTest, CheckValueTypeByClass) {
#define XX_VALUE_TYPE(name, type, cql, klass) \
  EXPECT_TRUE(strlen(klass) == 0 || ValueTypes::by_class(klass) == name);

  CASS_VALUE_TYPE_MAPPING(XX_VALUE_TYPE)
#undef XX_VALUE_TYPE
}

TEST(DataTypeUnitTest, CheckValueTypeByClassCaseInsensitive){
#define XX_VALUE_TYPE(name, type, cql, klass)                           \
  {                                                                     \
    String upper(klass);                                                \
    std::transform(upper.begin(), upper.end(), upper.begin(), toupper); \
    EXPECT_TRUE(upper.empty() || ValueTypes::by_class(upper) == name);  \
  }

  CASS_VALUE_TYPE_MAPPING(XX_VALUE_TYPE)
#undef XX_VALUE_TYPE
}

TEST(DataTypeUnitTest, CheckValueTypesByCql) {
#define XX_VALUE_TYPE(name, type, cql, klass) \
  EXPECT_TRUE(strlen(cql) == 0 || ValueTypes::by_cql(cql) == name);

  CASS_VALUE_TYPE_MAPPING(XX_VALUE_TYPE)
#undef XX_VALUE_TYPE
}

TEST(DataTypeUnitTest, CheckValueTypesByCqlCasInsensitive){
#define XX_VALUE_TYPE(name, type, cql, klass)                           \
  {                                                                     \
    String upper(cql);                                                  \
    std::transform(upper.begin(), upper.end(), upper.begin(), toupper); \
    EXPECT_TRUE(upper.empty() || ValueTypes::by_cql(upper) == name);    \
  }

  CASS_VALUE_TYPE_MAPPING(XX_VALUE_TYPE)
#undef XX_VALUE_TYPE
}

TEST(DataTypeUnitTest, SimpleDataTypeCache) {
  SimpleDataTypeCache cache;

  DataType::ConstPtr by_class = cache.by_class("org.apache.cassandra.db.marshal.AsciiType");
  DataType::ConstPtr by_cql = cache.by_cql("ascii");
  DataType::ConstPtr by_value_type = cache.by_value_type(CASS_VALUE_TYPE_ASCII);

  EXPECT_EQ(by_class->value_type(), CASS_VALUE_TYPE_ASCII);
  EXPECT_EQ(by_cql->value_type(), CASS_VALUE_TYPE_ASCII);
  EXPECT_EQ(by_value_type->value_type(), CASS_VALUE_TYPE_ASCII);

  EXPECT_EQ(by_class.get(), by_cql.get());
  EXPECT_EQ(by_class.get(), by_value_type.get());
}
