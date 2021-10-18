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

#include "buffer.hpp"
#include "cassandra.h"
#include "string.hpp"
#include "value.hpp"

#include <time.h>

using namespace datastax::internal::core;
using namespace datastax;

// The following CassValue's are used in tests as "bad data".

// Create a CassValue representing a text type.
static DataType::ConstPtr s_text_type(new DataType(CASS_VALUE_TYPE_TEXT));
static Value s_text_value_Value(s_text_type, Decoder(NULL, 0));
static CassValue* s_text_value = CassValue::to(&s_text_value_Value);

// ST is simple-type name (e.g. int8 and the like) and T is the full type name (e.g. cass_int8_t,
// CassUuid, etc.).
#define TEST_TYPE(ST, T, SLT)                                                                 \
  TEST(ValueUnitTest, Bad##ST) {                                                              \
    T output;                                                                                 \
    EXPECT_EQ(cass_value_get_##ST(s_text_value, &output), CASS_ERROR_LIB_INVALID_VALUE_TYPE); \
    DataType::ConstPtr data_type(new DataType(CASS_VALUE_TYPE_##SLT));                        \
    Value null_value(data_type);                                                              \
    EXPECT_EQ(cass_value_get_##ST(NULL, &output), CASS_ERROR_LIB_NULL_VALUE);                 \
    EXPECT_EQ(cass_value_get_##ST(CassValue::to(&null_value), &output),                       \
              CASS_ERROR_LIB_NULL_VALUE);                                                     \
    Value invalid_value(data_type, Decoder("", 0));                                           \
    EXPECT_EQ(cass_value_get_##ST(CassValue::to(&invalid_value), &output),                    \
              CASS_ERROR_LIB_NOT_ENOUGH_DATA);                                                \
  }

#define TEST_SIMPLE_TYPE(T, SLT) TEST_TYPE(T, cass_##T##_t, SLT)

TEST_SIMPLE_TYPE(int8, TINY_INT)
TEST_SIMPLE_TYPE(int16, SMALL_INT)
TEST_SIMPLE_TYPE(int32, INT)
TEST_SIMPLE_TYPE(uint32, DATE)
TEST_SIMPLE_TYPE(int64, BIGINT)
TEST_SIMPLE_TYPE(float, FLOAT)
TEST_SIMPLE_TYPE(double, DOUBLE)
TEST_SIMPLE_TYPE(bool, BOOLEAN)
TEST_TYPE(uuid, CassUuid, UUID)

TEST(ValueUnitTest, BadBytes) {
  const cass_byte_t* bytes = NULL;
  size_t bytes_size = 0;
  EXPECT_EQ(cass_value_get_bytes(NULL, &bytes, &bytes_size), CASS_ERROR_LIB_NULL_VALUE);
}

TEST(ValueUnitTest, BadString) {
  const char* str = NULL;
  size_t str_length = 0;
  EXPECT_EQ(cass_value_get_string(NULL, &str, &str_length), CASS_ERROR_LIB_NULL_VALUE);
}

TEST(ValueUnitTest, BadInet) {
  CassInet inet;
  DataType::ConstPtr data_type(new DataType(CASS_VALUE_TYPE_INET));

  EXPECT_EQ(cass_value_get_inet(NULL, &inet), CASS_ERROR_LIB_NULL_VALUE);

  Value null_value(data_type);
  EXPECT_EQ(cass_value_get_inet(CassValue::to(&null_value), &inet), CASS_ERROR_LIB_NULL_VALUE);

  Value invalid_value(data_type, Decoder("12345678901234567", 17));
  EXPECT_EQ(cass_value_get_inet(CassValue::to(&invalid_value), &inet), CASS_ERROR_LIB_INVALID_DATA);
}

TEST(ValueUnitTest, BadDuration) {
  cass_int32_t months, days;
  cass_int64_t nanos;
  EXPECT_EQ(cass_value_get_duration(s_text_value, &months, &days, &nanos),
            CASS_ERROR_LIB_INVALID_VALUE_TYPE);

  DataType::ConstPtr data_type(new DataType(CASS_VALUE_TYPE_DURATION));
  Value invalid_value(data_type, Decoder("", 0));
  EXPECT_EQ(cass_value_get_duration(CassValue::to(&invalid_value), &months, &days, &nanos),
            CASS_ERROR_LIB_NOT_ENOUGH_DATA);
}

TEST(ValueUnitTest, BadDecimal) {
  const cass_byte_t* varint;
  size_t varint_size;
  cass_int32_t scale;
  EXPECT_EQ(cass_value_get_decimal(s_text_value, &varint, &varint_size, &scale),
            CASS_ERROR_LIB_INVALID_VALUE_TYPE);

  DataType::ConstPtr data_type(new DataType(CASS_VALUE_TYPE_DECIMAL));
  Value invalid_value(data_type, Decoder("", 0));
  EXPECT_EQ(cass_value_get_decimal(CassValue::to(&invalid_value), &varint, &varint_size, &scale),
            CASS_ERROR_LIB_NOT_ENOUGH_DATA);
}

TEST(ValueUnitTest, NullInNextRow) {
  DataType::ConstPtr data_type(new DataType(CASS_VALUE_TYPE_INT));

  // Size (int32_t) and contents of element
  const signed char input[8] = { 0, 0, 0, 4, 0, 0, 0, 2 };
  Decoder decoder((const char*)input, 12);

  // init with a non null column in a row
  Value value(data_type, 2, decoder);
  EXPECT_FALSE(value.is_null());

  const signed char null_input[4] = { -1, 1, 1, 1 };
  Decoder null_decoder((const char*)null_input, 4);

  // simulate next row null value in the column
  null_decoder.update_value(value);
  EXPECT_TRUE(value.is_null());

  // next row non null value check is_null() returns correct value
  Decoder decoder2((const char*)input, 12);
  decoder2.update_value(value);
  EXPECT_FALSE(value.is_null());
}

TEST(ValueUnitTest, NullElementInCollectionList) {
  const signed char input[12] = {
    -1, -1, -1, -1,            // Element 1 is NULL
    0,  0,  0,  4,  0, 0, 0, 2 // Size (int32_t) and contents of element 2
  };
  Decoder decoder((const char*)input, 12);
  DataType::ConstPtr element_data_type(new DataType(CASS_VALUE_TYPE_INT));
  CollectionType::ConstPtr data_type = CollectionType::list(element_data_type, false);
  Value value(data_type, 2, decoder);
  ASSERT_EQ(cass_true, cass_value_is_collection(CassValue::to(&value)));

  CassIterator* it = cass_iterator_from_collection(CassValue::to(&value));
  EXPECT_EQ(cass_true, cass_iterator_next(it));
  const CassValue* element = cass_iterator_get_value(it);
  EXPECT_EQ(cass_true, cass_value_is_null(element));
  cass_int32_t element_value;
  EXPECT_EQ(cass_true, cass_iterator_next(it));
  EXPECT_EQ(CASS_OK, cass_value_get_int32(element, &element_value));
  EXPECT_EQ(2, element_value);
  cass_iterator_free(it);
}

TEST(ValueUnitTest, NullElementInCollectionMap) {
  const signed char input[21] = {
    -1, -1, -1, -1,               // Key 1 is NULL
    0,  0,  0,  4,  0,   0, 0, 2, // Size (int32_t) and contents of value 1
    0,  0,  0,  1,  'a',          // Key 2 is a
    -1, -1, -1, -1                // Value 2 is NULL
  };
  Decoder decoder((const char*)input, 21);
  DataType::ConstPtr key_data_type(new DataType(CASS_VALUE_TYPE_TEXT));
  DataType::ConstPtr value_data_type(new DataType(CASS_VALUE_TYPE_INT));
  CollectionType::ConstPtr data_type = CollectionType::map(key_data_type, value_data_type, false);
  Value value(data_type, 2, decoder);
  ASSERT_EQ(cass_true, cass_value_is_collection(CassValue::to(&value)));

  CassIterator* it = cass_iterator_from_collection(CassValue::to(&value));
  EXPECT_EQ(cass_true, cass_iterator_next(it));
  const CassValue* element = cass_iterator_get_value(it);
  EXPECT_EQ(cass_true, cass_value_is_null(element));
  cass_int32_t value_value;
  EXPECT_EQ(cass_true, cass_iterator_next(it));
  EXPECT_EQ(CASS_OK, cass_value_get_int32(element, &value_value));
  EXPECT_EQ(2, value_value);

  EXPECT_EQ(cass_true, cass_iterator_next(it));
  element = cass_iterator_get_value(it);
  const char* key_value = NULL;
  size_t key_value_length = 0;
  EXPECT_EQ(CASS_OK, cass_value_get_string(element, &key_value, &key_value_length));
  EXPECT_EQ("a", String(key_value, key_value_length));
  EXPECT_EQ(cass_true, cass_iterator_next(it));
  EXPECT_EQ(cass_true, cass_value_is_null(element));
  cass_iterator_free(it);
}

TEST(ValueUnitTest, NullElementInCollectionSet) {
  const signed char input[12] = {
    0,  0,  0,  4,  0, 0, 0, 2, // Size (int32_t) and contents of element 1
    -1, -1, -1, -1,             // Element 2 is NULL
  };
  Decoder decoder((const char*)input, 12);
  DataType::ConstPtr element_data_type(new DataType(CASS_VALUE_TYPE_INT));
  CollectionType::ConstPtr data_type = CollectionType::set(element_data_type, false);
  Value value(data_type, 2, decoder);
  ASSERT_EQ(cass_true, cass_value_is_collection(CassValue::to(&value)));

  CassIterator* it = cass_iterator_from_collection(CassValue::to(&value));
  EXPECT_EQ(cass_true, cass_iterator_next(it));
  const CassValue* element = cass_iterator_get_value(it);
  cass_int32_t element_value;
  EXPECT_EQ(CASS_OK, cass_value_get_int32(element, &element_value));
  EXPECT_EQ(2, element_value);
  EXPECT_EQ(cass_true, cass_iterator_next(it));
  EXPECT_EQ(cass_true, cass_value_is_null(element));
  cass_iterator_free(it);
}
