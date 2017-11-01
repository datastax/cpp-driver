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

#include "value.hpp"
#include "constants.hpp"
#include "cassandra.h"

#include <time.h>

// The following CassValue's are used in tests as "bad data".

// Create a CassValue representing a text type.
static cass::DataType::ConstPtr s_text_type(new cass::DataType(CASS_VALUE_TYPE_TEXT));
static cass::Value s_text_value_Value(-1, s_text_type, NULL, 0);
static CassValue* s_text_value = CassValue::to(&s_text_value_Value);

// ST is simple-type name (e.g. int8 and the like) and T is the full type name (e.g. cass_int8_t, CassUuid, etc.).
#define TEST_TYPE(ST, T, SLT) \
TEST(ValueUnitTest, Bad##ST) \
{ \
  T output; \
  EXPECT_EQ(cass_value_get_##ST(s_text_value, &output), \
  CASS_ERROR_LIB_INVALID_VALUE_TYPE); \
  cass::DataType::ConstPtr data_type(new cass::DataType(CASS_VALUE_TYPE_##SLT)); \
  cass::Value null_value(data_type); \
  EXPECT_EQ(cass_value_get_##ST(NULL, &output), \
  CASS_ERROR_LIB_NULL_VALUE); \
  EXPECT_EQ(cass_value_get_##ST(CassValue::to(&null_value), &output), \
  CASS_ERROR_LIB_NULL_VALUE); \
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

TEST(ValueUnitTest, BadBytes)
{
  const cass_byte_t* bytes = NULL;
  size_t bytes_size = 0;
  EXPECT_EQ(cass_value_get_bytes(NULL, &bytes, &bytes_size),
            CASS_ERROR_LIB_NULL_VALUE);
}

TEST(ValueUnitTest, BadString)
{
  const char* str = NULL;
  size_t str_length = 0;
  EXPECT_EQ(cass_value_get_string(NULL, &str, &str_length),
            CASS_ERROR_LIB_NULL_VALUE);
}

TEST(ValueUnitTest, BadInet)
{
  CassInet inet;
  cass::DataType::ConstPtr data_type(new cass::DataType(CASS_VALUE_TYPE_INET));

  EXPECT_EQ(cass_value_get_inet(NULL, &inet),
            CASS_ERROR_LIB_NULL_VALUE);

  cass::Value null_value(data_type);
  EXPECT_EQ(cass_value_get_inet(CassValue::to(&null_value), &inet),
            CASS_ERROR_LIB_NULL_VALUE);
}

TEST(ValueUnitTest, BadDuration)
{
  cass_int32_t months, days;
  cass_int64_t nanos;
  EXPECT_EQ(cass_value_get_duration(s_text_value, &months, &days, &nanos),
            CASS_ERROR_LIB_INVALID_VALUE_TYPE);
}

TEST(ValueUnitTest, BadDecimal)
{
  const cass_byte_t* varint;
  size_t varint_size;
  cass_int32_t scale;
  EXPECT_EQ(cass_value_get_decimal(s_text_value, &varint, &varint_size, &scale),
            CASS_ERROR_LIB_INVALID_VALUE_TYPE);
}
