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

#include "value.hpp"
#include "cassandra.h"

#include <boost/test/unit_test.hpp>
#include <time.h>

// The following CassValue's are used in tests as "bad data".

// Create a CassValue representing a text type.
static cass::DataType::ConstPtr s_text_type(new cass::DataType(CASS_VALUE_TYPE_TEXT));
static cass::Value s_text_value_Value(4, s_text_type, NULL, 0);
static CassValue* s_text_value = CassValue::to(&s_text_value_Value);

BOOST_AUTO_TEST_SUITE(bad_value)

// ST is simple-type name (e.g. int8 and the like) and T is the full type name (e.g. cass_int8_t, CassUuid, etc.).
#define TEST_TYPE(ST, T) \
BOOST_AUTO_TEST_CASE(bad_##ST) \
{ \
  T output; \
  BOOST_CHECK_EQUAL(cass_value_get_##ST(s_text_value, &output), CASS_ERROR_LIB_INVALID_VALUE_TYPE); \
}

#define TEST_SIMPLE_TYPE(T) TEST_TYPE(T, cass_##T##_t)

TEST_SIMPLE_TYPE(int8)
TEST_SIMPLE_TYPE(int16)
TEST_SIMPLE_TYPE(int32)
TEST_SIMPLE_TYPE(uint32)
TEST_SIMPLE_TYPE(int64)
TEST_SIMPLE_TYPE(float)
TEST_SIMPLE_TYPE(double)
TEST_TYPE(uuid, CassUuid)
TEST_TYPE(inet, CassInet)

BOOST_AUTO_TEST_CASE(bad_duration)
{
  cass_int32_t months, days;
  cass_int64_t nanos;
  BOOST_CHECK_EQUAL(cass_value_get_duration(s_text_value, &months, &days, &nanos), CASS_ERROR_LIB_INVALID_VALUE_TYPE);
}

BOOST_AUTO_TEST_CASE(bad_decimal)
{
  const cass_byte_t* varint;
  size_t varint_size;
  cass_int32_t scale;
  BOOST_CHECK_EQUAL(cass_value_get_decimal(s_text_value, &varint, &varint_size, &scale),
    CASS_ERROR_LIB_INVALID_VALUE_TYPE);
}

BOOST_AUTO_TEST_SUITE_END()
