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

#include "encode.hpp"

#include <boost/test/unit_test.hpp>
#include <time.h>

using namespace cass;

BOOST_AUTO_TEST_SUITE(encode_duration)

BOOST_AUTO_TEST_CASE(base)
{
  CassDuration value(0, 0, 0);
  Buffer result = encode(value);
  BOOST_CHECK_EQUAL(3, result.size());
  const char* result_data = result.data();
  BOOST_CHECK_EQUAL(result_data[0], 0);
  BOOST_CHECK_EQUAL(result_data[1], 0);
  BOOST_CHECK_EQUAL(result_data[2], 0);
}

BOOST_AUTO_TEST_CASE(simple_positive)
{
  CassDuration value(1, 2, 3);
  Buffer result = encode(value);
  BOOST_CHECK_EQUAL(3, result.size());
  const char* result_data = result.data();
  BOOST_CHECK_EQUAL(result_data[0], 2);
  BOOST_CHECK_EQUAL(result_data[1], 4);
  BOOST_CHECK_EQUAL(result_data[2], 6);
}

BOOST_AUTO_TEST_CASE(simple_negative)
{
  CassDuration value(-1, -2, -3);
  Buffer result = encode(value);
  BOOST_CHECK_EQUAL(3, result.size());
  const char* result_data = result.data();
  BOOST_CHECK_EQUAL(result_data[0], 1);
  BOOST_CHECK_EQUAL(result_data[1], 3);
  BOOST_CHECK_EQUAL(result_data[2], 5);
}

BOOST_AUTO_TEST_CASE(edge_positive)
{
  CassDuration value((1UL << 31) - 1, (1UL << 31) - 1, (1ULL << 63) - 1);
  Buffer result = encode(value);
  BOOST_CHECK_EQUAL(19, result.size());
  unsigned const char* result_data = reinterpret_cast<unsigned const char*>(result.data());

  // The first 5 bytes represent (1UL << 31 - 1), the max 32-bit number. Byte 0
  // has the first 4 bits set to indicate that there are 4 bytes beyond this one that
  // define this field (each field is a vint of a zigzag encoding of the original
  // value). Encoding places the least-significant byte at byte 4 and works backwards
  // to record more significant bytes. Zigzag encoding just left shifts a value
  // by one bit for positive values, so byte 4 ends in a 0. The last 9 bytes represent
  // (1UL << 63 - 1), the max 64-bit integer. Byte 10 has the first 8 bits set to indicate
  // there are 8 follow up bytes to encode this value. The last byte also ends in a 0 because
  // it is a positive value.

  BOOST_CHECK_EQUAL(result_data[0], 0xf0);
  for (int ind = 1; ind < 4; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }
  BOOST_CHECK_EQUAL(result_data[4], 0xfe);

  // The same interpretation applies to "days" and "nanos".
  BOOST_CHECK_EQUAL(result_data[5], 0xf0);
  for (int ind = 6; ind < 9; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }
  BOOST_CHECK_EQUAL(result_data[9], 0xfe);

  BOOST_CHECK_EQUAL(result_data[10], 0xff);
  for (int ind = 11; ind < 18; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }
  BOOST_CHECK_EQUAL(result_data[18], 0xfe);
}

BOOST_AUTO_TEST_CASE(edge_negative)
{
  CassDuration value(1L << 31, 1L << 31, 1LL << 63);
  Buffer result = encode(value);
  BOOST_CHECK_EQUAL(19, result.size());
  unsigned const char* result_data = reinterpret_cast<unsigned const char*>(result.data());

  // We have 5-bytes for 1L << 31, the min 32-bit number. The zigzag
  // representation is 4 bytes of 0xff, and the first byte is 0xf0 to say we have
  // 4 bytes of value beyond these size-spec-bits. The last 9 bytes represent
  // 1L << 63 with all the first byte bits set to indicate 8 more bytes are needed to
  // encode this value.

  BOOST_CHECK_EQUAL(result_data[0], 0xf0);
  for (int ind = 1; ind <= 4; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }

  // The same is true for "days" and "nanos".
  BOOST_CHECK_EQUAL(result_data[5], 0xf0);
  for (int ind = 6; ind <= 9; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }

  BOOST_CHECK_EQUAL(result_data[10], 0xff);
  for (int ind = 11; ind <= 18; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }
}

BOOST_AUTO_TEST_SUITE_END()
