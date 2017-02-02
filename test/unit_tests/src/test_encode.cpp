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
  CassDuration value((1ULL << 63) - 1, (1ULL << 63) - 1, (1ULL << 63) - 1);
  Buffer result = encode(value);
  BOOST_CHECK_EQUAL(27, result.size());
  unsigned const char* result_data = reinterpret_cast<unsigned const char*>(result.data());

  // The first 9 bytes represent (1LL<<63 - 1), the max 64-bit number. Byte 0
  // has all bits set to indicate that there are 8 bytes beyond this one that
  // define this field (each field is a vint of a zigzag encoding of the original
  // value). Encoding places the least-significant byte at byte 8 and works backwards
  // to record more significant bytes. Zigzag encoding just left shifts a value
  // by one bit for positive values, so byte 8 ends in a 0.

  for (int ind = 0; ind < 8; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }
  BOOST_CHECK_EQUAL(result_data[8], 0xfe);

  // The same interpretation applies to "days" and "nanos".
  for (int ind = 9; ind < 17; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }
  BOOST_CHECK_EQUAL(result_data[17], 0xfe);

  for (int ind = 18; ind < 26; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }
  BOOST_CHECK_EQUAL(result_data[26], 0xfe);
}

BOOST_AUTO_TEST_CASE(edge_negative)
{
  CassDuration value(1LL << 63, 1LL << 63, 1LL << 63);
  Buffer result = encode(value);
  BOOST_CHECK_EQUAL(27, result.size());
  unsigned const char* result_data = reinterpret_cast<unsigned const char*>(result.data());

  // We have 9-bytes for 1LL << 63, the min 64-bit number. The zigzag
  // representation is 8 bytes of 0xff, and the first byte is 0xff to say we have
  // 8 bytes of value beyond these size-spec-bits.
  //
  // The same is true for "days" and "nanos".
  for (int ind = 0; ind < 27; ++ind) {
    BOOST_CHECK_EQUAL(result_data[ind], 0xff);
  }
}

BOOST_AUTO_TEST_SUITE_END()
