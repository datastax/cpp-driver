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

#include "encode.hpp"

#include <time.h>

using namespace datastax::internal::core;

TEST(EncodeDurationUnitTest, Base) {
  CassDuration value(0, 0, 0);
  Buffer result = encode(value);
  EXPECT_EQ(3u, result.size());
  const char* result_data = result.data();
  EXPECT_EQ(result_data[0], 0);
  EXPECT_EQ(result_data[1], 0);
  EXPECT_EQ(result_data[2], 0);
}

TEST(EncodeDurationUnitTest, SimplePositive) {
  CassDuration value(1, 2, 3);
  Buffer result = encode(value);
  EXPECT_EQ(3u, result.size());
  const char* result_data = result.data();
  EXPECT_EQ(result_data[0], 2);
  EXPECT_EQ(result_data[1], 4);
  EXPECT_EQ(result_data[2], 6);
}

TEST(EncodeDurationUnitTest, SimpleNegative) {
  CassDuration value(-1, -2, -3);
  Buffer result = encode(value);
  EXPECT_EQ(3u, result.size());
  const char* result_data = result.data();
  EXPECT_EQ(result_data[0], 1);
  EXPECT_EQ(result_data[1], 3);
  EXPECT_EQ(result_data[2], 5);
}

TEST(EncodeDurationUnitTest, EdgePositive) {
  CassDuration value(std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::max(),
                     std::numeric_limits<int64_t>::max());
  Buffer result = encode(value);
  EXPECT_EQ(19u, result.size());
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

  EXPECT_EQ(result_data[0], 0xf0);
  for (int ind = 1; ind < 4; ++ind) {
    EXPECT_EQ(result_data[ind], 0xff);
  }
  EXPECT_EQ(result_data[4], 0xfe);

  // The same interpretation applies to "days" and "nanos".
  EXPECT_EQ(result_data[5], 0xf0);
  for (int ind = 6; ind < 9; ++ind) {
    EXPECT_EQ(result_data[ind], 0xff);
  }
  EXPECT_EQ(result_data[9], 0xfe);

  EXPECT_EQ(result_data[10], 0xff);
  for (int ind = 11; ind < 18; ++ind) {
    EXPECT_EQ(result_data[ind], 0xff);
  }
  EXPECT_EQ(result_data[18], 0xfe);
}

TEST(EncodeDurationUnitTest, EdgeNegative) {
  CassDuration value(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::min(),
                     std::numeric_limits<int64_t>::min());
  Buffer result = encode(value);
  EXPECT_EQ(19u, result.size());
  unsigned const char* result_data = reinterpret_cast<unsigned const char*>(result.data());

  // We have 5-bytes for 1L << 31, the min 32-bit number. The zigzag
  // representation is 4 bytes of 0xff, and the first byte is 0xf0 to say we have
  // 4 bytes of value beyond these size-spec-bits. The last 9 bytes represent
  // 1L << 63 with all the first byte bits set to indicate 8 more bytes are needed to
  // encode this value.

  EXPECT_EQ(result_data[0], 0xf0);
  for (int ind = 1; ind <= 4; ++ind) {
    EXPECT_EQ(result_data[ind], 0xff);
  }

  // The same is true for "days" and "nanos".
  EXPECT_EQ(result_data[5], 0xf0);
  for (int ind = 6; ind <= 9; ++ind) {
    EXPECT_EQ(result_data[ind], 0xff);
  }

  EXPECT_EQ(result_data[10], 0xff);
  for (int ind = 11; ind <= 18; ++ind) {
    EXPECT_EQ(result_data[ind], 0xff);
  }
}
