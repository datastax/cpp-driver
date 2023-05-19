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
#include "serialization.hpp"

using namespace datastax::internal;

TEST(SerializationTest, DecodeZigZag) { ASSERT_EQ(1LL << 63, decode_zig_zag((long)-1)); }

TEST(SerializationTest, DecodeByte) {
  const signed char input[2] = { -1, 0 };
  uint8_t value = 0;

  const char* pos = decode_byte((const char*)&input[0], value);
  ASSERT_EQ((const char*)&input[1], pos);
  ASSERT_EQ(std::numeric_limits<uint8_t>::max(), value);
  pos = decode_byte(pos, value);
  ASSERT_EQ(std::numeric_limits<uint8_t>::min(), value);
}

TEST(SerializationTest, DecodeInt8) {
  const signed char input[2] = { -128, 127 };
  int8_t value = 0;

  const char* pos = decode_int8((const char*)&input[0], value);
  ASSERT_EQ((const char*)&input[1], pos);
  ASSERT_EQ(std::numeric_limits<int8_t>::min(), value);
  pos = decode_int8(pos, value);
  ASSERT_EQ(std::numeric_limits<int8_t>::max(), value);
}

TEST(SerializationTest, DecodeUInt16) {
  const signed char input[4] = { -1, -1, 0, 0 };
  uint16_t value = 0;

  const char* pos = decode_uint16((const char*)&input[0], value);
  ASSERT_EQ((const char*)&input[2], pos);
  ASSERT_EQ(std::numeric_limits<uint16_t>::max(), value);
  pos = decode_uint16(pos, value);
  ASSERT_EQ(std::numeric_limits<uint16_t>::min(), value);
}

TEST(SerializationTest, DecodeInt16) {
  const signed char input[4] = { -128, 0, 127, -1 };
  int16_t value = 0;

  // SUCCESS
  const char* pos = decode_int16((const char*)&input[0], value);
  ASSERT_EQ((const char*)&input[2], pos);
  ASSERT_EQ(std::numeric_limits<int16_t>::min(), value);
  pos = decode_int16(pos, value);
  ASSERT_EQ(std::numeric_limits<int16_t>::max(), value);
}

TEST(SerializationTest, DecodeUInt32) {
  const signed char input[8] = { -1, -1, -1, -1, 0, 0, 0, 0 };
  uint32_t value = 0;

  const char* pos = decode_uint32((const char*)&input[0], value);
  ASSERT_EQ((const char*)&input[4], pos);
  ASSERT_EQ(std::numeric_limits<uint32_t>::max(), value);
  pos = decode_uint32(pos, value);
  ASSERT_EQ(std::numeric_limits<uint32_t>::min(), value);
}

TEST(SerializationTest, DecodeInt32) {
  const signed char input[8] = { -128, 0, 0, 0, 127, -1, -1, -1 };
  int32_t value = 0;

  const char* pos = decode_int32((const char*)&input[0], value);
  ASSERT_EQ((const char*)&input[4], pos);
  ASSERT_EQ(std::numeric_limits<int32_t>::min(), value);
  pos = decode_int32(pos, value);
  ASSERT_EQ(std::numeric_limits<int32_t>::max(), value);
}

TEST(SerializationTest, DecodeInt64) {
  const signed char input[16] = { -128, 0, 0, 0, 0, 0, 0, 0, 127, -1, -1, -1, -1, -1, -1, -1 };
  int64_t value = 0;

  const char* pos = decode_int64((const char*)&input[0], value);
  ASSERT_EQ((const char*)&input[8], pos);
  ASSERT_EQ(std::numeric_limits<int64_t>::min(), value);
  pos = decode_int64(pos, value);
  ASSERT_EQ(std::numeric_limits<int64_t>::max(), value);
}

TEST(SerializationTest, DecodeFloat) {
  const signed char input[8] = { 0, -128, 0, 0, 127, 127, -1, -1 };
  float value = 0;

  const char* pos = decode_float((const char*)&input[0], value);
  ASSERT_EQ((const char*)&input[4], pos);
  ASSERT_EQ(std::numeric_limits<float>::min(), value);
  pos = decode_float(pos, value);
  ASSERT_EQ(std::numeric_limits<float>::max(), value);
}

TEST(SerializationTest, DecodeDouble) {
  const signed char input[16] = { 0, 16, 0, 0, 0, 0, 0, 0, 127, -17, -1, -1, -1, -1, -1, -1 };
  double value = 0;

  const char* pos = decode_double((const char*)&input[0], value);
  ASSERT_EQ((const char*)&input[8], pos);
  ASSERT_EQ(std::numeric_limits<double>::min(), value);
  pos = decode_double(pos, value);
  ASSERT_EQ(std::numeric_limits<double>::max(), value);
}

TEST(SerializationTest, DecodeUuid) {
  const signed char input[32] = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0 };
  CassUuid value;

  const char* pos = decode_uuid((const char*)&input[0], &value);
  ASSERT_EQ((const char*)&input[16], pos);
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), value.clock_seq_and_node);
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), value.time_and_version);
  pos = decode_uuid(pos, &value);
  ASSERT_EQ(std::numeric_limits<uint64_t>::min(), value.clock_seq_and_node);
  ASSERT_EQ(std::numeric_limits<uint64_t>::min(), value.time_and_version);
}
