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

#ifndef DATASTAX_INTERNAL_SERIALIZATION_HPP
#define DATASTAX_INTERNAL_SERIALIZATION_HPP

#include "address.hpp"
#include "cassandra.h"
#include "map.hpp"
#include "string.hpp"
#include "string_ref.hpp"
#include "utils.hpp"

#include <uv.h>

#include <assert.h>
#include <limits>
#include <string.h>

namespace datastax { namespace internal {

// http://commandcenter.blogspot.com/2012/04/byte-order-fallacy.html
// This frees us from having to deal with endian stuff on every platform.
// If it's not fast enough then we can go back to using the bytes swaps.

// TODO(mpenick): Use output pointers instead of references and make input const
// "decode_byte(input, &value)" is clearer than "decode_byte(input, value)"

inline char* encode_byte(char* output, uint8_t value) {
  output[0] = static_cast<char>(value);
  return output + sizeof(uint8_t);
}

inline const char* decode_byte(const char* input, uint8_t& output) {
  output = static_cast<uint8_t>(input[0]);
  return input + sizeof(uint8_t);
}

inline char* encode_int8(char* output, int8_t value) {
  output[0] = static_cast<char>(value);
  return output + sizeof(int8_t);
}

inline const char* decode_int8(const char* input, int8_t& output) {
  output = static_cast<int8_t>(input[0]);
  return input + sizeof(int8_t);
}

inline char* encode_uint16(char* output, uint16_t value) {
  output[0] = static_cast<char>(value >> 8);
  output[1] = static_cast<char>(value >> 0);
  return output + sizeof(uint16_t);
}

inline const char* decode_uint16(const char* input, uint16_t& output) {
  output = static_cast<uint16_t>((static_cast<uint16_t>(static_cast<uint8_t>(input[1])) << 0) |
                                 (static_cast<uint16_t>(static_cast<uint8_t>(input[0])) << 8));
  return input + sizeof(uint16_t);
}

inline char* encode_int16(char* output, int16_t value) {
  output[0] = static_cast<char>(value >> 8);
  output[1] = static_cast<char>(value >> 0);
  return output + sizeof(int16_t);
}

inline const char* decode_int16(const char* input, int16_t& output) {
  output = static_cast<int16_t>((static_cast<int16_t>(static_cast<uint8_t>(input[1])) << 0) |
                                (static_cast<int16_t>(static_cast<uint8_t>(input[0])) << 8));
  return input + sizeof(int16_t);
}

inline char* encode_uint32(char* output, uint32_t value) {
  output[0] = static_cast<char>(value >> 24);
  output[1] = static_cast<char>(value >> 16);
  output[2] = static_cast<char>(value >> 8);
  output[3] = static_cast<char>(value >> 0);
  return output + sizeof(uint32_t);
}

inline const char* decode_uint32(const char* input, uint32_t& output) {
  output = (static_cast<uint32_t>(static_cast<uint8_t>(input[3])) << 0) |
           (static_cast<uint32_t>(static_cast<uint8_t>(input[2])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(input[1])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(input[0])) << 24);
  return input + sizeof(uint32_t);
}

inline char* encode_int32(char* output, int32_t value) {
  output[0] = static_cast<char>(value >> 24);
  output[1] = static_cast<char>(value >> 16);
  output[2] = static_cast<char>(value >> 8);
  output[3] = static_cast<char>(value >> 0);
  return output + sizeof(int32_t);
}

inline const char* decode_int32(const char* input, int32_t& output) {
  output = (static_cast<int32_t>(static_cast<uint8_t>(input[3])) << 0) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[2])) << 8) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[1])) << 16) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[0])) << 24);
  return input + sizeof(int32_t);
}

inline char* encode_int64(char* output, int64_t value) {
  STATIC_ASSERT(sizeof(int64_t) == 8);
  output[0] = static_cast<char>(value >> 56);
  output[1] = static_cast<char>(value >> 48);
  output[2] = static_cast<char>(value >> 40);
  output[3] = static_cast<char>(value >> 32);
  output[4] = static_cast<char>(value >> 24);
  output[5] = static_cast<char>(value >> 16);
  output[6] = static_cast<char>(value >> 8);
  output[7] = static_cast<char>(value >> 0);
  return output + sizeof(int64_t);
}

inline const char* decode_int64(const char* input, int64_t& output) {
  STATIC_ASSERT(sizeof(int64_t) == 8);
  output = (static_cast<int64_t>(static_cast<uint8_t>(input[7])) << 0) |
           (static_cast<int64_t>(static_cast<uint8_t>(input[6])) << 8) |
           (static_cast<int64_t>(static_cast<uint8_t>(input[5])) << 16) |
           (static_cast<int64_t>(static_cast<uint8_t>(input[4])) << 24) |
           (static_cast<int64_t>(static_cast<uint8_t>(input[3])) << 32) |
           (static_cast<int64_t>(static_cast<uint8_t>(input[2])) << 40) |
           (static_cast<int64_t>(static_cast<uint8_t>(input[1])) << 48) |
           (static_cast<int64_t>(static_cast<uint8_t>(input[0])) << 56);
  return input + sizeof(int64_t);
}

inline char* encode_float(char* output, float value) {
  STATIC_ASSERT(std::numeric_limits<float>::is_iec559);
  return encode_int32(output, copy_cast<float, int32_t>(value));
}

inline const char* decode_float(const char* input, float& output) {
  STATIC_ASSERT(std::numeric_limits<float>::is_iec559);
  int32_t int_value;
  const char* pos = decode_int32(input, int_value);
  output = copy_cast<int32_t, float>(int_value);
  return pos;
}

inline char* encode_double(char* output, double value) {
  STATIC_ASSERT(std::numeric_limits<double>::is_iec559);
  return encode_int64(output, copy_cast<double, int64_t>(value));
}

inline const char* decode_double(const char* input, double& output) {
  STATIC_ASSERT(std::numeric_limits<double>::is_iec559);
  int64_t int_value;
  const char* pos = decode_int64(input, int_value);
  output = copy_cast<int64_t, double>(int_value);
  return pos;
}

inline char* encode_uuid(char* output, CassUuid uuid) {
  uint64_t time_and_version = uuid.time_and_version;
  output[3] = static_cast<char>(time_and_version & 0x00000000000000FFLL);
  time_and_version >>= 8;
  output[2] = static_cast<char>(time_and_version & 0x00000000000000FFLL);
  time_and_version >>= 8;
  output[1] = static_cast<char>(time_and_version & 0x00000000000000FFLL);
  time_and_version >>= 8;
  output[0] = static_cast<char>(time_and_version & 0x00000000000000FFLL);
  time_and_version >>= 8;

  output[5] = static_cast<char>(time_and_version & 0x00000000000000FFLL);
  time_and_version >>= 8;
  output[4] = static_cast<char>(time_and_version & 0x00000000000000FFLL);
  time_and_version >>= 8;

  output[7] = static_cast<char>(time_and_version & 0x00000000000000FFLL);
  time_and_version >>= 8;
  output[6] = static_cast<char>(time_and_version & 0x000000000000000FFLL);

  uint64_t clock_seq_and_node = uuid.clock_seq_and_node;
  for (size_t i = 0; i < 8; ++i) {
    output[15 - i] = static_cast<char>(clock_seq_and_node & 0x00000000000000FFL);
    clock_seq_and_node >>= 8;
  }
  // UUID is 128-bit, which is 16 bytes.
  return output + 16;
}

inline const char* decode_uuid(const char* input, CassUuid* output) {
  output->time_and_version = static_cast<uint64_t>(static_cast<uint8_t>(input[3]));
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[2])) << 8;
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[1])) << 16;
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[0])) << 24;

  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[5])) << 32;
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[4])) << 40;

  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[7])) << 48;
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[6])) << 56;

  output->clock_seq_and_node = 0;
  for (size_t i = 0; i < 8; ++i) {
    output->clock_seq_and_node |= static_cast<uint64_t>(static_cast<uint8_t>(input[15 - i]))
                                  << (8 * i);
  }
  return input + 16;
}

inline int64_t decode_zig_zag(uint64_t n) {
  // n is an unsigned long because we want a logical shift right
  // (it should 0-fill high order bits), not arithmetic shift right.
  return (n >> 1) ^ -static_cast<int64_t>(n & 1);
}

inline uint64_t encode_zig_zag(int64_t n) { return (n << 1) ^ (n >> 63); }

}} // namespace datastax::internal

#endif
