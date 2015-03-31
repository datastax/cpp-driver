/*
  Copyright (c) 2014-2015 DataStax

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

#ifndef __CASS_SERIALIZATION_HPP_INCLUDED__
#define __CASS_SERIALIZATION_HPP_INCLUDED__

#include "address.hpp"
#include "cassandra.h"
#include "common.hpp"
#include "string_ref.hpp"

#include <uv.h>

#include <assert.h>
#include <limits>
#include <list>
#include <map>
#include <string>
#include <string.h>

namespace cass {

// http://commandcenter.blogspot.com/2012/04/byte-order-fallacy.html
// This frees us from having to deal with endian stuff on every platform.
// If it's not fast enough then we can go back to using the bytes swaps.

// TODO(mpenick): Use output pointers instead of references and make input const
// "decode_byte(input, &value)" is clearer than "decode_byte(input, value)"

inline char* encode_byte(char* output, uint8_t value) {
  output[0] = static_cast<char>(value);
  return output + sizeof(uint8_t);
}

inline char* decode_byte(char* input, uint8_t& output) {
  output = static_cast<uint8_t>(input[0]);
  return input + sizeof(uint8_t);
}

inline void encode_uint16(char* output, uint16_t value) {
  output[0] = static_cast<char>(value >> 8);
  output[1] = static_cast<char>(value >> 0);
}

inline char* decode_uint16(char* input, uint16_t& output) {
  output = (static_cast<uint16_t>(static_cast<uint8_t>(input[1])) << 0) |
           (static_cast<uint16_t>(static_cast<uint8_t>(input[0])) << 8);
  return input + sizeof(uint16_t);
}

inline void encode_int32(char* output, int32_t value) {
  output[0] = static_cast<char>(value >> 24);
  output[1] = static_cast<char>(value >> 16);
  output[2] = static_cast<char>(value >> 8);
  output[3] = static_cast<char>(value >> 0);
}

inline char* decode_int32(char* input, int32_t& output) {
  output = (static_cast<int32_t>(static_cast<uint8_t>(input[3])) << 0) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[2])) << 8) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[1])) << 16) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[0])) << 24);
  return input + sizeof(int32_t);
}

inline void encode_int64(char* output, cass_int64_t value) {
  STATIC_ASSERT(sizeof(cass_int64_t) == 8);
  output[0] = static_cast<char>(value >> 56);
  output[1] = static_cast<char>(value >> 48);
  output[2] = static_cast<char>(value >> 40);
  output[3] = static_cast<char>(value >> 32);
  output[4] = static_cast<char>(value >> 24);
  output[5] = static_cast<char>(value >> 16);
  output[6] = static_cast<char>(value >> 8);
  output[7] = static_cast<char>(value >> 0);
}

inline void encode_uint64(uint8_t* output, uint64_t value) {
  STATIC_ASSERT(sizeof(cass_uint64_t) == 8);
  output[0] = static_cast<uint8_t>(value >> 56);
  output[1] = static_cast<uint8_t>(value >> 48);
  output[2] = static_cast<uint8_t>(value >> 40);
  output[3] = static_cast<uint8_t>(value >> 32);
  output[4] = static_cast<uint8_t>(value >> 24);
  output[5] = static_cast<uint8_t>(value >> 16);
  output[6] = static_cast<uint8_t>(value >> 8);
  output[7] = static_cast<uint8_t>(value >> 0);
}

inline char* decode_int64(char* input, cass_int64_t& output) {
  STATIC_ASSERT(sizeof(cass_int64_t) == 8);
  output = (static_cast<cass_int64_t>(static_cast<uint8_t>(input[7])) << 0) |
           (static_cast<cass_int64_t>(static_cast<uint8_t>(input[6])) << 8) |
           (static_cast<cass_int64_t>(static_cast<uint8_t>(input[5])) << 16) |
           (static_cast<cass_int64_t>(static_cast<uint8_t>(input[4])) << 24) |
           (static_cast<cass_int64_t>(static_cast<uint8_t>(input[3])) << 32) |
           (static_cast<cass_int64_t>(static_cast<uint8_t>(input[2])) << 40) |
           (static_cast<cass_int64_t>(static_cast<uint8_t>(input[1])) << 48) |
           (static_cast<cass_int64_t>(static_cast<uint8_t>(input[0])) << 56);
  return input + sizeof(cass_int64_t);
}

inline void encode_float(char* output, float value) {
  STATIC_ASSERT(std::numeric_limits<float>::is_iec559);
  encode_int32(output, copy_cast<float, int32_t>(value));
}

inline char* decode_float(char* input, float& output) {
  STATIC_ASSERT(std::numeric_limits<float>::is_iec559);
  int32_t int_value;
  char* pos = decode_int32(input, int_value);
  output = copy_cast<int32_t, float>(int_value);
  return pos;
}

inline void encode_double(char* output, double value) {
  STATIC_ASSERT(std::numeric_limits<double>::is_iec559);
  encode_int64(output, copy_cast<double, int64_t>(value));
}

inline char* decode_double(char* input, double& output) {
  STATIC_ASSERT(std::numeric_limits<double>::is_iec559);
  cass_int64_t int_value;
  char* pos = decode_int64(input, int_value);
  output = copy_cast<int64_t, double>(int_value);
  return pos;
}

inline char* decode_string(char* input, char** output, size_t& size) {
  uint16_t string_size;
  char* pos = decode_uint16(input, string_size);
  size = string_size;
  *output = pos;
  return pos + string_size;
}

inline char* decode_string_ref(char* buffer, StringRef* output) {
  char* str;
  size_t str_size;
  buffer = decode_string(buffer, &str, str_size);
  *output = StringRef(str, str_size);
  return buffer;
}

inline char* decode_long_string(char* input, char** output, size_t& size) {
  int32_t string_size;
  char* pos = decode_int32(input, string_size);
  assert(string_size >= 0);
  size = string_size;
  *output = pos;
  return pos + string_size;
}

inline char* decode_bytes(char* input, char** output, size_t& size) {
  int32_t bytes_size;
  char* pos = decode_int32(input, bytes_size);
  if (bytes_size < 0) {
    *output = NULL;
    size = 0;
    return pos;
  } else {
    *output = pos;
    size = bytes_size;
    return pos + size;
  }
}

inline char* decode_inet(char* input, Address* output) {
  uint8_t address_len;
  char address[16];
  int32_t port;

  char* pos = decode_byte(input, address_len);

  assert(address_len <= 16);
  memcpy(address, pos, address_len);
  pos += address_len;

  pos = decode_int32(pos, port);

  Address::from_inet(address, address_len, port, output);

  return pos;
}

inline char* decode_string_map(char* input,
                               std::map<std::string, std::string>& map) {

  map.clear();
  uint16_t len = 0;
  char* buffer = decode_uint16(input, len);

  for (int i = 0; i < len; i++) {
    char* key = 0;
    size_t key_size = 0;
    char* value = 0;
    size_t value_size = 0;

    buffer = decode_string(buffer, &key, key_size);
    buffer = decode_string(buffer, &value, value_size);
    map.insert(std::make_pair(std::string(key, key_size),
                              std::string(value, value_size)));
  }
  return buffer;
}

inline char* decode_stringlist(char* input, std::list<std::string>& output) {
  output.clear();
  uint16_t len = 0;
  char* buffer = decode_uint16(input, len);

  for (int i = 0; i < len; i++) {
    char* s = NULL;
    size_t s_size = 0;

    buffer = decode_string(buffer, &s, s_size);
    output.push_back(std::string(s, s_size));
  }
  return buffer;
}

typedef std::map<std::string, std::list<std::string> > StringMultimap;

inline char* decode_string_multimap(char* input, StringMultimap& output) {
  output.clear();
  uint16_t len = 0;
  char* buffer = decode_uint16(input, len);

  for (int i = 0; i < len; i++) {
    char* key = 0;
    size_t key_size = 0;
    std::list<std::string> value;

    buffer = decode_string(buffer, &key, key_size);
    buffer = decode_stringlist(buffer, value);
    output.insert(std::make_pair(std::string(key, key_size), value));
  }
  return buffer;
}

inline char* decode_option(char* input, uint16_t& type, char** class_name,
                           size_t& class_name_size) {
  char* buffer = decode_uint16(input, type);
  if (type == CASS_VALUE_TYPE_CUSTOM) {
    buffer = decode_string(buffer, class_name, class_name_size);
  }
  return buffer;
}

inline void encode_uuid(char* output, CassUuid uuid) {
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
}

inline char* decode_uuid(char* input, CassUuid* output) {
  output->time_and_version  = static_cast<uint64_t>(static_cast<uint8_t>(input[3]));
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[2])) << 8;
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[1])) << 16;
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[0])) << 24;

  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[5])) << 32;
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[4])) << 40;

  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[7])) << 48;
  output->time_and_version |= static_cast<uint64_t>(static_cast<uint8_t>(input[6])) << 56;

  output->clock_seq_and_node = 0;
  for (size_t i = 0; i < 8; ++i) {
    output->clock_seq_and_node |= static_cast<uint64_t>(static_cast<uint8_t>(input[15 - i])) << (8 * i);
  }
  return input + 16;
}

} // namespace cass

#endif
