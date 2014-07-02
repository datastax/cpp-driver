/*
  Copyright 2014 DataStax

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

#include "cassandra.h"

#include "third_party/boost/boost/limits.hpp"
#include "third_party/boost/boost/cstdint.hpp"

#include <assert.h>
#include <string.h>
#include <list>
#include <map>
#include <string>

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
  output[0] = value >> 8;
  output[1] = value >> 0;
}

inline char* decode_uint16(char* input, uint16_t& output) {
  output = (static_cast<uint16_t>(static_cast<uint8_t>(input[1])) << 0) |
           (static_cast<uint16_t>(static_cast<uint8_t>(input[0])) << 8);
  return input + sizeof(uint16_t);
}

inline void encode_int32(char* output, int32_t value) {
  output[0] = value >> 24;
  output[1] = value >> 16;
  output[2] = value >> 8;
  output[3] = value >> 0;
}

inline char* decode_int32(char* input, int32_t& output) {
  output = (static_cast<int32_t>(static_cast<uint8_t>(input[3])) << 0) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[2])) << 8) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[1])) << 16) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[0])) << 24);
  return input + sizeof(int32_t);
}

inline void encode_int64(char* output, int64_t value) {
  output[0] = value >> 56;
  output[1] = value >> 48;
  output[2] = value >> 40;
  output[3] = value >> 32;
  output[4] = value >> 24;
  output[5] = value >> 16;
  output[6] = value >> 8;
  output[7] = value >> 0;
}

inline char* decode_int64(char* input, int64_t& output) {
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

inline void encode_float(char* output, float value) {
  assert(std::numeric_limits<float>::is_iec559);
  assert(sizeof(float) == sizeof(int32_t));
  encode_int32(output, *reinterpret_cast<int32_t*>(&value));
}

inline char* decode_float(char* input, float& output) {
  assert(std::numeric_limits<float>::is_iec559);
  assert(sizeof(float) == sizeof(int32_t));
  return decode_int32(input, *reinterpret_cast<int32_t*>(&output));
}

inline void encode_double(char* output, double value) {
  assert(std::numeric_limits<double>::is_iec559);
  assert(sizeof(double) == sizeof(int64_t));
  encode_int64(output, *reinterpret_cast<int64_t*>(&value));
}

inline char* decode_double(char* input, double& output) {
  assert(std::numeric_limits<double>::is_iec559);
  assert(sizeof(double) == sizeof(int64_t));
  return decode_int64(input, *reinterpret_cast<int64_t*>(&output));
}

inline char* decode_string(char* input, char** output, size_t& size) {
  *output = decode_uint16(input, ((uint16_t&)size));
  return *output + size;
}

inline char* decode_long_string(char* input, char** output, size_t& size) {
  *output = decode_int32(input, ((int32_t&)size));
  return *output + size;
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

} // namespace cass

#endif
