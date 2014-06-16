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

#include <list>
#include <map>
#include <string>

namespace cass {

// http://commandcenter.blogspot.com/2012/04/byte-order-fallacy.html
// This frees us from having to deal with endian stuff on every platform.
// If it's not fast enough then we can go back to using the bytes swaps.

// TODO(mpenick): Change all these char* to uint8_t* and use output pointers
// instead of references
// "decode_byte(input, &value)" is clearer than "decode_byte(input, value)"

inline char* encode_byte(char* output, uint8_t value) {
  output[0] = static_cast<char>(value);
  return output + sizeof(uint8_t);
}

inline char* decode_byte(char* input, uint8_t& output) {
  output = static_cast<uint8_t>(input[0]);
  return input + sizeof(uint8_t);
}

inline char* encode_short(char* output, uint16_t value) {
  output[0] = value >> 8;
  output[1] = value >> 0;
  return output + sizeof(uint16_t);
}

inline char* decode_short(char* input, uint16_t& output) {
  output = (static_cast<uint16_t>(static_cast<uint8_t>(input[1])) << 0) |
           (static_cast<uint16_t>(static_cast<uint8_t>(input[0])) << 8);
  return input + sizeof(uint16_t);
}

inline char* encode_int(char* output, int32_t value) {
  output[0] = value >> 24;
  output[1] = value >> 16;
  output[2] = value >> 8;
  output[3] = value >> 0;
  return output + sizeof(int32_t);
}

inline char* decode_int(char* input, int32_t& output) {
  output = (static_cast<int32_t>(static_cast<uint8_t>(input[3])) << 0) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[2])) << 8) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[1])) << 16) |
           (static_cast<int32_t>(static_cast<uint8_t>(input[0])) << 24);
  return input + sizeof(int32_t);
}

inline char* encode_int64(char* output, int64_t value) {
  output[0] = value >> 56;
  output[1] = value >> 48;
  output[2] = value >> 40;
  output[3] = value >> 32;
  output[4] = value >> 24;
  output[5] = value >> 16;
  output[6] = value >> 8;
  output[7] = value >> 0;
  return output + sizeof(int64_t);
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

inline char* encode_float(char* output, float value) {
  assert(std::numeric_limits<float>::is_iec559);
  assert(sizeof(float) == sizeof(int32_t));
  return encode_int(output, *reinterpret_cast<int32_t*>(&value));
}

inline char* decode_float(char* input, float& output) {
  assert(std::numeric_limits<float>::is_iec559);
  assert(sizeof(float) == sizeof(int32_t));
  return decode_int(input, *reinterpret_cast<int32_t*>(&output));
}

inline char* encode_double(char* output, double value) {
  assert(std::numeric_limits<double>::is_iec559);
  assert(sizeof(double) == sizeof(int64_t));
  return encode_int64(output, *reinterpret_cast<int64_t*>(&value));
}

inline char* decode_double(char* input, double& output) {
  assert(std::numeric_limits<double>::is_iec559);
  assert(sizeof(double) == sizeof(int64_t));
  return decode_int64(input, *reinterpret_cast<int64_t*>(&output));
}

inline char* encode_string(char* output, const char* input, size_t size) {
  char* buffer = encode_short(output, size);
  memcpy(buffer, input, size);
  return buffer + size;
}

inline char* decode_string(char* input, char** output, size_t& size) {
  *output = decode_short(input, ((uint16_t&)size));
  return *output + size;
}

inline char* encode_decimal(char* output, int32_t scale, const uint8_t* varint,
                            size_t varint_length) {
  char* buffer = encode_int(output, scale);
  memcpy(buffer, varint, varint_length);
  return buffer + varint_length;
}

inline char* encode_inet(char* output, const uint8_t* address,
                         uint8_t address_len) {
  memcpy(output, address, address_len);
  return output + address_len;
}

inline char* decode_long_string(char* input, char** output, size_t& size) {
  *output = decode_int(input, ((int32_t&)size));
  return *output + size;
}

inline char* encode_long_string(char* output, const char* input, size_t size) {
  char* buffer = encode_int(output, size);
  memcpy(buffer, input, size);
  return buffer + size;
}

inline char* encode_bytes(char* output, const char* input, int32_t size) {
  char* buffer = encode_int(output, size);
  if (size > 0) {
    memcpy(buffer, input, size);
    return buffer + size;
  }
  return buffer;
}

inline char* encode_string_map(char* output,
                               const std::map<std::string, std::string>& map) {

  char* buffer = encode_short(output, map.size());
  for (std::map<std::string, std::string>::const_iterator it = map.begin();
       it != map.end(); ++it) {
    buffer = encode_string(buffer, it->first.c_str(), it->first.size());
    buffer = encode_string(buffer, it->second.c_str(), it->second.size());
  }
  return buffer;
}

inline char* encode_uuid(char* output, const uint8_t* uuid) {
  memcpy(output, uuid, 16);
  return output + 16;
}

inline char* decode_string_map(char* input,
                               std::map<std::string, std::string>& map) {

  map.clear();
  uint16_t len = 0;
  char* buffer = decode_short(input, len);

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
  char* buffer = decode_short(input, len);

  for (int i = 0; i < len; i++) {
    char* s = NULL;
    size_t s_size = 0;

    buffer = decode_string(buffer, &s, s_size);
    output.push_back(std::string(s, s_size));
  }
  return buffer;
}

typedef std::map<std::string, std::list<std::string> > string_multimap_t;

inline char* decode_string_multimap(char* input, string_multimap_t& output) {
  output.clear();
  uint16_t len = 0;
  char* buffer = decode_short(input, len);

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
  char* buffer = decode_short(input, type);
  if (type == CASS_VALUE_TYPE_CUSTOM) {
    buffer = decode_string(buffer, class_name, class_name_size);
  }
  return buffer;
}

} // namespace cass

#endif
