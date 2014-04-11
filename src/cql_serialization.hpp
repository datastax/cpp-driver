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

#ifndef __SERIALIZATION_HPP_INCLUDED__
#define __SERIALIZATION_HPP_INCLUDED__

#include <list>
#include <map>
#include <string>

namespace cql {

inline char*
encode_byte(
    char*   output,
    uint8_t value) {
  *output = value;
  return output + 1;
}

inline char*
decode_short(
    char*    input,
    int16_t& output) {
  output = ntohs(*(reinterpret_cast<int16_t*>(input)));
  return input + sizeof(int16_t);
}

inline char*
encode_short(
    char*   output,
    int16_t value) {
  int16_t net_value = htons(value);
  memcpy(output, &net_value, sizeof(net_value));
  return output + sizeof(int16_t);
}

inline char*
decode_int(
    char*    input,
    int32_t& output) {
  output = ntohl(*(reinterpret_cast<const int32_t*>(input)));
  return input + sizeof(int32_t);
}

inline char*
encode_int(
    char*   output,
    int32_t value) {
  int32_t net_value = htonl(value);
  memcpy(output, &net_value, sizeof(net_value));
  return output + sizeof(int32_t);
}

inline char*
decode_string(
    char*   input,
    char**  output,
    size_t& size) {
  *output = decode_short(input, ((int16_t&) size));
  return *output + size;
}

inline char*
encode_string(
    char*       output,
    const char* input,
    size_t      size) {
  char* buffer = encode_short(output, size);
  memcpy(buffer, input, size);
  return buffer + size;
}

inline char*
decode_long_string(
    char*   input,
    char**  output,
    size_t& size) {
  *output = decode_int(input, ((int32_t&) size));
  return *output + size;
}

inline char*
encode_long_string(
    char*       output,
    const char* input,
    size_t      size) {
  char* buffer = encode_int(output, size);
  memcpy(buffer, input, size);
  return buffer + size;
}

inline char*
encode_string_map(
    char*                                     output,
    const std::map<std::string, std::string>& map) {

  char* buffer = encode_short(output, map.size());
  for (std::map<std::string, std::string>::const_iterator it = map.begin();
       it != map.end();
       ++it) {
    buffer = encode_string(buffer, it->first.c_str(), it->first.size());
    buffer = encode_string(buffer, it->second.c_str(), it->second.size());
  }
  return buffer;
}

inline char*
decode_string_map(
    char*                               input,
    std::map<std::string, std::string>& map) {

  map.clear();
  int16_t len    = 0;
  char*   buffer = decode_short(input, len);

  for (int i = 0; i < len; i++) {
    char*  key        = 0;
    size_t key_size   = 0;
    char*  value      = 0;
    size_t value_size = 0;

    buffer = decode_string(buffer, &key, key_size);
    buffer = decode_string(buffer, &value, value_size);
    map.insert(
        std::make_pair(
            std::string(key, key_size),
            std::string(value, value_size)));
  }
  return buffer;
}

inline char*
decode_stringlist(
    char*                   input,
    std::list<std::string>& output) {
  output.clear();
  int16_t len    = 0;
  char*   buffer = decode_short(input, len);

  for (int i = 0; i < len; i++) {
    char*  s      = NULL;
    size_t s_size = 0;

    buffer = decode_string(buffer, &s, s_size);
    output.push_back(std::string(s, s_size));
  }
  return buffer;
}

typedef std::map<std::string, std::list<std::string> > string_multimap_t;

inline char*
decode_string_multimap(
    char*              input,
    string_multimap_t& output) {

  output.clear();
  int16_t len    = 0;
  char*   buffer = decode_short(input, len);

  for (int i = 0; i < len; i++) {
    char*                  key        = 0;
    size_t                 key_size   = 0;
    std::list<std::string> value;

    buffer = decode_string(buffer, &key, key_size);
    buffer = decode_stringlist(buffer, value);
    output.insert(std::make_pair(std::string(key, key_size), value));
  }
  return buffer;
}

inline char*
decode_option(
    char*    input,
    int16_t& type,
    char**   class_name,
    size_t&  class_name_size) {
  char* buffer = decode_short(input, type);
  if (type == CQL_COLUMN_TYPE_CUSTOM) {
    buffer = decode_string(buffer, class_name, class_name_size);
  }
  return buffer;
}
}
#endif
