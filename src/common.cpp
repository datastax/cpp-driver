/*
  Copyright (c) 2014 DataStax

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

#include "common.hpp"

#include "buffer_piece.hpp"
#include "constants.hpp"

#include <rapidjson/document.h>

#include <algorithm>
#include <assert.h>

namespace cass {

uv_buf_t alloc_buffer(size_t suggested_size) {
  return uv_buf_init(new char[suggested_size], suggested_size);
}

uv_buf_t alloc_buffer(uv_handle_t* handle, size_t suggested_size) {
  (void)handle;
  return alloc_buffer(suggested_size);
}

void free_buffer(uv_buf_t buf) {
  delete[] buf.base;
}

std::string opcode_to_string(int opcode) {
  switch (opcode) {
    case CQL_OPCODE_ERROR:
      return "CQL_OPCODE_ERROR";
    case CQL_OPCODE_STARTUP:
      return "CQL_OPCODE_STARTUP";
    case CQL_OPCODE_READY:
      return "CQL_OPCODE_READY";
    case CQL_OPCODE_AUTHENTICATE:
      return "CQL_OPCODE_AUTHENTICATE";
    case CQL_OPCODE_CREDENTIALS:
      return "CQL_OPCODE_CREDENTIALS";
    case CQL_OPCODE_OPTIONS:
      return "CQL_OPCODE_OPTIONS";
    case CQL_OPCODE_SUPPORTED:
      return "CQL_OPCODE_SUPPORTED";
    case CQL_OPCODE_QUERY:
      return "CQL_OPCODE_QUERY";
    case CQL_OPCODE_RESULT:
      return "CQL_OPCODE_RESULT";
    case CQL_OPCODE_PREPARE:
      return "CQL_OPCODE_PREPARE";
    case CQL_OPCODE_EXECUTE:
      return "CQL_OPCODE_EXECUTE";
    case CQL_OPCODE_REGISTER:
      return "CQL_OPCODE_REGISTER";
    case CQL_OPCODE_EVENT:
      return "CQL_OPCODE_EVENT";
    case CQL_OPCODE_BATCH:
      return "CQL_OPCODE_BATCH";
    case CQL_OPCODE_AUTH_CHALLENGE:
      return "CQL_OPCODE_AUTH_CHALLENGE";
    case CQL_OPCODE_AUTH_RESPONSE:
      return "CQL_OPCODE_AUTH_RESPONSE";
    case CQL_OPCODE_AUTH_SUCCESS:
      return "CQL_OPCODE_AUTH_SUCCESS";
  };
  assert(false);
  return "";
}

std::string& trim(std::string& str) {
  // Trim front
  str.erase(str.begin(),
            std::find_if(str.begin(), str.end(),
                         std::not1(std::ptr_fun<int, int>(::isspace))));
  // Trim back
  str.erase(std::find_if(str.rbegin(), str.rend(),
                         std::not1(std::ptr_fun<int, int>(::isspace))).base(),
            str.end());
  return str;
}

void json_to_list(const BufferPiece& alias_json, std::list<std::string>* output) {
  char* buffer = new char[alias_json.size() + 1];
  memcpy(buffer, alias_json.data(), alias_json.size());
  buffer[alias_json.size()] = '\0';

  rapidjson::Document d;
  d.ParseInsitu(buffer);
  if (!d.HasParseError()) {
    for (rapidjson::Value::ConstValueIterator i = d.Begin(); i != d.End(); ++i) {
      output->push_back(i->GetString());
    }
  } else {
    //TODO: log to global logging
  }
  delete[] buffer;
}

void json_to_map(const BufferPiece& json_buffer, std::map<std::string, std::string>* output) {
  char* buffer = new char[json_buffer.size() + 1];
  memcpy(buffer, json_buffer.data(), json_buffer.size());
  buffer[json_buffer.size()] = '\0';

  rapidjson::Document d;
  d.ParseInsitu(buffer);
  if (!d.HasParseError()) {
    for (rapidjson::Value::ConstMemberIterator i = d.MemberBegin(); i != d.MemberEnd(); ++i) {
      (*output)[i->name.GetString()] = i->value.GetString();
    }
  } else {
    //TODO: log to global logging
  }
  delete[] buffer;
}

} // namespace cass
