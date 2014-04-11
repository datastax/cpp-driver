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

#ifndef __PREPARE_HPP_INCLUDED__
#define __PREPARE_HPP_INCLUDED__

#include <string>
#include "cql_body.hpp"

namespace cql {

class BodyPrepare
    : public cql::Body {
 private:
  std::string _prepare;

 public:
  BodyPrepare()
  {}

  uint8_t
  opcode() {
    return CQL_OPCODE_PREPARE;
  }

  void
  prepare_string(
      const char* input,
      size_t      size) {
    _prepare.assign(input, size);
  }

  void
  prepare_string(
      const std::string& input) {
    _prepare = input;
  }

  bool
  consume(
      char*  buffer,
      size_t size) {
    (void) buffer;
    (void) size;
    return true;
  }

  bool
  prepare(
      size_t reserved,
      char** output,
      size_t& size) {
    size    = reserved + sizeof(int32_t) + _prepare.size();
    *output = new char[size];
    encode_long_string(
        *output + reserved,
        _prepare.c_str(),
        _prepare.size());
    return true;
  }

 private:
  BodyPrepare(const BodyPrepare&) {}
  void operator=(const BodyPrepare&) {}
};
}
#endif
