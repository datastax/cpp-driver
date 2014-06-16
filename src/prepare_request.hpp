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

#ifndef __CASS_PREPARE_REQUEST_HPP_INCLUDED__
#define __CASS_PREPARE_REQUEST_HPP_INCLUDED__

#include <string>
#include "message_body.hpp"

namespace cass {

struct PrepareRequest : public MessageBody {
  std::string statement;

  PrepareRequest()
      : MessageBody(CQL_OPCODE_PREPARE) {}

  void prepare_string(const char* input, size_t size) {
    statement.assign(input, size);
  }

  void prepare_string(const std::string& input) { statement = input; }

  bool prepare(size_t reserved, char** output, size_t& size) {
    size = reserved + sizeof(int32_t) + statement.size();
    *output = new char[size];
    encode_long_string(*output + reserved, statement.c_str(), statement.size());
    return true;
  }
};

} // namespace cass

#endif
