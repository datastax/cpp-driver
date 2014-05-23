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

#ifndef __CASS_ERROR_RESPONSE_HPP_INCLUDED__
#define __CASS_ERROR_RESPONSE_HPP_INCLUDED__

#include "message_body.hpp"
#include "serialization.hpp"

namespace cass {

struct ErrorResponse : public MessageBody {
  std::unique_ptr<char> guard;
  int32_t               code;
  char*                 message;
  size_t                message_size;

  ErrorResponse()
     : MessageBody(CQL_OPCODE_ERROR)
     , code(0xFFFFFFFF)
     , message(NULL)
     , message_size(0) {}

  ErrorResponse(int32_t code, const char* input, size_t input_size)
    : MessageBody(CQL_OPCODE_ERROR)
    , guard(new char[input_size])
    , code(code)
    , message(guard.get())
    , message_size(input_size) {
    memcpy(message, input, input_size);
  }

  bool
  consume(
      char*  buffer,
      size_t size) {
    (void) size;
    buffer = decode_int(buffer, code);
    decode_string(buffer, &message, message_size);
    return true;
  }

  bool
  prepare(
      size_t  reserved,
      char**  output,
      size_t& size) {
    size = reserved + sizeof(int32_t) + sizeof(int16_t) + message_size;
    *output = new char[size];

    char* buffer = *output + reserved;
    buffer       = encode_int(buffer, code);
    encode_string(buffer, message, message_size);
    return true;
  }
};

} // namespace cass

#endif
