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

#ifndef __CASS_ERROR_RESPONSE_HPP_INCLUDED__
#define __CASS_ERROR_RESPONSE_HPP_INCLUDED__

#include "response.hpp"
#include "constants.hpp"
#include "scoped_ptr.hpp"

#include <uv.h>

#include <string.h>
#include <string>

namespace cass {

class ErrorResponse : public Response {
public:
  ErrorResponse()
      : Response(CQL_OPCODE_ERROR)
      , code_(0xFFFFFFFF)
      , message_(NULL)
      , message_size_(0)
      , prepared_id_(NULL)
      , prepared_id_size_(0) {}

  ErrorResponse(int32_t code, const char* input, size_t input_size)
      : Response(CQL_OPCODE_ERROR)
      , guard(new char[input_size])
      , code_(code)
      , message_(guard.get())
      , message_size_(input_size) {
    memcpy(message_, input, input_size);
  }

  int32_t code() const { return code_; }

  std::string prepared_id() const {
    return std::string(prepared_id_, prepared_id_size_);
  }

  std::string message() const { return std::string(message_, message_size_); }

  bool decode(int version, char* buffer, size_t size);

private:
  ScopedPtr<char> guard;
  int32_t code_;
  char* message_;
  size_t message_size_;
  char* prepared_id_;
  size_t prepared_id_size_;
};

std::string error_response_message(const std::string& prefix, ErrorResponse* error);
bool check_error_or_invalid_response(const std::string& prefix, uint8_t expected_opcode,
                                     Response* response);

} // namespace cass

#endif
