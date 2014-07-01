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

#ifndef __CASS_RESPONSE_HPP_INCLUDED__
#define __CASS_RESPONSE_HPP_INCLUDED__

#include "macros.hpp"
#include "scoped_ptr.hpp"

#include "third_party/boost/boost/cstdint.hpp"

namespace cass {

class Response {
public:
  Response(uint8_t opcode)
      : opcode_(opcode) {}

  virtual ~Response() {}

  uint8_t opcode() const { return opcode_; }
  char* buffer() { return buffer_.get(); }
  void set_buffer(char* buffer) { buffer_.reset(buffer); }

  virtual bool decode(char* buffer, size_t size) = 0;

private:
  uint8_t opcode_;
  ScopedPtr<char[]> buffer_;

private:
  DISALLOW_COPY_AND_ASSIGN(Response);
};

class ResponseMessage {
public:
  int32_t decode(char* data, int32_t size);

private:
  ScopedPtr<Response> response_body_;
};

} // namespace cass

#endif
