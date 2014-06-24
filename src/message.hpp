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

#ifndef __CASS_MESSAGE_HPP_INCLUDED__
#define __CASS_MESSAGE_HPP_INCLUDED__

#include "host.hpp"
#include "request.hpp"
#include "response.hpp"
#include "scoped_ptr.hpp"
#include "ref_counted.h"

#define CASS_HEADER_SIZE 8

namespace cass {

class Message {
public:
  Message()
      : version_(0x02)
      , flags_(0)
      , stream_(0)
      , opcode_(0)
      , length_(0)
      , received_(0)
      , header_received_(false)
      , header_buffer_pos_(header_buffer_)
      , request_body_(NULL)
      , body_buffer_pos_(NULL)
      , body_ready_(false)
      , body_error_(false) {}

  Message(uint8_t opcode)
      : version_(0x02)
      , flags_(0)
      , stream_(0)
      , opcode_(opcode)
      , length_(0)
      , received_(0)
      , header_received_(false)
      , header_buffer_pos_(header_buffer_)
      , request_body_(NULL)
      , body_buffer_pos_(NULL)
      , body_ready_(false)
      , body_error_(false) {
    assert(allocate_body(opcode));
  }

  uint8_t opcode() const { return opcode_; }
  void set_opcode(uint8_t opcode) { opcode_ = opcode; }

  int8_t stream() const { return stream_; }
  void set_stream(uint8_t stream) { stream_ = stream; }

  ScopedRefPtr<Request>& request_body() { return request_body_; }

  ScopedPtr<Response>& response_body() { return response_body_; }

  bool body_ready() const { return body_ready_; }

  bool allocate_body(int8_t opcode_);

  bool prepare(char** output, size_t& size);

  ssize_t consume(char* input, size_t size);

private:
  uint8_t version_;
  int8_t flags_;
  int8_t stream_;
  uint8_t opcode_;
  int32_t length_;
  size_t received_;
  bool header_received_;
  char header_buffer_[CASS_HEADER_SIZE];
  char* header_buffer_pos_;
  ScopedRefPtr<Request> request_body_;
  ScopedPtr<Response> response_body_;
  char* body_buffer_pos_;
  bool body_ready_;
  bool body_error_;
};

} // namespace cass

#endif
