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

#include "constants.hpp"
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

  virtual bool decode(int version, char* buffer, size_t size) = 0;

private:
  uint8_t opcode_;
  ScopedPtr<char[]> buffer_;

private:
  DISALLOW_COPY_AND_ASSIGN(Response);
};

class ResponseMessage {
public:
  ResponseMessage()
      : version_(0x02)
      , flags_(0)
      , stream_(0)
      , opcode_(0)
      , length_(0)
      , received_(0)
      , is_header_received_(false)
      , header_buffer_pos_(header_buffer_)
      , is_body_ready_(false)
      , is_body_error_(false)
      , body_buffer_pos_(NULL) {}

  uint8_t opcode() const { return opcode_; }

  int8_t stream() const { return stream_; }

  ScopedPtr<Response>& response_body() { return response_body_; }

  bool is_body_ready() const { return is_body_ready_; }

  int decode(int version, char* input, size_t size);

private:
  bool allocate_body(int8_t opcode);

private:
  uint8_t version_;
  int8_t flags_;
  int8_t stream_;
  uint8_t opcode_;
  int32_t length_;
  size_t received_;

  bool is_header_received_;
  char header_buffer_[CASS_HEADER_SIZE_V1_AND_V2];
  char* header_buffer_pos_;

  bool is_body_ready_;
  bool is_body_error_;
  ScopedPtr<Response> response_body_;
  char* body_buffer_pos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ResponseMessage);
};

} // namespace cass

#endif
