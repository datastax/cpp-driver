/*
  Copyright (c) 2014-2016 DataStax

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

#include "utils.hpp"
#include "constants.hpp"
#include "hash_table.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"
#include "scoped_ptr.hpp"

#include <uv.h>

namespace cass {

class Response : public RefCounted<Response> {
public:
  typedef SharedRefPtr<Response> Ptr;

  struct CustomPayloadItem {
    CustomPayloadItem(StringRef name, StringRef value)
      : name(name)
      , value(value) { }
    StringRef name;
    StringRef value;
  };
  typedef SmallVector<CustomPayloadItem, 8> CustomPayloadVec;
  typedef SmallVector<StringRef, 8> WarningVec;

  Response(uint8_t opcode)
      : opcode_(opcode) { }

  virtual ~Response() { }

  uint8_t opcode() const { return opcode_; }

  char* data() const { return buffer_->data(); }

  const RefBuffer::Ptr& buffer() const { return buffer_; }

  void set_buffer(size_t size) {
    buffer_ = RefBuffer::Ptr(RefBuffer::create(size));
  }

  const CustomPayloadVec& custom_payload() const { return custom_payload_; }

  char* decode_custom_payload(char* buffer, size_t size);

  char* decode_warnings(char* buffer, size_t size);

  virtual bool decode(int version, char* buffer, size_t size) = 0;

private:
  uint8_t opcode_;
  RefBuffer::Ptr buffer_;
  CustomPayloadVec custom_payload_;

private:
  DISALLOW_COPY_AND_ASSIGN(Response);
};

class ResponseMessage {
public:
  ResponseMessage()
      : version_(0)
      , flags_(0)
      , stream_(0)
      , opcode_(0)
      , length_(0)
      , received_(0)
      , header_size_(0)
      , is_header_received_(false)
      , header_buffer_pos_(header_buffer_)
      , is_body_ready_(false)
      , is_body_error_(false)
      , body_buffer_pos_(NULL) {}

  uint8_t floats() const { return flags_; }

  uint8_t opcode() const { return opcode_; }

  int16_t stream() const { return stream_; }

  const Response::Ptr& response_body() { return response_body_; }

  bool is_body_ready() const { return is_body_ready_; }

  ssize_t decode(char* input, size_t size);

private:
  bool allocate_body(int8_t opcode);

private:
  uint8_t version_;
  uint8_t flags_;
  int16_t stream_;
  uint8_t opcode_;
  int32_t length_;
  size_t received_;
  size_t header_size_;

  bool is_header_received_;
  char header_buffer_[CASS_HEADER_SIZE_V3];
  char* header_buffer_pos_;

  bool is_body_ready_;
  bool is_body_error_;
  Response::Ptr response_body_;
  char* body_buffer_pos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ResponseMessage);
};

} // namespace cass

#endif
