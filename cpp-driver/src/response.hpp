/*
  Copyright (c) DataStax, Inc.

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

#ifndef DATASTAX_INTERNAL_RESPONSE_HPP
#define DATASTAX_INTERNAL_RESPONSE_HPP

#include "allocated.hpp"
#include "constants.hpp"
#include "decoder.hpp"
#include "hash_table.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"
#include "utils.hpp"

#include <uv.h>

#define CHECK_RESULT(result) \
  if (!(result)) return false;

namespace datastax { namespace internal { namespace core {

class Response : public RefCounted<Response> {
public:
  typedef SharedRefPtr<Response> Ptr;

  Response(uint8_t opcode);

  virtual ~Response() {}

  uint8_t opcode() const { return opcode_; }

  char* data() const { return buffer_->data(); }

  const RefBuffer::Ptr& buffer() const { return buffer_; }

  void set_buffer(size_t size) { buffer_ = RefBuffer::Ptr(RefBuffer::create(size)); }

  bool has_tracing_id() const;

  const CassUuid& tracing_id() const { return tracing_id_; }

  const CustomPayloadVec& custom_payload() const { return custom_payload_; }

  const WarningVec& warnings() const { return warnings_; }

  bool decode_trace_id(Decoder& decoder);

  bool decode_custom_payload(Decoder& decoder);

  bool decode_warnings(Decoder& decoder);

  virtual bool decode(Decoder& decoder) = 0;

private:
  uint8_t opcode_;
  RefBuffer::Ptr buffer_;
  CassUuid tracing_id_;
  CustomPayloadVec custom_payload_;
  WarningVec warnings_;

private:
  DISALLOW_COPY_AND_ASSIGN(Response);
};

class ResponseMessage : public Allocated {
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

  uint8_t flags() const { return flags_; }

  uint8_t opcode() const { return opcode_; }

  int16_t stream() const { return stream_; }

  const Response::Ptr& response_body() { return response_body_; }

  bool is_body_ready() const { return is_body_ready_; }

  ssize_t decode(const char* input, size_t size);

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

}}} // namespace datastax::internal::core

#endif
