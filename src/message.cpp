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

#include "message.hpp"
#include "options_request.hpp"
#include "startup_request.hpp"
#include "batch_request.hpp"
#include "query_request.hpp"
#include "prepare_request.hpp"
#include "execute_request.hpp"

#include "error_response.hpp"
#include "ready_response.hpp"
#include "supported_response.hpp"
#include "result_response.hpp"

namespace cass {

bool Message::allocate_body(int8_t opcode) {
  request_body_.reset();
  response_body_.reset();
  switch (opcode) {
    case CQL_OPCODE_RESULT:
      response_body_.reset(new ResultResponse());
      return true;

    case CQL_OPCODE_PREPARE:
      request_body_.reset(new PrepareRequest());
      return true;

    case CQL_OPCODE_ERROR:
      response_body_.reset(new ErrorResponse());
      return true;

    case CQL_OPCODE_OPTIONS:
      request_body_.reset(new OptionsRequest());
      return true;

    case CQL_OPCODE_STARTUP:
      request_body_.reset(new StartupRequest());
      return true;

    case CQL_OPCODE_SUPPORTED:
      response_body_.reset(new SupportedResponse());
      return true;

    case CQL_OPCODE_QUERY:
      request_body_.reset(new QueryRequest());
      return true;

    case CQL_OPCODE_READY:
      response_body_.reset(new ReadyResponse());
      return true;

    default:
      return false;
  }
}

ssize_t Message::decode(char* input, size_t size) {
  char* input_pos = input;

  received_ += size;

  if (!header_received_) {
    if (received_ >= CASS_HEADER_SIZE) {
      // We may have received more data then we need, only copy what we need
      size_t overage = received_ - CASS_HEADER_SIZE;
      size_t needed = size - overage;

      memcpy(header_buffer_pos_, input_pos, needed);
      header_buffer_pos_ += needed;
      input_pos += needed;
      assert(header_buffer_pos_ == header_buffer_ + CASS_HEADER_SIZE);

      char* buffer = header_buffer_;
      version_ = *(buffer++);
      flags_ = *(buffer++);
      stream_ = *(buffer++);
      opcode_ = *(buffer++);

      decode_int32(buffer, length_);

      header_received_ = true;

      if (!allocate_body(opcode_) || !response_body_) {
        return -1;
      }

      response_body_->set_buffer(new char[length_]);
      body_buffer_pos_ = response_body_->buffer();
    } else {
      // We haven't received all the data for the header. We consume the
      // entire buffer.
      memcpy(header_buffer_pos_, input_pos, size);
      header_buffer_pos_ += size;
      return size;
    }
  }

  const size_t remaining = size - (input_pos - input);
  const size_t frame_size = CASS_HEADER_SIZE + length_;

  if (received_ >= frame_size) {
    // We may have received more data then we need, only copy what we need
    size_t overage = received_ - frame_size;
    size_t needed = remaining - overage;

    memcpy(body_buffer_pos_, input_pos, needed);
    body_buffer_pos_ += needed;
    input_pos += needed;
    assert(body_buffer_pos_ == response_body_->buffer() + length_);

    if (!response_body_->decode(response_body_->buffer(), length_)) {
      body_error_ = true;
    }
    body_ready_ = true;
  } else {
    // We haven't received all the data for the frame. We consume the entire
    // buffer.
    memcpy(body_buffer_pos_, input_pos, remaining);
    body_buffer_pos_ += remaining;
    return size;
  }

  return input_pos - input;
}

} // namespace cass
