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

#include "response.hpp"

#include "auth_responses.hpp"
#include "error_response.hpp"
#include "event_response.hpp"
#include "logger.hpp"
#include "ready_response.hpp"
#include "result_response.hpp"
#include "supported_response.hpp"

#include <cstring>

using namespace datastax::internal::core;

/**
 * A dummy invalid protocol error response that's used to handle responses
 * encoded with deprecated protocol versions.
 */
class InvalidProtocolErrorResponse : public ErrorResponse {
public:
  InvalidProtocolErrorResponse()
      : ErrorResponse(CQL_ERROR_PROTOCOL_ERROR, "Invalid or unsupported protocol version") {}

  virtual bool decode(Decoder& decoder) {
    return true; //  Ignore decoding the body
  }
};

Response::Response(uint8_t opcode)
    : opcode_(opcode) {
  memset(&tracing_id_, 0, sizeof(CassUuid));
}

bool Response::has_tracing_id() const { return tracing_id_.time_and_version != 0; }

bool Response::decode_trace_id(Decoder& decoder) { return decoder.decode_uuid(&tracing_id_); }

bool Response::decode_custom_payload(Decoder& decoder) {
  return decoder.decode_custom_payload(custom_payload_);
}

bool Response::decode_warnings(Decoder& decoder) { return decoder.decode_warnings(warnings_); }

bool ResponseMessage::allocate_body(int8_t opcode) {
  response_body_.reset();
  switch (opcode) {

    case CQL_OPCODE_ERROR:
      response_body_.reset(new ErrorResponse());
      return true;

    case CQL_OPCODE_READY:
      response_body_.reset(new ReadyResponse());
      return true;

    case CQL_OPCODE_AUTHENTICATE:
      response_body_.reset(new AuthenticateResponse());
      return true;

    case CQL_OPCODE_SUPPORTED:
      response_body_.reset(new SupportedResponse());
      return true;

    case CQL_OPCODE_RESULT:
      response_body_.reset(new ResultResponse());
      return true;

    case CQL_OPCODE_EVENT:
      response_body_.reset(new EventResponse());
      return true;

    case CQL_OPCODE_AUTH_CHALLENGE:
      response_body_.reset(new AuthChallengeResponse());
      return true;

    case CQL_OPCODE_AUTH_SUCCESS:
      response_body_.reset(new AuthSuccessResponse());
      return true;

    default:
      return false;
  }
}

ssize_t ResponseMessage::decode(const char* input, size_t size) {
  const char* input_pos = input;

  received_ += size;

  if (!is_header_received_) {
    if (version_ == 0) {
      if (received_ < 1) {
        LOG_ERROR("Expected at least 1 byte to decode header version");
        return -1;
      }
      version_ = input[0] & 0x7F; // "input" will always have at least 1 bytes
      if (version_ >= CASS_PROTOCOL_VERSION_V3) {
        header_size_ = CASS_HEADER_SIZE_V3;
      } else {
        header_size_ = CASS_HEADER_SIZE_V1_AND_V2;
      }
    }

    if (received_ >= header_size_) {
      // We may have received more data then we need, only copy what we need
      size_t overage = received_ - header_size_;
      size_t needed = size - overage;

      memcpy(header_buffer_pos_, input_pos, needed);
      header_buffer_pos_ += needed;
      input_pos += needed;
      assert(header_buffer_pos_ == header_buffer_ + header_size_);

      const char* buffer = header_buffer_ + 1; // Skip over "version" byte
      flags_ = *(buffer++);

      if (version_ >= CASS_PROTOCOL_VERSION_V3) {
        buffer = decode_int16(buffer, stream_);
      } else {
        stream_ = *(buffer++);
      }
      opcode_ = *(buffer++);

      decode_int32(buffer, length_);

      is_header_received_ = true;

      // If a deprecated version of the protocol is encountered then we fake
      // an invalid protocol error.
      if (version_ < CASS_PROTOCOL_VERSION_V3) {
        response_body_.reset(new InvalidProtocolErrorResponse());
      } else if (!allocate_body(opcode_) || !response_body_) {
        return -1;
      }

      response_body_->set_buffer(length_);
      body_buffer_pos_ = response_body_->data();
    } else {
      // We haven't received all the data for the header. We consume the
      // entire buffer.
      memcpy(header_buffer_pos_, input_pos, size);
      header_buffer_pos_ += size;
      return size;
    }
  }

  const size_t remaining = size - (input_pos - input);
  const size_t frame_size = header_size_ + length_;

  if (received_ >= frame_size) {
    // We may have received more data then we need, only copy what we need
    size_t overage = received_ - frame_size;
    size_t needed = remaining - overage;

    memcpy(body_buffer_pos_, input_pos, needed);
    body_buffer_pos_ += needed;
    input_pos += needed;
    assert(body_buffer_pos_ == response_body_->data() + length_);
    Decoder decoder(response_body_->data(), length_, ProtocolVersion(version_));

    if (flags_ & CASS_FLAG_TRACING) {
      if (!response_body_->decode_trace_id(decoder)) return -1;
    }

    if (flags_ & CASS_FLAG_WARNING) {
      if (!response_body_->decode_warnings(decoder)) return -1;
    }

    if (flags_ & CASS_FLAG_CUSTOM_PAYLOAD) {
      if (!response_body_->decode_custom_payload(decoder)) return -1;
    }

    if (!response_body_->decode(decoder)) {
      is_body_error_ = true;
      return -1;
    }

    is_body_ready_ = true;
  } else {
    // We haven't received all the data for the frame. We consume the entire
    // buffer.
    memcpy(body_buffer_pos_, input_pos, remaining);
    body_buffer_pos_ += remaining;
    return size;
  }

  return input_pos - input;
}
