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

#include "host.hpp"

#define CASS_HEADER_SIZE 8

namespace cass {

struct Message {
  uint8_t version;
  int8_t flags;
  int8_t stream;
  uint8_t opcode;
  int32_t length;
  size_t received;
  bool header_received;
  char header_buffer[CASS_HEADER_SIZE];
  char* header_buffer_pos;
  std::unique_ptr<MessageBody> body;
  char* body_buffer_pos;
  bool body_ready;
  bool body_error;

  Message()
      : version(0x02)
      , flags(0)
      , stream(0)
      , opcode(0)
      , length(0)
      , received(0)
      , header_received(false)
      , header_buffer_pos(header_buffer)
      , body_ready(false) {}

  Message(uint8_t opcode)
      : version(0x02)
      , flags(0)
      , stream(0)
      , opcode(opcode)
      , length(0)
      , received(0)
      , header_received(false)
      , header_buffer_pos(header_buffer)
      , body(allocate_body(opcode))
      , body_ready(false) {}

  static MessageBody* allocate_body(uint8_t opcode) {
    switch (opcode) {
      case CQL_OPCODE_RESULT:
        return static_cast<MessageBody*>(new ResultResponse());

      case CQL_OPCODE_PREPARE:
        return static_cast<MessageBody*>(new PrepareRequest());

      case CQL_OPCODE_ERROR:
        return static_cast<MessageBody*>(new ErrorResponse());

      case CQL_OPCODE_OPTIONS:
        return static_cast<MessageBody*>(new OptionsRequest());

      case CQL_OPCODE_STARTUP:
        return static_cast<MessageBody*>(new StartupRequest());

      case CQL_OPCODE_SUPPORTED:
        return static_cast<MessageBody*>(new SupportedResponse());

      case CQL_OPCODE_QUERY:
        return static_cast<MessageBody*>(new QueryRequest());

      case CQL_OPCODE_READY:
        return static_cast<MessageBody*>(new ReadyResponse());

      default:
        assert(false);
        return nullptr;
    }
  }

  bool prepare(char** output, size_t& size) {
    size = 0;

    if (body && body->prepare(CASS_HEADER_SIZE, output, size)) {
      length = size - CASS_HEADER_SIZE;

      uint8_t* buffer = reinterpret_cast<uint8_t*>(*output);
      buffer[0] = version;
      buffer[1] = flags;
      buffer[2] = stream;
      buffer[3] = opcode;
      encode_int(reinterpret_cast<char*>(buffer + 4), length);
      return true;
    }

    return false;
  }

  /**
   *
   *
   * @param buf the source buffer
   *
   * @return how many bytes copied
   */
  ssize_t consume(char* input, size_t size) {
    char* input_pos = input;

    received += size;

    if (!header_received) {
      if (received >= CASS_HEADER_SIZE) {
        // We may have received more data then we need, only copy what we need
        size_t overage = received - CASS_HEADER_SIZE;
        size_t needed = size - overage;

        memcpy(header_buffer_pos, input_pos, needed);
        header_buffer_pos += needed;
        input_pos += needed;
        assert(header_buffer_pos == header_buffer + CASS_HEADER_SIZE);

        char* buffer = header_buffer;
        version = *(buffer++);
        flags = *(buffer++);
        stream = *(buffer++);
        opcode = *(buffer++);

        decode_int(buffer, length);

        header_received = true;

        body.reset(allocate_body(opcode));
        body->set_buffer(new char[length]);
        body_buffer_pos = body->buffer();

        if (!body) {
          // Unhandled opcode
          return -1;
        }
      } else {
        // We haven't received all the data for the header. We consume the
        // entire buffer.
        memcpy(header_buffer_pos, input_pos, size);
        header_buffer_pos += size;
        return size;
      }
    }

    const size_t remaining = size - (input_pos - input);
    const size_t frame_size = CASS_HEADER_SIZE + length;

    if (received >= frame_size) {
      // We may have received more data then we need, only copy what we need
      size_t overage = received - frame_size;
      size_t needed = remaining - overage;

      memcpy(body_buffer_pos, input_pos, needed);
      body_buffer_pos += needed;
      input_pos += needed;
      assert(body_buffer_pos == body->buffer() + length);

      if (!body->consume(body->buffer(), length)) {
        body_error = true;
      }
      body_ready = true;
    } else {
      // We haven't received all the data for the frame. We consume the entire
      // buffer.
      memcpy(body_buffer_pos, input_pos, remaining);
      body_buffer_pos += remaining;
      return size;
    }

    return input_pos - input;
  }
};

} // namespace cass

#endif
