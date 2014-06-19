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
#include "scoped_ptr.hpp"
#include "ref_counted.h"

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
  ScopedRefPtr<Request> request_body;
  ScopedPtr<Response> response_body;
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
      , request_body(NULL)
      , body_buffer_pos(NULL)
      , body_ready(false)
      , body_error(false) {}

  Message(uint8_t opcode)
      : version(0x02)
      , flags(0)
      , stream(0)
      , opcode(opcode)
      , length(0)
      , received(0)
      , header_received(false)
      , header_buffer_pos(header_buffer)
      , request_body(NULL)
      , body_buffer_pos(NULL)
      , body_ready(false)
      , body_error(false) {
    assert(allocate_body(opcode));
  }

  bool allocate_body(int8_t opcode) {
    request_body.reset();
    response_body.reset();
    switch (opcode) {
      case CQL_OPCODE_RESULT:
        response_body.reset(new ReadyResponse());
        return true;

      case CQL_OPCODE_PREPARE:
        request_body.reset(new PrepareRequest());
        return true;

      case CQL_OPCODE_ERROR:
        response_body.reset(new ErrorResponse());
        return true;

      case CQL_OPCODE_OPTIONS:
        request_body.reset(new OptionsRequest());
        return true;

      case CQL_OPCODE_STARTUP:
        request_body.reset(new StartupRequest());
        return true;

      case CQL_OPCODE_SUPPORTED:
        response_body.reset(new SupportedResponse());
        return true;

      case CQL_OPCODE_QUERY:
        request_body.reset(new QueryRequest());
        return true;

      case CQL_OPCODE_READY:
        response_body.reset(new ReadyResponse());
        return true;

      default:
        return false;
    }
  }

  bool prepare(char** output, size_t& size) {
    size = 0;

    if (request_body && request_body->prepare(CASS_HEADER_SIZE, output, size)) {
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

        if(!allocate_body(opcode) || !response_body) {
          return -1;
        }

        response_body->set_buffer(new char[length]);
        body_buffer_pos = response_body->buffer();
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
      assert(body_buffer_pos == response_body->buffer() + length);

      if (!response_body->consume(response_body->buffer(), length)) {
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
