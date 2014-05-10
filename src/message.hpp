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

#include "batch_statement.hpp"
#include "body_error.hpp"
#include "body_options.hpp"
#include "body_prepare.hpp"
#include "body_ready.hpp"
#include "body_result.hpp"
#include "body_startup.hpp"
#include "body_supported.hpp"
#include "bound_statement.hpp"
#include "query_statement.hpp"


#define CASS_HEADER_SIZE 8

namespace cass {

struct Message;

typedef std::unique_ptr<Message> MessagePtr;
typedef FutureImpl<std::string, MessagePtr> MessageFutureImpl;

struct Message {
  uint8_t                         version;
  int8_t                          flags;
  int8_t                          stream;
  uint8_t                         opcode;
  int32_t                         length;
  int32_t                         received;
  bool                            header_received;
  char                            header_buffer[CASS_HEADER_SIZE];
  char*                           header_buffer_pos;
  std::unique_ptr<MessageBody>    body;
  char*                           body_buffer_pos;
  bool                            body_ready;
  bool                            body_error;

  Message() :
      version(0x02),
      flags(0),
      stream(0),
      opcode(0),
      length(0),
      received(0),
      header_received(false),
      header_buffer_pos(header_buffer),
      body_ready(false)
  {}

  Message(
      uint8_t  opcode) :
      version(0x02),
      flags(0),
      stream(0),
      opcode(opcode),
      length(0),
      received(0),
      header_received(false),
      header_buffer_pos(header_buffer),
      body(allocate_body(opcode)),
      body_ready(false)
  {}

  inline static MessageBody*
  allocate_body(
      uint8_t  opcode) {
    switch (opcode) {
      case CQL_OPCODE_RESULT:
        return static_cast<MessageBody*>(new Result());

      case CQL_OPCODE_PREPARE:
        return static_cast<MessageBody*>(new PrepareStatement());

      case CQL_OPCODE_ERROR:
        return static_cast<MessageBody*>(new BodyError());

      case CQL_OPCODE_OPTIONS:
        return static_cast<MessageBody*>(new BodyOptions());

      case CQL_OPCODE_STARTUP:
        return static_cast<MessageBody*>(new BodyStartup());

      case CQL_OPCODE_SUPPORTED:
        return static_cast<MessageBody*>(new BodySupported());

      case CQL_OPCODE_QUERY:
        return static_cast<MessageBody*>(new QueryStatement());

      case CQL_OPCODE_READY:
        return static_cast<MessageBody*>(new BodyReady());

      default:
        assert(false);
    }
  }

  bool
  prepare(
      char**  output,
      size_t& size) {
    size = 0;
    if (body.get()) {
      body->prepare(CASS_HEADER_SIZE, output, size);

      if (!size) {
        *output = new char[CASS_HEADER_SIZE];
        size = CASS_HEADER_SIZE;
      } else {
        length = size - CASS_HEADER_SIZE;
      }

      uint8_t* buffer = reinterpret_cast<uint8_t*>(*output);
      buffer[0]       = version;
      buffer[1]       = flags;
      buffer[2]       = stream;
      buffer[3]       = opcode;
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
  int
  consume(
      char*  input,
      size_t size) {
    char* input_pos  = input;
    received        += size;

    if (!header_received) {
      if (received >= CASS_HEADER_SIZE) {
        // we've received more data then we need, just copy what we need
        size_t overage = received - CASS_HEADER_SIZE;
        size_t needed  = size - overage;
        memcpy(header_buffer_pos, input_pos, needed);

        char* buffer       = header_buffer;
        version            = *(buffer++);
        flags              = *(buffer++);
        stream             = *(buffer++);
        opcode             = *(buffer++);
        memcpy(&length, buffer, sizeof(int32_t));
        length = ntohl(length);

        input_pos         += needed;
        header_buffer_pos  = header_buffer + CASS_HEADER_SIZE;
        header_received    = true;

        body.reset(allocate_body(opcode));
        body->set_buffer(new char[length]);
        body_buffer_pos = body->buffer();

        if (body == NULL) {
          return -1;
        }
      } else {
        // we haven't received all the data yet
        // copy the entire input to our buffer
        memcpy(header_buffer_pos, input_pos, size);
        header_buffer_pos += size;
        input_pos += size;
        return size;
      }
    }

    if (received - CASS_HEADER_SIZE >= length) {
      // we've received more data then we need, just copy what we need
      size_t overage = received - length - CASS_HEADER_SIZE;
      size_t needed = (size - (input_pos - input)) - overage;

      memcpy(body_buffer_pos, input_pos, needed);
      body_buffer_pos += needed;
      input_pos       += needed;

      if (!body->consume(body->buffer(), length)) {
        body_error = true;
      }
      body_ready = true;
    } else {
      // we haven't received all the data yet
      // copy the entire input to our buffer
      memcpy(body_buffer_pos, input_pos, size - (input_pos - input));
      input_pos       += size;
      body_buffer_pos += size;
      return size;
    }
    return input_pos - input;
  }
};

} // namespace cass

#endif
