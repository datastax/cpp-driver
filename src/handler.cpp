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

#include "handler.hpp"

#include "config.hpp"
#include "connection.hpp"
#include "constants.hpp"
#include "logger.hpp"
#include "request.hpp"
#include "result_response.hpp"
#include "serialization.hpp"

namespace cass {

int32_t Handler::encode(int version, int flags, BufferVec* bufs) const {
  if (version != 1 && version != 2) {
    return Request::ENCODE_ERROR_UNSUPPORTED_PROTOCOL;
  }

  size_t index = bufs->size();
  bufs->push_back(Buffer()); // Placeholder

  const Request* req = request();
  int32_t length = req->encode(version, bufs);
  if (length < 0) {
    return length;
  }

  Buffer buf(CASS_HEADER_SIZE_V1_AND_V2);
  size_t pos = 0;
  pos = buf.encode_byte(pos, version);
  pos = buf.encode_byte(pos, flags);
  pos = buf.encode_byte(pos, stream_);
  pos = buf.encode_byte(pos, req->opcode());
  buf.encode_int32(pos, length);
  (*bufs)[index] = buf;

  return length + CASS_HEADER_SIZE_V1_AND_V2;
}

void Handler::set_state(Handler::State next_state) {
  switch (state_) {
    case REQUEST_STATE_NEW:
      if (next_state == REQUEST_STATE_NEW) {
        state_ = REQUEST_STATE_NEW;
        stream_ = -1;
      } else if (next_state == REQUEST_STATE_WRITING) {
        state_ = REQUEST_STATE_WRITING;
      } else {
        assert(false && "Invalid request state after new");
      }
      break;

    case REQUEST_STATE_WRITING:
      if (next_state == REQUEST_STATE_READING) { // Success
        state_ = next_state;
      } else if (next_state == REQUEST_STATE_READ_BEFORE_WRITE ||
                 next_state == REQUEST_STATE_DONE) {
        stop_timer();
        state_ = next_state;
      } else if (next_state == REQUEST_STATE_WRITE_TIMEOUT) {
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after writing");
      }
      break;

    case REQUEST_STATE_READING:
      if (next_state == REQUEST_STATE_DONE) { // Success
        stop_timer();
        state_ = next_state;
      } else if (next_state == REQUEST_STATE_READ_TIMEOUT) {
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after reading");
      }
      break;

    case REQUEST_STATE_WRITE_TIMEOUT:
      assert(next_state == REQUEST_STATE_DONE &&
             "Invalid request state after write timeout");
      state_ = next_state;
      break;

    case REQUEST_STATE_READ_TIMEOUT:
      assert(next_state == REQUEST_STATE_DONE &&
             "Invalid request state after read timeout");
      state_ = next_state;
      break;

    case REQUEST_STATE_READ_BEFORE_WRITE:
      assert(next_state == REQUEST_STATE_DONE &&
             "Invalid request state after read before write");
      state_ = next_state;
      break;

    case REQUEST_STATE_DONE:
      assert(next_state == REQUEST_STATE_NEW && "Invalid request state after done");
      state_ = next_state;
      break;

    default:
      assert(false && "Invalid request state");
      break;
  }
}


} // namespace cass
