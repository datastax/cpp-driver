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
#include "logger.hpp"
#include "request.hpp"
#include "result_response.hpp"

namespace cass {

bool Handler::encode(int version, int flags) {
  return request()->encode(version, flags, stream_, &writer_.bufs());
}

void Handler::write(uv_stream_t* stream, void* data,
                    RequestWriter::Callback cb) {
  writer_.write(stream, data, cb);
}

void Handler::set_state(Handler::State next_state) {
  switch (state_) {
    case REQUEST_STATE_NEW:
      if (next_state == REQUEST_STATE_NEW) {
        state_ = next_state;
        stream_ = -1;
      } else if (next_state == REQUEST_STATE_WRITING) {
        state_ = next_state;
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
      } else if (next_state == REQUEST_STATE_TIMEOUT) {
        state_ = REQUEST_STATE_TIMEOUT_WRITE_OUTSTANDING;
      } else {
        assert(false && "Invalid request state after writing");
      }
      break;

    case REQUEST_STATE_READING:
      if (next_state == REQUEST_STATE_DONE) { // Success
        stop_timer();
        state_ = next_state;
      } else if (next_state == REQUEST_STATE_TIMEOUT) {
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after reading");
      }
      break;

    case REQUEST_STATE_TIMEOUT:
      assert(next_state == REQUEST_STATE_DONE &&
             "Invalid request state after read timeout");
      state_ = next_state;
      break;

    case REQUEST_STATE_TIMEOUT_WRITE_OUTSTANDING:
      assert((next_state == REQUEST_STATE_TIMEOUT || next_state == REQUEST_STATE_DONE) &&
              "Invalid request state after timeout (write outstanding)");
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
