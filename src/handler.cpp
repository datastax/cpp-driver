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
      } else if (next_state == REQUEST_STATE_TIMEOUT) {
        state_ = next_state;
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
