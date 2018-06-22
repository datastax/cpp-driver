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

#ifndef __CASS_TIMERSPIN_HPP_INCLUDED__
#define __CASS_TIMERSPIN_HPP_INCLUDED__

#include "timer.hpp"

namespace cass {

/**
 * A wrapper for uv_idle.
 */
class Idle {
public:
  typedef cass::Callback<void, Idle*> Callback;

  Idle()
    : handle_(NULL)
    , state_(CLOSED) { }

  ~Idle() {
    close_handle();
  }

  /**
   * Start the idle handle.
   *
   * @param loop The event loop that will process the handle.
   * @param callback A callback that handles idle events.
   */
  int start(uv_loop_t* loop, const Callback& callback) {
    int rc = 0;
    if (handle_ == NULL) {
      handle_ = Memory::allocate<uv_idle_t>();
      handle_->loop = NULL;
      handle_->data = this;
    }
    if (state_ == CLOSED) {
      rc = uv_idle_init(loop, handle_);
      if (rc != 0) return rc;
      state_ = STOPPED;
    }
    if (state_ == STOPPED) {
      rc = uv_idle_start(handle_, on_idle);
      if (rc != 0) return rc;
      state_ = STARTED;
    }
    callback_ = callback;
    return 0;
  }

  /**
   * Stop the idle handle.
   */
  void stop() {
    if (state_ == STARTED) {
      state_ = STOPPED;
      uv_idle_stop(handle_);
    }
  }

  /**
   * Close the idle handle.
   */
  void close_handle() {
    if (handle_ != NULL) {
      if (state_ == CLOSED) { // The handle was allocate, but initialization failed.
        Memory::deallocate(handle_);
      } else { // If initialized or started then close the handle properly.
        uv_close(reinterpret_cast<uv_handle_t*>(handle_), on_close);
      }
      state_ = CLOSED;
      handle_ = NULL;
    }
  }

public:
  bool is_running() const { return state_ == STARTED; }
  uv_loop_t* loop() {  return handle_ ? handle_->loop : NULL;  }

private:
  static void on_idle(uv_idle_t* handle) {
    Idle* idle = static_cast<Idle*>(handle->data);
    idle->callback_(idle);
  }

  static void on_close(uv_handle_t* handle) {
    Memory::deallocate(reinterpret_cast<uv_idle_t*>(handle));
  }

private:
  enum State {
    CLOSED,
    STOPPED,
    STARTED
  };

private:
  uv_idle_t* handle_;
  State state_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(Idle);
};

class TimerSpin {
public:
  typedef cass::Callback<void, TimerSpin*> Callback;

  TimerSpin()
    : timeout_(0) { }

  void start(uv_loop_t* loop, uint64_t timeout_us, const Callback& callback) {
    uint64_t ms = timeout_us / 1000;
    uint64_t us =  timeout_us - ms * 1000;

    timeout_ = uv_hrtime() + timeout_us * 1000;

    if (ms > 1) {
      timer_.start(loop, ms, on_timeout);
    } else if (us > 950) {
      timer_.start(loop, ms + 1, on_timeout);
    } else {
      idle_.start(loop, bind_callback(&TimerSpin::on_idle, this));
    }
  }

  void stop();

private:
  void on_timeout(Timer* timer) {
    uint64_t now = uv_hrtime();
    if (timeout_ != 0 && timeout_ <= uv_hrtime()) {
      timeout_ = 0;
      timer_callback_(this);
    }
  }

  void on_idle(Idle* idle) {
    uint64_t now = uv_hrtime();
    if (timeout_ > now) {
      idle_.stop();
    }
  }

private:
  Idle idle_;
  Timer timer_;
  uint64_t timeout_;
  Callback callback_;
};

} // namespace cass

#endif
