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

#ifndef __CASS_TIMER_HPP_INCLUDED__
#define __CASS_TIMER_HPP_INCLUDED__

#include "callback.hpp"
#include "macros.hpp"
#include "memory.hpp"

#include <uv.h>

namespace cass {

class Timer {
public:
  typedef cass::Callback<void, Timer*> Callback;

  Timer()
    : handle_(NULL)
    , state_(CLOSED) { }

  ~Timer() {
    close_handle();
  }

  int start(uv_loop_t* loop, uint64_t timeout, const Callback& callback) {
    int rc = 0;
    if (handle_ == NULL) {
      handle_ = Memory::allocate<uv_timer_t>();
      handle_->loop = NULL;
      handle_->data = this;
    }
    if (state_ == CLOSED) {
      rc = uv_timer_init(loop, handle_);
      if (rc != 0) return rc;
      state_ = STOPPED;
    }
    if (state_ == STOPPED) {
      rc = uv_timer_start(handle_, on_timeout, timeout, 0);
      if (rc != 0) return rc;
      state_ = STARTED;
    }
    callback_ = callback;
    return 0;
  }

  void stop() {
    if (state_ == STARTED) {
      state_ = STOPPED;
      uv_timer_stop(handle_);
    }
  }

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
  uv_loop_t* loop() { return handle_ ? handle_->loop : NULL; }

private:
  static void on_timeout(uv_timer_t* handle) {
    Timer* timer = static_cast<Timer*>(handle->data);
    timer->handle_timeout();
  }

  void handle_timeout() {
    state_ = STOPPED;
    callback_(this);
  }

  static void on_close(uv_handle_t* handle) {
    Memory::deallocate(reinterpret_cast<uv_timer_t*>(handle));
  }

private:
  enum State {
    CLOSED,
    STOPPED,
    STARTED
  };

private:
  uv_timer_t* handle_;
  State state_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(Timer);
};

} // namespace cass

#endif
