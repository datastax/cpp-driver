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

#include "macros.hpp"

#include <uv.h>

namespace cass {

class Timer {
public:
  typedef void (*Callback)(Timer*);

  Timer()
    : handle_(NULL)
    , data_(NULL) { }

  ~Timer() {
    stop();
  }

  void* data() const { return data_; }

  bool is_running() const {
    if (handle_ == NULL) return false;
    return uv_is_active(reinterpret_cast<uv_handle_t*>(handle_)) != 0;
  }

  void start(uv_loop_t* loop, uint64_t timeout, void* data,
             Callback cb) {
    if (handle_ == NULL) {
      handle_ = new uv_timer_t;
      handle_->data = this;
      uv_timer_init(loop, handle_);
    }
    data_ = data;
    cb_ = cb;
    uv_timer_start(handle_, on_timeout, timeout, 0);
  }

  void stop() {
    if (handle_ == NULL) return;
    // This also stops the timer
    uv_close(reinterpret_cast<uv_handle_t*>(handle_), on_close);
    handle_ = NULL;
  }

#if UV_VERSION_MAJOR == 0
  static void on_timeout(uv_timer_t* handle, int status) {
#else
  static void on_timeout(uv_timer_t* handle) {
#endif
    Timer* timer = static_cast<Timer*>(handle->data);
    // The timer handle needs to be closed everytime because a stopped timer
    // will not prevent uv_run() from exiting the event loop and the handle
    // can't be deleted immediately because the event loop is still using the
    // memory. Closing the handle everytime guarantees it will be cleaned up
    // properly without crashing the event loop.
    timer->stop();
    timer->cb_(timer);
  }

  static void on_close(uv_handle_t* handle) {
    delete reinterpret_cast<uv_timer_t*>(handle);
  }

private:
  uv_timer_t* handle_;
  void* data_;
  Callback cb_;

private:
  DISALLOW_COPY_AND_ASSIGN(Timer);
};

} // namespace cass

#endif
