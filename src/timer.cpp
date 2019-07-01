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

#include "timer.hpp"

using namespace datastax::internal::core;

Timer::Timer()
    : handle_(NULL)
    , state_(CLOSED) {}

Timer::~Timer() { stop(); }

int Timer::start(uv_loop_t* loop, uint64_t timeout, const Timer::Callback& callback) {
  int rc = 0;
  if (handle_ == NULL) {
    handle_ = new AllocatedT<uv_timer_t>();
    handle_->loop = NULL;
    handle_->data = this;
  }
  if (state_ == CLOSED) {
    rc = uv_timer_init(loop, handle_);
    if (rc != 0) return rc;
    state_ = STOPPED;
  }
  rc = uv_timer_start(handle_, on_timeout, timeout, 0);
  if (rc != 0) return rc;
  state_ = STARTED;
  callback_ = callback;
  return 0;
}

void Timer::stop() {
  if (handle_ != NULL) {
    if (state_ == CLOSED) { // The handle was allocated, but initialization failed.
      delete handle_;
    } else { // If initialized or started then close the handle properly.
      uv_close(reinterpret_cast<uv_handle_t*>(handle_), on_close);
    }
    state_ = CLOSED;
    handle_ = NULL;
  }
}

void Timer::on_timeout(uv_timer_t* handle) {
  Timer* timer = static_cast<Timer*>(handle->data);
  timer->handle_timeout();
}

void Timer::handle_timeout() {
  state_ = STOPPED;
  callback_(this);
}

void Timer::on_close(uv_handle_t* handle) {
  delete reinterpret_cast<AllocatedT<uv_timer_t>*>(handle);
}
