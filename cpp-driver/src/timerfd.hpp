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

#ifndef __CASS_TIMERFD_HPP_INCLUDED__
#define __CASS_TIMERFD_HPP_INCLUDED__

#include "cassconfig.hpp"

#ifdef HAVE_TIMERFD
#include "callback.hpp"
#include "macros.hpp"
#include "memory.hpp"

#include <string.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <uv.h>

namespace cass {

class TimerFd {
public:
  typedef cass::Callback<void, TimerFd*> Callback;

  TimerFd()
    : handle_(NULL)
    , fd_(-1)
    , state_(CLOSED) { }

  ~TimerFd() {
    close_handle();
  }

  int start(uv_loop_t* loop, uint64_t timeout_us, const Callback& callback) {
    int rc = 0;
    if (fd_ == -1) {
      fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
      if (fd_ == -1)  return errno;
    }
    if (handle_ == NULL) {
      handle_ = Memory::allocate<uv_poll_t>();
      handle_->loop = NULL;
      handle_->data = this;
    }
    if (state_ == CLOSED) {
      rc = uv_poll_init(loop, handle_, fd_);
      if (rc != 0) return rc;
      rc = uv_poll_start(handle_, UV_READABLE, on_timeout);
      if (rc != 0) return rc;
      state_ = STOPPED;
    }
    if (state_ == STOPPED) {
      set_time(timeout_us);
      state_ = STARTED;
    }
    callback_ = callback;
    return rc;
  }

  void stop() {
    if (state_ == STARTED) {
      state_ = STOPPED;
      set_time(0);
    }
  }

  void close_handle() {
    if (fd_ != -1) {
      close(fd_);
      fd_ = -1;
    }
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
  void set_time(uint64_t timeout_us) {
    struct itimerspec ts;
    memset(&ts.it_interval, 0, sizeof(struct timespec));
    ts.it_value.tv_sec = timeout_us / (1000 * 1000);
    ts.it_value.tv_nsec = (timeout_us % (1000 * 1000))  * 1000;
    timerfd_settime(fd_, 0, &ts, NULL);
  }

  static void on_timeout(uv_poll_t* poll, int status, int events) {
    TimerFd* timer = static_cast<TimerFd*>(poll->data);
    timer->handle_timeout();
  }

  void handle_timeout() {
    uint64_t count;
    int result = read(fd_, &count, sizeof(count));
    UNUSED_(result);
    state_ = STOPPED;
    callback_(this);
  }

  static void on_close(uv_handle_t* handle) {
    Memory::deallocate(reinterpret_cast<uv_poll_t*>(handle));
  }

private:
  enum State {
    CLOSED,
    STOPPED,
    STARTED
  };

private:
  uv_poll_t* handle_;
  int fd_;
  State state_;
  Callback callback_;
};

} // namespace cass

#endif // HAVE_TIMERFD

#endif
