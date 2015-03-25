/*
  Copyright (c) 2014-2015 DataStax

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

#ifndef __CASS_LOOP_THREAD_HPP_INCLUDED__
#define __CASS_LOOP_THREAD_HPP_INCLUDED__

#include "macros.hpp"

#include <assert.h>
#include <uv.h>

namespace cass {

class LoopThread {
public:
  LoopThread()
#if UV_VERSION_MAJOR == 0
      : loop_(uv_loop_new())
#else
      : is_loop_initialized_(false)
#endif
      , is_joinable_(false) {}

  virtual ~LoopThread() { 
#if UV_VERSION_MAJOR == 0
    uv_loop_delete(loop_); 
#else
    if (is_loop_initialized_) {
      uv_loop_close(&loop_);
    }
#endif
  }

  int init() {
    int rc = 0;
#if UV_VERSION_MAJOR > 0
    rc = uv_loop_init(&loop_);
    if (rc != 0) return rc;
    is_loop_initialized_ = true;
#endif

#if !defined(_WIN32)
    rc = uv_signal_init(loop(), &sigpipe_);
    if (rc != 0) return rc;
    rc = uv_signal_start(&sigpipe_, on_signal, SIGPIPE);
#endif
    return rc;
  }

  void close_handles() {
#if !defined(_WIN32)
    uv_signal_stop(&sigpipe_);
    uv_close(copy_cast<uv_signal_t*, uv_handle_t*>(&sigpipe_), NULL);
#endif
  }

#if UV_VERSION_MAJOR == 0
  uv_loop_t* loop() { return loop_; }
#else
  uv_loop_t* loop() { return &loop_; }
#endif

  int run() {
    int rc = uv_thread_create(&thread_, on_run_internal, this);
    if (rc == 0) is_joinable_ = true;
    return rc;
  }

  void join() {
    if (is_joinable_) {
      is_joinable_ = false;
      int rc = uv_thread_join(&thread_);
      UNUSED_(rc);
      assert(rc == 0);
    }
  }

protected:
  virtual void on_run() {}
  virtual void on_after_run() {}

private:
  static void on_run_internal(void* data) {
    LoopThread* thread = static_cast<LoopThread*>(data);
    thread->on_run();
    uv_run(thread->loop(), UV_RUN_DEFAULT);
    thread->on_after_run();
  }

#if !defined(_WIN32)
  static void on_signal(uv_signal_t* signal, int signum) {
    // Ignore SIGPIPE
  }
#endif

#if UV_VERSION_MAJOR == 0
  uv_loop_t* loop_;
#else
  uv_loop_t loop_;
  bool is_loop_initialized_;
#endif

  uv_thread_t thread_;
  bool is_joinable_;

#if !defined(_WIN32)
  uv_signal_t sigpipe_;
#endif
};

} // namespace cass

#endif
