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

#ifndef __CASS_LOOP_THREAD_HPP_INCLUDED__
#define __CASS_LOOP_THREAD_HPP_INCLUDED__

#include <assert.h>
#include <uv.h>

namespace cass {

class LoopThread {
public:
  LoopThread()
      : loop_(uv_loop_new())
      , thread_id_(0)
      , is_joinable_(false) {}

  int init() {
    int rc = 0;
#if !defined(WIN32) && !defined(_WIN32)
    rc = uv_signal_init(loop(), &sigpipe_);
    if (rc != 0) return rc;
    rc = uv_signal_start(&sigpipe_, on_signal, SIGPIPE);
#endif
    return rc;
  }

  void close_handles() {
#if !defined(WIN32) && !defined(_WIN32)
    uv_signal_stop(&sigpipe_);
    uv_close(copy_cast<uv_signal_t*, uv_handle_t*>(&sigpipe_), NULL);
#endif
  }

  virtual ~LoopThread() { uv_loop_delete(loop_); }

  uv_loop_t* loop() { return loop_; }

  int run() {
    int rc = uv_thread_create(&thread_, on_run_internal, this);
    if (rc == 0) is_joinable_ = true;
    return rc;
  }

  void join() {
    if (is_joinable_) {
      is_joinable_ = false;
      assert(uv_thread_join(&thread_) == 0);
    }
  }

  unsigned long thread_id() { return thread_id_; }

protected:
  virtual void on_run() {}
  virtual void on_after_run() {}

private:
  static void on_run_internal(void* data) {
    LoopThread* thread = static_cast<LoopThread*>(data);
    thread->thread_id_ = uv_thread_self();
    thread->on_run();
    uv_run(thread->loop_, UV_RUN_DEFAULT);
    thread->on_after_run();
  }

#if !defined(WIN32) && !defined(_WIN32)
  static void on_signal(uv_signal_t* signal, int signum) {
    // Ignore SIGPIPE
  }
#endif

  uv_loop_t* loop_;
  uv_thread_t thread_;
  unsigned long thread_id_;
  bool is_joinable_;

#if !defined(WIN32) && !defined(_WIN32)
  uv_signal_t sigpipe_;
#endif
};

} // namespace cass

#endif
