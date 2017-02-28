/*
  Copyright (c) 2014-2016 DataStax

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

#include "cassconfig.hpp"
#include "logger.hpp"
#include "macros.hpp"

#include <assert.h>
#include <uv.h>

#if !defined(_WIN32)
#include <signal.h>
#endif

namespace cass {

#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
static int block_sigpipe() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  return pthread_sigmask(SIG_BLOCK, &set, NULL);
}

static void consume_blocked_sigpipe() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  struct timespec ts = { 0, 0 };
  int num = sigtimedwait(&set, NULL, &ts);
  if (num > 0) {
    LOG_WARN("Caught and ignored SIGPIPE on loop thread");
  }
}
#endif

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

#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
    rc = block_sigpipe();
    if (rc != 0) return rc;
    rc = uv_prepare_init(loop(), &prepare_);
    if (rc != 0) return rc;
    rc = uv_prepare_start(&prepare_, on_prepare);
    if (rc != 0) return rc;
#endif
    return rc;
  }

  void close_handles() {
#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
    uv_prepare_stop(&prepare_);
    uv_close(reinterpret_cast<uv_handle_t*>(&prepare_), NULL);
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

#if UV_VERSION_MAJOR == 0
  uv_loop_t* loop_;
#else
  uv_loop_t loop_;
  bool is_loop_initialized_;
#endif

#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)

#if UV_VERSION_MAJOR == 0
  static void on_prepare(uv_prepare_t *prepare, int status) {
     consume_blocked_sigpipe();
  }
#else
  static void on_prepare(uv_prepare_t *prepare) {
    consume_blocked_sigpipe();
  }
#endif

  uv_prepare_t prepare_;
#endif

  uv_thread_t thread_;
  bool is_joinable_;
};

} // namespace cass

#endif
