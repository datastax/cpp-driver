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

#include <uv.h>

namespace cass {

class LoopThread {
public:
  LoopThread()
      : loop_(uv_loop_new())
      , is_running_(false) {}

  virtual ~LoopThread() { uv_loop_delete(loop_); }

  void run() {
    is_running_ = true;
    uv_thread_create(&thread_, on_run_internal, this);
  }

  void join() {
    if (is_running_) {
      uv_thread_join(&thread_);
      is_running_ = false;
    }
  }

protected:
  uv_loop_t* loop() { return loop_; }

  virtual void on_run() {}
  virtual void on_after_run() {}

private:
  void static on_run_internal(void* data) {
    LoopThread* thread = static_cast<LoopThread*>(data);
    thread->on_run();
    uv_run(thread->loop_, UV_RUN_DEFAULT);
    thread->on_after_run();
  }

  uv_loop_t* loop_;
  uv_thread_t thread_;
  std::atomic<bool> is_running_;
};

} // namespace cass

#endif
