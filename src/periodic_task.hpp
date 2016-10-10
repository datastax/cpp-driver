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

#ifndef __CASS_PERIODIC_TASK_HPP_INCLUDED__
#define __CASS_PERIODIC_TASK_HPP_INCLUDED__

#include "macros.hpp"
#include "ref_counted.hpp"

#include <uv.h>

namespace cass {

class PeriodicTask : public RefCounted<PeriodicTask> {
public:
  typedef SharedRefPtr<PeriodicTask> Ptr;

  typedef void (*Callback)(PeriodicTask*);

  void* data() { return data_; }

  static Ptr start(uv_loop_t* loop, uint64_t repeat, void* data,
                             Callback work_cb, Callback after_work_cb) {
    Ptr task(new PeriodicTask(data, work_cb, after_work_cb));

    task->inc_ref(); // Timer reference
    uv_timer_init(loop, &task->timer_handle_);
    uv_timer_start(&task->timer_handle_, on_timeout, repeat, repeat);
    return task;
  }

  static void stop(const Ptr& task) {
    uv_timer_stop(&task->timer_handle_);
    close(task);
  }

private:
  static void close(const Ptr& task) {
    uv_close(reinterpret_cast<uv_handle_t*>(&task->timer_handle_), on_close);
  }

#if UV_VERSION_MAJOR == 0
  static void on_timeout(uv_timer_t* handle, int status) {
#else
  static void on_timeout(uv_timer_t* handle) {
#endif
    PeriodicTask* task = static_cast<PeriodicTask*>(handle->data);

    if (task->is_running_) return;

    task->inc_ref(); // Work reference
    task->is_running_ = true;
    uv_queue_work(handle->loop, &task->work_request_, on_work, on_after_work);
  }

  static void on_close(uv_handle_t* handle) {
    PeriodicTask* task = static_cast<PeriodicTask*>(handle->data);
    task->dec_ref(); // Remove timer reference
  }

  static void on_work(uv_work_t* request) {
    PeriodicTask* task = static_cast<PeriodicTask*>(request->data);
    task->work_cb_(task);
  }

  static void on_after_work(uv_work_t* request, int status) {
    PeriodicTask* task = static_cast<PeriodicTask*>(request->data);
    task->after_work_cb_(task);
    task->is_running_ = false;
    task->dec_ref(); // Remove work reference
  }

private:
  PeriodicTask(void* data, Callback work_cb, Callback after_work_cb)
    : data_(data)
    , work_cb_(work_cb)
    , after_work_cb_(after_work_cb)
    , is_running_(false) {
    timer_handle_.data = this;
    work_request_.data = this;
  }

  void* data_;
  Callback work_cb_;
  Callback after_work_cb_;
  bool is_running_;

  uv_timer_t timer_handle_;
  uv_work_t work_request_;

private:
  DISALLOW_COPY_AND_ASSIGN(PeriodicTask);
};

} // namespace cass

#endif
