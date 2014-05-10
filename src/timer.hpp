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

#ifndef __CASS_TIMER_HPP_INCLUDED__
#define __CASS_TIMER_HPP_INCLUDED__

namespace cass {

class Timer {
  public:
    typedef std::function<void(void*)> Callback;

    static Timer* start(uv_loop_t* loop, uint64_t timeout, void* data, Callback cb) {
      Timer* timer = new Timer(data, cb);
      timer->handle_.data = timer;
      uv_timer_init(loop, &timer->handle_);
      uv_timer_start(&timer->handle_, on_timeout, timeout, 0);
      return timer;
    }

    static void stop(Timer* timer) {
      uv_timer_stop(&timer->handle_);
      delete timer;
    }

  private:
    static void on_timeout(uv_timer_t* handle, int status) {
      Timer* timer = static_cast<Timer*>(handle->data);
      timer->cb_(timer->data_);
      delete timer;
    }

  private:
    Timer(void* data, Callback cb)
      : data_(data)
      , cb_(cb) { }

    uv_timer_t handle_;
    void* data_;
    Callback cb_;
};

} // namespace cass

#endif
