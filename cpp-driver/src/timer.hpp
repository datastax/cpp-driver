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

#ifndef DATASTAX_INTERNAL_TIMER_HPP
#define DATASTAX_INTERNAL_TIMER_HPP

#include "allocated.hpp"
#include "callback.hpp"
#include "macros.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class Timer {
public:
  typedef internal::Callback<void, Timer*> Callback;

  Timer();
  ~Timer();

  int start(uv_loop_t* loop, uint64_t timeout, const Callback& callback);
  void stop();

public:
  bool is_running() const { return state_ == STARTED; }
  uv_loop_t* loop() { return handle_ ? handle_->loop : NULL; }

private:
  static void on_timeout(uv_timer_t* handle);
  void handle_timeout();

  static void on_close(uv_handle_t* handle);

private:
  enum State { CLOSED, STOPPED, STARTED };

private:
  AllocatedT<uv_timer_t>* handle_;
  State state_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(Timer);
};

}}} // namespace datastax::internal::core

#endif
