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

#ifndef DATASTAX_INTERNAL_ASYNC_HPP
#define DATASTAX_INTERNAL_ASYNC_HPP

#include "allocated.hpp"
#include "callback.hpp"
#include "macros.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

/**
 * A wrapper for uv_async. This is useful for signaling an event loop that's
 * running on another thread.
 */
class Async {
public:
  typedef internal::Callback<void, Async*> Callback;

  Async();

  ~Async();

  /**
   * Start the async handle.
   *
   * @param loop The event loop that will process the handle.
   * @param callback A callback that handles async send events.
   */
  int start(uv_loop_t* loop, const Callback& callback);

  /**
   * Notify the event loop. The callback will be run.
   */
  void send();

  /**
   * Close the async handle.
   */
  void close_handle();

  /**
   * Determines if the async handle is currently processing notifications.
   *
   * @return Returns true if processing notifications.
   */
  bool is_running() const;

public:
  uv_loop_t* loop() { return handle_ ? handle_->loop : NULL; }

private:
  static void on_async(uv_async_t* handle);
  static void on_close(uv_handle_t* handle);

private:
  AllocatedT<uv_async_t>* handle_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(Async);
};

}}} // namespace datastax::internal::core

#endif
