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

#ifndef __CASS_ASYNC_HPP_INCLUDED__
#define __CASS_ASYNC_HPP_INCLUDED__

#include "callback.hpp"
#include "macros.hpp"
#include "memory.hpp"

#include <uv.h>

namespace cass {

/**
 * A wrapper for uv_async. This is useful for signaling an event loop that's
 * running on another thread.
 */
class Async {
public:
  typedef cass::Callback<void, Async*> Callback;

  Async()
    : handle_(NULL) { }

  ~Async() {
    close_handle();
  }

  /**
   * Start the async handle.
   *
   * @param loop The event loop that will process the handle.
   * @param callback A callback that handles async send events.
   */
  int start(uv_loop_t* loop, const Callback& callback) {
    if (handle_ == NULL) {
      handle_ = Memory::allocate<uv_async_t>();
      handle_->data = this;
      int rc = uv_async_init(loop, handle_, on_async);
      if (rc != 0) return rc;
    }
    callback_ = callback;
    return 0;
  }

  /**
   * Notify the event loop. The callback will be run.
   */
  void send() {
    if (handle_ == NULL) return;
    uv_async_send(handle_);
  }

  /**
   * Close the async handle.
   */
  void close_handle() {
    if (handle_ == NULL) return;
    uv_close(reinterpret_cast<uv_handle_t*>(handle_), on_close);
    handle_ = NULL;
  }

  /**
   * Determines if the async handle is currently processing notifications.
   *
   * @return Returns true if processing notifications.
   */
  bool is_running() const {
    if (handle_ == NULL) return false;
    return uv_is_active(reinterpret_cast<uv_handle_t*>(handle_)) != 0;
  }

public:
  uv_loop_t* loop() { return handle_ ? handle_->loop : NULL; }

private:
  static void on_async(uv_async_t* handle) {
    Async* async = static_cast<Async*>(handle->data);
    async->callback_(async);
  }

  static void on_close(uv_handle_t* handle) {
    Memory::deallocate(reinterpret_cast<uv_async_t*>(handle));
  }

private:
  uv_async_t* handle_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(Async);
};

} // namespace cass

#endif
