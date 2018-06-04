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

#ifndef __CASS_PREPARE_HPP_INCLUDED__
#define __CASS_PREPARE_HPP_INCLUDED__

#include "callback.hpp"
#include "macros.hpp"
#include "memory.hpp"

#include <uv.h>

namespace cass {

/**
 * A wrapper for uv_prepare. This is useful for processing that needs to be
 * done before the event loop goes back into waiting.
 */
class Prepare {
public:
  typedef cass::Callback<void, Prepare*> Callback;

  Prepare()
    : handle_(NULL)
    , state_(CLOSED) { }

  ~Prepare() {
    close_handle();
  }

  /**
   * Start the prepare handle.
   *
   * @param loop The event loop that will process the handle.
   * @param callback A callback that handles prepare events.
   */
  int start(uv_loop_t* loop, const Callback& callback) {
    int rc = 0;
    if (handle_ == NULL) {
      handle_ = Memory::allocate<uv_prepare_t>();
      handle_->loop = NULL;
      handle_->data = this;
    }
    if (state_ == CLOSED) {
      rc = uv_prepare_init(loop, handle_);
      if (rc != 0) return rc;
      state_ = STOPPED;
    }
    if (state_ == STOPPED) {
      rc = uv_prepare_start(handle_, on_prepare);
      if (rc != 0) return rc;
      state_ = STARTED;
    }
    callback_ = callback;
    return 0;
  }

  /**
   * Stop the prepare handle.
   */
  void stop() {
    if (state_ == STARTED) {
      state_ = STOPPED;
      uv_prepare_stop(handle_);
    }
  }

  /**
   * Close the prepare handle.
   */
  void close_handle() {
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
  uv_loop_t* loop() {  return handle_ ? handle_->loop : NULL;  }

private:
  static void on_prepare(uv_prepare_t* handle) {
    Prepare* prepare = static_cast<Prepare*>(handle->data);
    prepare->callback_(prepare);
  }

  static void on_close(uv_handle_t* handle) {
    Memory::deallocate(reinterpret_cast<uv_prepare_t*>(handle));
  }

private:
  enum State {
    CLOSED,
    STOPPED,
    STARTED
  };

private:
  uv_prepare_t* handle_;
  State state_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(Prepare);
};

} // namespace cass

#endif
