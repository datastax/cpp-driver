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

template<class T>
class LoopWatcher {
public:
  typedef cass::Callback<void, T*> Callback;
  typedef typename T::HandleType HandleType;

  LoopWatcher()
    : handle_(NULL)
    , state_(CLOSED) { }

  ~LoopWatcher() {
    close_handle();
  }

  /**
   * Start the handle.
   *
   * @param loop The event loop that will process the handle.
   * @param callback A callback that handles events.
   */
  int start(uv_loop_t* loop, const Callback& callback) {
    int rc = 0;
    if (handle_ == NULL) {
      handle_ = Memory::allocate<HandleType>();
      handle_->loop = NULL;
      handle_->data = this;
    }
    if (state_ == CLOSED) {
      rc = T::init_handle(loop, handle_);
      if (rc != 0) return rc;
      state_ = STOPPED;
    }
    if (state_ == STOPPED) {
      rc = T::start_handle(handle_, on_run);
      if (rc != 0) return rc;
      state_ = STARTED;
    }
    callback_ = callback;
    return 0;
  }

  /**
   * Stop the handle.
   */
  void stop() {
    if (state_ == STARTED) {
      state_ = STOPPED;
      T::stop_handle(handle_);
    }
  }

  /**
   * Close the handle.
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
  static void on_run(HandleType* handle) {
    T* watcher = static_cast<T*>(handle->data);
    watcher->callback_(watcher);
  }

  static void on_close(uv_handle_t* handle) {
    Memory::deallocate(reinterpret_cast<HandleType*>(handle));
  }

private:
  enum State {
    CLOSED,
    STOPPED,
    STARTED
  };

private:
  HandleType* handle_;
  State state_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(LoopWatcher);
};

/**
 * A wrapper for uv_prepare. This is useful for processing that needs to be
 * done before the event loop goes back into waiting.
 */
class Prepare : public LoopWatcher<Prepare> {
private:
  friend class LoopWatcher<Prepare>;

  typedef uv_prepare_t HandleType;
  typedef uv_prepare_cb CallbackType;

  int init_handle(uv_loop_t* loop, HandleType* handle) {
    return uv_prepare_init(loop, handle);
  }

  int start_handle(HandleType* handle, CallbackType callback) {
    return uv_prepare_start(handle, callback);
  }

  void stop_handle(HandleType* handle) {
    uv_prepare_stop(handle);
  }
};

} // namespace cass

#endif
