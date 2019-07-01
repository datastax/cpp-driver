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

#ifndef DATASTAX_INTERNAL_LOOP_WATCHER_HPP
#define DATASTAX_INTERNAL_LOOP_WATCHER_HPP

#include "allocated.hpp"
#include "callback.hpp"
#include "macros.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

template <class Type, class HType>
class LoopWatcher {
public:
  typedef internal::Callback<void, Type*> Callback;
  typedef HType HandleType;

  LoopWatcher()
      : handle_(NULL)
      , state_(CLOSED) {}

  ~LoopWatcher() { close_handle(); }

  /**
   * Start the handle.
   *
   * @param loop The event loop that will process the handle.
   * @param callback A callback that handles events.
   */
  int start(uv_loop_t* loop, const Callback& callback) {
    int rc = 0;
    if (handle_ == NULL) {
      handle_ = new AllocatedT<HandleType>();
      handle_->loop = NULL;
      handle_->data = this;
    }
    if (state_ == CLOSED) {
      rc = Type::init_handle(loop, handle_);
      if (rc != 0) return rc;
      state_ = STOPPED;
    }
    if (state_ == STOPPED) {
      rc = Type::start_handle(handle_, on_run);
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
      Type::stop_handle(handle_);
    }
  }

  /**
   * Close the handle.
   */
  void close_handle() {
    if (handle_ != NULL) {
      if (state_ == CLOSED) { // The handle was allocated, but initialization failed.
        delete handle_;
      } else { // If initialized or started then close the handle properly.
        uv_close(reinterpret_cast<uv_handle_t*>(handle_), on_close);
      }
      state_ = CLOSED;
      handle_ = NULL;
    }
  }

public:
  bool is_running() const { return state_ == STARTED; }
  uv_loop_t* loop() { return handle_ ? handle_->loop : NULL; }

private:
  static void on_run(HandleType* handle) {
    Type* watcher = static_cast<Type*>(handle->data);
    watcher->callback_(watcher);
  }

  static void on_close(uv_handle_t* handle) {
    delete reinterpret_cast<AllocatedT<HandleType>*>(handle);
  }

private:
  enum State { CLOSED, STOPPED, STARTED };

private:
  AllocatedT<HandleType>* handle_;
  State state_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(LoopWatcher);
};

/**
 * A wrapper for uv_prepare. This is useful for running a callback right before
 * the event loop begins polling.
 */
class Prepare : public LoopWatcher<Prepare, uv_prepare_t> {
private:
  typedef uv_prepare_cb HandleCallback;
  friend class LoopWatcher<Prepare, HandleType>;

  static int init_handle(uv_loop_t* loop, HandleType* handle) {
    return uv_prepare_init(loop, handle);
  }

  static int start_handle(HandleType* handle, HandleCallback callback) {
    return uv_prepare_start(handle, callback);
  }

  static void stop_handle(HandleType* handle) { uv_prepare_stop(handle); }
};

/**
 * A wrapper for uv_check. This is useful for running a callback right after
 * the event loop returns from polling.
 */
class Check : public LoopWatcher<Check, uv_check_t> {
private:
  typedef uv_check_cb HandleCallback;
  friend class LoopWatcher<Check, HandleType>;

  static int init_handle(uv_loop_t* loop, HandleType* handle) {
    return uv_check_init(loop, handle);
  }

  static int start_handle(HandleType* handle, HandleCallback callback) {
    return uv_check_start(handle, callback);
  }

  static void stop_handle(HandleType* handle) { uv_check_stop(handle); }
};

}}} // namespace datastax::internal::core

#endif
