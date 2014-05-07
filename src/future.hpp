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

#ifndef __CASS_FUTURE_HPP_INCLUDED__
#define __CASS_FUTURE_HPP_INCLUDED__

#include <atomic>
#include <uv.h>

namespace cass {

struct Error;

struct Future {
  std::unique_ptr<Error> error;

  virtual bool
  ready() = 0;

  virtual void
  wait() = 0;

  virtual bool
  wait_for(
      size_t wait) = 0;

  virtual
  ~Future() {}
};

template<typename Data,
         typename Result>
struct FutureImpl
    : public Future {
  typedef std::function<void(FutureImpl<Data, Result>*)> Callback;

  std::atomic<bool>       flag;
  std::mutex              mutex;
  std::condition_variable condition;
  Data                    data;
  Result                  result;
  Callback                callback;
  bool                    use_local_loop;
  uv_work_t               uv_work_req;

  FutureImpl() :
      flag(false),
      data(NULL),
      result(nullptr),
      callback(NULL),
      use_local_loop(false)
  {}

  bool
  ready() {
    return flag.load(std::memory_order_consume);
  }

  /**
   * call to set the ready condition to true and notify interested parties
   * threads blocking on wait or wait_for will be woken up and resumed
   * if the caller specified a callback it will be triggered
   *
   * be sure to set the value of result prior to calling notify
   *
   * we execute the callback in a separate thread so that badly
   * behaving client code can't interfere with event/network handling
   *
   * @param loop the libuv event loop
   */
  void
  notify(
      uv_loop_t* loop) {
    flag.store(true, std::memory_order_release);
    condition.notify_all();

    if (callback) {
      if (use_local_loop || loop == NULL) {
        callback(this);
      } else {
        // we execute the callback in a separate thread so that badly
        // behaving client code can't interfere with event/network handling
        uv_work_req.data = this;
        uv_queue_work(
            loop,
            &uv_work_req,
            &FutureImpl<Data, Result>::callback_executor,
            NULL);
      }
    }
  }

  /**
   * wait until the ready condition is met and results are ready
   */
  void
  wait() {
    if (!flag.load(std::memory_order_consume)) {
      std::unique_lock<std::mutex> lock(mutex);
      condition.wait(
          lock,
          std::bind(&FutureImpl<Data, Result>::ready, this));
    }
  }

  bool
  wait_for(
      size_t wait) {
    return wait_for(std::chrono::microseconds(wait));
  }

  /**
   * wait until the ready condition is met, or specified time has elapsed.
   * return value indicates false for timeout
   *
   * @param time
   *
   * @return false for timeout, true if value is ready
   */
  template< class Rep, class Period >
  bool
  wait_for(
      const std::chrono::duration<Rep, Period>& time) {
    if (!flag.load(std::memory_order_consume)) {
      std::unique_lock<std::mutex> lock(mutex);
      return condition.wait_for(
          lock,
          time,
          std::bind(&FutureImpl<Data, Result>::ready, this));
    }
    return true;
  }

 private:
  /**
   * Called by the libuv worker thread, and this method calls the callback
   * this is done to isolate customer code from ours
   *
   * @param work
   */
  static void
  callback_executor(
      uv_work_t* work) {
    (void) work;
    if (work && work->data) {
      FutureImpl<Data, Result>* request
          = reinterpret_cast<FutureImpl<Data, Result>*>(work->data);

      if (request->callback) {
        request->callback(request);
      }
    }
  }

  // don't allow copy
  FutureImpl(FutureImpl<Data, Result>&) {}
  void operator=(const FutureImpl&) {}
};

} // namespace cass

#endif
