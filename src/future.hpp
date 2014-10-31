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

#include "cassandra.h"
#include "host.hpp"
#include "macros.hpp"
#include "scoped_mutex.hpp"
#include "scoped_ptr.hpp"
#include "ref_counted.hpp"

#include "third_party/boost/boost/function.hpp"

#include <uv.h>
#include <assert.h>
#include <string>

namespace cass {

struct Error;

enum FutureType {
  CASS_FUTURE_TYPE_SESSION_CONNECT,
  CASS_FUTURE_TYPE_SESSION_CLOSE,
  CASS_FUTURE_TYPE_RESPONSE
};

class Future : public RefCounted<Future> {
public:
  typedef boost::function2<void, CassFuture*, void*> Callback;

  struct Error {
    Error(CassError code, const std::string& message)
        : code(code)
        , message(message) {}

    CassError code;
    std::string message;
  };

  Future(FutureType type)
      : is_set_(false)
      , type_(type)
      , loop_(NULL) {
    uv_mutex_init(&mutex_);
    uv_cond_init(&cond_);
  }

  virtual ~Future() {
    uv_mutex_destroy(&mutex_);
    uv_cond_destroy(&cond_);
  }

  FutureType type() const { return type_; }

  bool ready() {
    ScopedMutex lock(&mutex_);
    return is_set_;
  }

  virtual void wait() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
  }

  virtual bool wait_for(uint64_t timeout_us) {
    ScopedMutex lock(&mutex_);
    return internal_wait_for(lock, timeout_us);
  }

  bool is_error() { return get_error() != NULL; }

  Error* get_error() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
    return error_.get();
  }

  void set() {
    ScopedMutex lock(&mutex_);
    internal_set(lock);
  }

  void set_error(CassError code, const std::string& message) {
    ScopedMutex lock(&mutex_);
    internal_set_error(code, message, lock);
  }

  void set_loop(uv_loop_t* loop) {
    loop_ = loop;
  }

  bool set_callback(Callback callback, void* data);

protected:
  void internal_wait(ScopedMutex& lock) {
    while (!is_set_) {
      uv_cond_wait(&cond_, lock.get());
    }
  }

  bool internal_wait_for(ScopedMutex& lock, uint64_t timeout_us) {
    if (!is_set_) {
      if (uv_cond_timedwait(&cond_, lock.get(), timeout_us * 1000) != 0) { // Expects nanos
        return false;
      }
    }
    return is_set_;
  }

  void internal_set(ScopedMutex& lock);

  void internal_set_error(CassError code, const std::string& message, ScopedMutex& lock) {
    error_.reset(new Error(code, message));
    internal_set(lock);
  }

  uv_mutex_t mutex_;

private:
  void run_callback_on_work_thread();
  static void on_work(uv_work_t* work);
  static void on_after_work(uv_work_t* work, int status);

private:
  bool is_set_;
  uv_cond_t cond_;
  FutureType type_;
  ScopedPtr<Error> error_;
  boost::atomic<uv_loop_t*> loop_;
  uv_work_t work_;
  Callback callback_;
  void* data_;

private:
  DISALLOW_COPY_AND_ASSIGN(Future);
};

template <class T>
class ResultFuture : public Future {
public:
  ResultFuture(FutureType type)
      : Future(type) {}

  ResultFuture(FutureType type, T* result)
      : Future(type)
      , result_(result) {}

  void set_result(Address address, T* result) {
    ScopedMutex lock(&mutex_);
    address_ = address;
    result_.reset(result);
    internal_set(lock);
  }

  T* release_result() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
    return result_.release();
  }

  void set_error_with_host_address(Address address, CassError code, const std::string& message) {
    ScopedMutex lock(&mutex_);
    address_ = address;
    internal_set_error(code, message, lock);
  }

  Address get_host_address() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
    return address_;
  }

private:
  Address address_;
  ScopedPtr<T> result_;
};

} // namespace cass

#endif
