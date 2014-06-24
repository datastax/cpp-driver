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

#include <uv.h>
#include <assert.h>

#include <string>

#include "cassandra.h"
#include "common.hpp"
#include "scoped_mutex.hpp"
#include "scoped_ptr.hpp"
#include "ref_counted.h"

namespace cass {

struct Error;

enum FutureType {
  CASS_FUTURE_TYPE_SESSION_CONNECT,
  CASS_FUTURE_TYPE_SESSION_CLOSE,
  CASS_FUTURE_TYPE_RESPONSE
};

class Future : public RefCounted<Future> {
public:
  struct Error {
    Error(CassError code, const std::string& message)
        : code(code)
        , message(message) {}

    CassError code;
    std::string message;
  };

  Future(FutureType type)
      : RefCounted<Future>(1) // For the external reference
      , is_set_(false)
      , type_(type) {
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

  virtual bool wait_for(uint64_t timeout) {
    ScopedMutex lock(&mutex_);
    if (!is_set_) {
      uv_cond_timedwait(&cond_, lock.get(), timeout * 1000); // Expects nanos
    }
    return is_set_;
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
    error_.reset(new Error(code, message));
    internal_set(lock);
  }

protected:
  void internal_wait(ScopedMutex& lock) {
    while (!is_set_) {
      uv_cond_wait(&cond_, lock.get());
    }
  }

  // This *MUST* be the final call. The object could be deleted.
  void internal_set(ScopedMutex& lock) {
    is_set_ = true;
    uv_cond_broadcast(&cond_);
    lock.unlock();
    release();
  }

  uv_mutex_t mutex_;

private:
  bool is_set_;
  uv_cond_t cond_;
  FutureType type_;
  ScopedPtr<Error> error_;

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

  void set_result(T* result) {
    ScopedMutex lock(&mutex_);
    result_.reset(result);
    internal_set(lock);
  }

  T* release_result() {
    ScopedMutex lock(&mutex_);
    internal_wait(lock);
    return result_.release();
  }

private:
  ScopedPtr<T> result_;
};

} // namespace cass

#endif
