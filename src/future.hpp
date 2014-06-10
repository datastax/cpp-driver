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

#include <atomic>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <string>

#include "cassandra.h"

namespace cass {

struct Error;

template<class T>
class RefCounted {
  public:
    RefCounted()
      : ref_count_(1) { }

    void retain() { ref_count_++; }

    void release() {
      int new_ref_count = --ref_count_;
      assert(new_ref_count >= 0);
      if(new_ref_count == 0) {
        delete static_cast<T*>(this);
      }
    }

  private:
      std::atomic<int> ref_count_;
};

enum FutureType {
  CASS_FUTURE_TYPE_SESSION_CONNECT,
  CASS_FUTURE_TYPE_SESSION_CLOSE,
  CASS_FUTURE_TYPE_RESPONSE,
};

class Future : public RefCounted<Future> {
  public:
    struct Error {
        Error(CassError code,
              const std::string& message)
          : code(code)
          , message(message) { }

        CassError code;
        std::string message;
    };

    Future(FutureType type)
      : is_set_(false)
      , type_(type) { }

    virtual ~Future() = default;

    FutureType type() const { return type_; }

    bool ready() {
      return wait_for(0);
    }

    virtual void wait() {
      std::unique_lock<std::mutex> lock(mutex_);
      while(!is_set_) {
        cond_.wait(lock);
      }
    }

    virtual bool wait_for(size_t timeout) {
      std::unique_lock<std::mutex> lock(mutex_);
      if(!is_set_) {
        cond_.wait_for(lock, std::chrono::microseconds(timeout));
      }
      return is_set_;
    }

    bool is_error() {
      return get_error() != nullptr;
    }

    Error* get_error() {
      std::unique_lock<std::mutex> lock(mutex_);
      while(!is_set_) {
        cond_.wait(lock);
      }
      return error_.get();
    }

    void set() {
      set([]() { }); // NOP set
    }

    void set_error(CassError code, const std::string& message) {
      set([this, code, message]() {
        error_.reset(new Error(code, message));
      });
    }

  protected:
    template <class S>
    void set(S s) {
      std::unique_lock<std::mutex> lock(mutex_);
      s();
      is_set_ = true;
      cond_.notify_all();
      lock.unlock();
      release();
    }

    bool is_set_;
    std::mutex mutex_;
    std::condition_variable cond_;

  private:
    FutureType type_;
    std::unique_ptr<Error> error_;
};

template <class T>
class ResultFuture : public Future {
  public:
    ResultFuture(FutureType type)
      : Future(type) { }

    ResultFuture(FutureType type, T* result)
      : Future(type)
      , result_(result) { }

    void set_result(T* result) {
      set([this, result]() {
        result_.reset(result);
      });
    }

    T* release_result() {
      std::unique_lock<std::mutex> lock(mutex_);
      while(!is_set_) {
        cond_.wait(lock);
      }
      return result_.release();
    }

  private:
     std::unique_ptr<T> result_;
};

} // namespace cass

#endif
