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

#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <future>

namespace cass {

struct Error;

enum FutureType {
  CASS_FUTURE_TYPE_SESSION,
  CASS_FUTURE_TYPE_REQUEST,
};

class Future {
  public:
    struct Result {
        virtual ~Result() { }
    };

    class ResultOrError {
      public:
        ResultOrError(const Error* error)
          : result_(nullptr)
          , error_(error) { }

        ResultOrError(Result* result)
          : result_(result)
          , error_(nullptr) { }

        ~ResultOrError() {
          Result* result = release();
          if(result != nullptr) {
            delete result;
          }
        }

        bool is_error() const {
          return error_ != nullptr;
        }

        const Error* error() const {
          return error_.get();
        }

        Result* release() {
          Result* expected = result_;
          if(result_.compare_exchange_strong(expected, nullptr)) {
            return expected;
          }
          return nullptr;
        }

      private:
        std::atomic<Result*> result_;
        std::unique_ptr<const Error> error_;
    };

    Future(FutureType type)
      : type_(type)
      , is_set_(false) { }

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

    ResultOrError* get() {
      std::unique_lock<std::mutex> lock(mutex_);
      while(!is_set_) {
        cond_.wait(lock);
      }
      return result_or_error_.get();
    }

    void set_error(const Error* error) {
      std::unique_lock<std::mutex> lock(mutex_);
      result_or_error_.reset(new ResultOrError(error));
      is_set_ = true;
      cond_.notify_all();
    }

    void set_result(Future::Result* result = nullptr) {
      std::unique_lock<std::mutex> lock(mutex_);
      result_or_error_.reset(new ResultOrError(result));
      is_set_ = true;
      cond_.notify_all();
    }

   private:
     FutureType type_;
     bool is_set_;
     std::mutex mutex_;
     std::condition_variable cond_;
     std::unique_ptr<ResultOrError> result_or_error_;
};

} // namespace cass

#endif
