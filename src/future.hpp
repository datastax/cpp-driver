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
#include <future>

namespace cass {

struct Error;

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

    Future()
      : future_(promise_.get_future()) { }

    virtual ~Future() = default;

    bool ready() {
      return future_.wait_for(std::chrono::microseconds(0)) == std::future_status::ready;
    }

    void wait() {
      future_.wait();
    }

    bool wait_for(size_t timeout) {
      return future_.wait_for(std::chrono::microseconds(timeout)) == std::future_status::ready;
    }

    ResultOrError& get() {
      return *future_.get();
    }

    void set_error(const Error* error) {
      result_or_error_.reset(new ResultOrError(error));
      promise_.set_value(result_or_error_);
    }

    void set_result(Future::Result* result = nullptr) {
      result_or_error_.reset(new ResultOrError(result));
      promise_.set_value(result_or_error_);
    }

   private:
     typedef std::unique_ptr<ResultOrError> ResultOrErrorPtr;
     std::promise<ResultOrErrorPtr&> promise_;
     std::shared_future<ResultOrErrorPtr&> future_;
     ResultOrErrorPtr result_or_error_;
};

} // namespace cass

#endif
