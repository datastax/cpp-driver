/*
  Copyright (c) 2014-2016 DataStax

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
#ifndef __DRIVER_OBJECT_FUTURE_HPP__
#define __DRIVER_OBJECT_FUTURE_HPP__
#include "cassandra.h"

#include "smart_ptr.hpp"

#include <string>

#include <gtest/gtest.h>

namespace driver {
  namespace object {

    /**
     * Deleter class for driver object CassFuture
     */
    class FutureDeleter {
    public:
      void operator()(CassFuture* future) {
        if (future) {
          cass_future_free(future);
        }
      }
    };

    // Create scoped and shared pointers for the native driver object
    namespace scoped {
      namespace native {
        typedef cass::ScopedPtr<CassFuture, FutureDeleter> FuturePtr;
      }
    }
    namespace shared {
      namespace native {
        typedef SmartPtr<CassFuture, FutureDeleter> FuturePtr;
      }
    }

    /**
     * Wrapped future object
     */
    class Future {
    friend class Result;
    friend class Session;
    public:
      /**
       * Create the future object from the native driver object
       *
       * @param future Native driver object
       */
      Future(CassFuture* future)
        : future_(future) {}

      /**
       * Create the future object from a shared reference
       *
       * @param future Shared reference
       */
      Future(shared::native::FuturePtr future)
        : future_(future) {}

      /**
       * Get the error code from the future
       *
       * @return Error code of the future
       */
      CassError error_code() {
        return cass_future_error_code(future_.get());
      }

      /**
       * Get the human readable description of the error code
       *
       * @return Error description
       */
      std::string error_description() {
        return std::string(cass_error_desc(error_code()));
      }

      /**
       * Get the error message of the future if an error occurred
       *
       * @return Error message
       */
      std::string error_message() {
        const char* message;
        size_t message_length;
        cass_future_error_message(future_.get(), &message, &message_length);
        return std::string(message, message_length);
      }

      /**
       * Wait for the future to resolve itself
       *
       * @param assert_ok True if error code for future should be asserted
       *                  CASS_OK; false otherwise (default: true)
       */
      void wait(bool assert_ok = true) {
        CassError wait_code = error_code();
        if (assert_ok) {
          ASSERT_EQ(CASS_OK, wait_code)
            << error_description() << ": " << error_message();
        }
      }

      /**
       * Wait for the future to resolve itself or timeout after the specified
       * duration
       *
       * @param timeout Timeout (in milliseconds) for the future to resolve
       *                itself
       * @param assert_true True if error code for future should be asserted
       *                    CASS_OK; false otherwise (default: true)
       */
      void wait(cass_duration_t timeout, bool assert_true = true) {
        cass_bool_t timed_out = cass_future_wait_timed(future_.get(), timeout);
        if (assert_true) {
          ASSERT_EQ(cass_true, timed_out)
            << error_description() << ": " << error_message();
        }
      }

    private:
      /**
       * Future driver reference object
       */
      shared::native::FuturePtr future_;
    };

    // Create scoped and shared pointers for the wrapped object
    namespace scoped {
      typedef cass::ScopedPtr<Future> FuturePtr;
    }
    namespace shared {
      typedef SmartPtr<Future> FuturePtr;
    }
  }
}

#endif // __DRIVER_OBJECT_FUTURE_HPP__
