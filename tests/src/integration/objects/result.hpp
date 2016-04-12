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
#ifndef __DRIVER_OBJECT_RESULT_HPP__
#define __DRIVER_OBJECT_RESULT_HPP__
#include "cassandra.h"

#include "objects/future.hpp"
#include "objects/iterator.hpp"
#include "objects/session.hpp"

#include "smart_ptr.hpp"

#include <string>

#include <gtest/gtest.h>

namespace driver {
  namespace object {

    /**
     * Deleter class for driver object CassResult
     */
    class ResultDeleter {
    public:
      void operator()(const CassResult* result) {
        if (result) {
          cass_result_free(result);
        }
      }
    };

    // Create scoped and shared pointers for the native driver object
    namespace scoped {
      namespace native {
        typedef cass::ScopedPtr<const CassResult, ResultDeleter> ResultPtr;
      }
    }
    namespace shared {
      namespace native {
        typedef SmartPtr<const CassResult, ResultDeleter> ResultPtr;
      }
    }

    class Result {
    friend class Rows;
    public:
      /**
       * Create the result object from the native driver object
       *
       * @param result Native driver object
       */
      Result(CassResult* result)
        : result_(result)
        , future_(NULL) {}

      /**
       * Create the result object from a shared reference
       *
       * @param result Shared reference
       */
      Result(shared::native::ResultPtr result)
        : result_(result)
        , future_(NULL) {}

      /**
       * Create the result object from a future object
       *
       * @param future Wrapped driver reference object
       */
      Result(shared::FuturePtr future)
        : result_(NULL)
        , future_(future) {
        // Ensure the result can be retrieved
        if (future->error_code() == CASS_OK) {
          result_ = cass_future_get_result(future->future_.get());
        }
      }

      /**
       * Get the number of columns from the result
       *
       * @return The number of columns in the result; 0 if result is NULL
       */
      size_t column_count() {
        if (result_.get()) {
          return cass_result_column_count(result_.get());
        }
        return 0;
      }

      /**
       * Get the rows from the result
       *
       * @return The rows from the result
       */
      shared::native::IteratorPtr rows() {
        if (result_.get()) {
          return cass_iterator_from_result(result_.get());
        }
        return NULL;
      }

      /**
       * Get the number of rows from the result
       *
       * @return The number of rows in the result; 0 if result is NULL
       */
      size_t row_count() {
        if (result_.get()) {
          return cass_result_row_count(result_.get());
        }
        return 0;
      }

    private:
      /**
       * Result driver reference object
       */
      shared::native::ResultPtr result_;
      /**
       * Future wrapped reference object
       */
      shared::FuturePtr future_;
    };

    // Create scoped and shared pointers for the wrapped object
    namespace scoped {
      typedef cass::ScopedPtr<Result> ResultPtr;
    }
    namespace shared {
      typedef SmartPtr<Result> ResultPtr;
    }
  }
}

#endif // __DRIVER_OBJECT_RESULT_HPP__
