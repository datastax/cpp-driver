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
#ifndef __DRIVER_OBJECT_ROWS_HPP__
#define __DRIVER_OBJECT_ROWS_HPP__
#include "cassandra.h"

#include "objects/iterator.hpp"
#include "objects/result.hpp"

#include <gtest/gtest.h>

namespace driver {
  namespace object {

    class RowsIterator {
    public:
      typedef std::forward_iterator_tag iterator_category;
      typedef ptrdiff_t difference_type;

      RowsIterator(CassIterator* iterator, size_t size)
        : iterator_(iterator)
        , size_(size) {}

      RowsIterator(shared::native::IteratorPtr iterator, size_t size)
        : iterator_(iterator)
        , size_(size) {}



    private:
      shared::native::IteratorPtr iterator_;
      size_t size_;
    };

    class Rows {
    public:
      /**
       * Create the rows object from the native driver result object
       *
       * @param result Native driver object
       */
      Rows(CassResult* result)
        : iterator_(NULL)
        , column_count_(0)
        , row_count_(0) {
        if (result) {
          iterator_ =  cass_iterator_from_result(result);
        }
      }

      /**
       * Create the rows object from a shared result reference
       *
       * @param result Shared reference
       */
      Rows(shared::native::ResultPtr result)
        : iterator_(NULL)
        , column_count_(0)
        , row_count_(0) {
        if (result.get()) {
          iterator_ = cass_iterator_from_result(result.get());
        }
      }

      /**
       * Create the rows object from a shared result object reference
       *
       * @param result Shared object reference
       */
      Rows(shared::ResultPtr result)
        : iterator_(NULL)
        , column_count_(0)
        , row_count_(0) {
        if (result->result_.get()) {
          iterator_ = cass_iterator_from_result(result->result_.get());
        }
      }

      /**
       * Get the total number of columns
       *
       * @return The total number of columns
       */
      size_t column_count() {
        return column_count_;
      }

      /**
       * Get the total number of rows
       *
       * @return The total number of rows
       */
      size_t row_count() {
        return row_count_;
      }

    private:
      /**
       * Iterator driver reference object
       */
      shared::native::IteratorPtr iterator_;
      /**
       * Number of columns in the rows
       */
      size_t column_count_;
      /**
       * Total number of rows
       */
      size_t row_count_;
    };

    // Create scoped and shared pointers for the wrapped object
    namespace scoped {
      typedef cass::ScopedPtr<Rows> RowsPtr;
    }
    namespace shared {
      typedef SmartPtr<Rows> RowsPtr;
    }
  }
}

#endif // __DRIVER_OBJECT_RESULT_HPP__
