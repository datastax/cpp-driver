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
#ifndef __DRIVER_OBJECT_ITERATOR_HPP__
#define __DRIVER_OBJECT_ITERATOR_HPP__
#include "cassandra.h"

#include "smart_ptr.hpp"

#include <gtest/gtest.h>

namespace driver {
  namespace object {

    /**
     * Deleter class for driver object CassIterator
     */
    class IteratorDeleter {
    public:
      void operator()(CassIterator* iterator) {
        if (iterator) {
          cass_iterator_free(iterator);
        }
      }
    };

    // Create scoped and shared pointers for the native driver object
    namespace scoped {
      namespace native {
        typedef cass::ScopedPtr<CassIterator, IteratorDeleter> IteratorPtr;
      }
    }
    namespace shared {
      namespace native {
        typedef SmartPtr<CassIterator, IteratorDeleter> IteratorPtr;
      }
    }
  }
}

#endif // __DRIVER_OBJECT_ITERATOR_HPP__
