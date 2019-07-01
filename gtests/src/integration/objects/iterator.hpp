/*
  Copyright (c) DataStax, Inc.

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

#ifndef __TEST_ITERATOR_HPP__
#define __TEST_ITERATOR_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include <gtest/gtest.h>

namespace test { namespace driver {

class Iterator : public Object<CassIterator, cass_iterator_free> {
public:
  /**
   * Create the empty iterator object
   */
  Iterator()
      : Object<CassIterator, cass_iterator_free>() {}

  /**
   * Create the iterator object from the native driver object
   *
   * @param iterator Native driver object
   */
  Iterator(CassIterator* iterator)
      : Object<CassIterator, cass_iterator_free>(iterator) {}

  /**
   * Create the future object from a shared reference
   *
   * @param future Shared reference
   */
  Iterator(Ptr iterator)
      : Object<CassIterator, cass_iterator_free>(iterator) {}
};

}} // namespace test::driver

#endif // __TEST_ITERATOR_HPP__
