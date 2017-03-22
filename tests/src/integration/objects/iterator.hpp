/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_ITERATOR_HPP__
#define __TEST_ITERATOR_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include <gtest/gtest.h>

namespace test {
namespace driver {

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

} // namespace driver
} // namespace test

#endif // __TEST_ITERATOR_HPP__
