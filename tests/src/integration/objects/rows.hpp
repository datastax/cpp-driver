/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_ROWS_HPP__
#define __TEST_ROWS_HPP__
#include "cassandra.h"

#include "objects/iterator.hpp"

namespace test {
namespace driver {

/**
 * Rows object
 */
class Rows {
public:
  /**
   * Create the rows object from a wrapped result object
   *
   * @param iterator Wrapped iterator object
   * @param row_count Total number of rows
   * @param column_count Number of column in a row
   */
  Rows(Iterator iterator, size_t row_count, size_t column_count)
    : iterator_(iterator)
    , row_count_(row_count)
    , column_count_(column_count) {}

  /**
   * Get the total number of columns in a row
   *
   * @return The total number of columns in a row
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

  /**
   * Get the next row
   *
   * @return The next row; NULL if iterator is NULL or has reached the end of
   *         the iterator
   */
  const CassRow* next() {
    if (cass_iterator_next(iterator_.get())) {
      return cass_iterator_get_row(iterator_.get());
    }
    return NULL;
  }

private:
  /**
   * Iterator driver wrapped object
   */
  Iterator iterator_;
  /**
   * Number of rows
   */
  size_t row_count_;
  /**
   * Number of columns in a row
   */
  size_t column_count_;
};

} // namespace driver
} // namespace test

#endif // __TEST_ROWS_HPP__
