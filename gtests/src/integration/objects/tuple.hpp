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

#ifndef __TEST_TUPLE_HPP__
#define __TEST_TUPLE_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "objects/iterator.hpp"
#include "objects/statement.hpp"

#include <gtest/gtest.h>

namespace test { namespace driver {

/**
 * Tuple object
 *
 * //TODO: Needs proper implementation
 */
class Tuple : public Object<CassTuple, cass_tuple_free> {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };

  /**
   * Create an empty tuple object
   *
   * @param size Number of elements in the tuple
   */
  Tuple(size_t size)
      : Object<CassTuple, cass_tuple_free>(cass_tuple_new(size))
      , size_(size)
      , is_null_(true) {}

  /**
   * Create the tuple from a particular column
   *
   * @param column Column to retrieve tuple from
   */
  Tuple(const CassValue* column)
      : size_(0)
      , is_null_(true) {
    initialize(column);
  }

  /**
   * Determine if the tuple is NULL (or unassigned)
   *
   * @return True if tuple is NULL; false otherwise
   */
  bool is_null() { return is_null_; }

  /**
   * Get the next value
   *
   * @return The next value; NULL if iterator is NULL or has reached the end of
   *         the iterator
   */
  const CassValue* next() {
    if (cass_iterator_next(iterator_.get())) {
      return cass_iterator_get_value(iterator_.get());
    }
    return NULL;
  }

  /**
   * Set the value in the tuple
   *
   * @param value Parameterized value to set to the tuple
   * @param index Index to place the value in the tuple
   * @throws Exception If tuple is not able to have values added to it (e.g.
   *         The tuple was generated from server result)
   */
  template <typename T>
  void set(T value, size_t index) {
    value.set(*this, index);
    is_null_ = false;
  }

  /**
   * Get the size of the tuple
   *
   * @return The number of elements in the tuple
   */
  size_t size() { return size_; }

  /**
   * Get the current value from the tuple iterator (retrieved from server)
   *
   * @return Current value in the tuple
   * @throws Exception If iterator is not valid
   */
  template <typename T>
  T value() {
    if (iterator_) {
      return T(cass_iterator_get_value(iterator_.get()));
    }
    throw Exception("Invalid Tuple: Values not retrieved from server");
  }

  /**
   * Gets all the values as a single type (retrieved from server)
   *
   * @return The tuple as a vector of a single type
   */
  template <typename T>
  std::vector<T> values() {
    std::vector<T> result;
    const CassValue* value;
    while ((value = next()) != NULL) {
      result.push_back(T(value));
    }
    return result;
  }

  /**
   * Bind the tuple to a statement at the given index
   *
   * @param statement The statement to bind the value to
   * @param index The index/position where the value will be bound in the
   *              statement
   */
  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_tuple(statement.get(), index, get()));
  }

protected:
  /**
   * Iterator driver wrapped object
   */
  Iterator iterator_;
  /**
   * Number of element in the tuple
   */
  size_t size_;
  /**
   * Flag to determine if a tuple is empty (null)
   */
  bool is_null_;

  /**
   * Initialize the iterator from the CassValue
   *
   * @param value CassValue to initialize iterator from
   */
  void initialize(const CassValue* value) {
    // Ensure the value is a tuple
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    ASSERT_EQ(CASS_VALUE_TYPE_TUPLE, cass_value_type(value));

    // Initialize the iterator
    size_ = cass_value_item_count(value);
    iterator_ = cass_iterator_from_tuple(value);

    // Determine if the tuple is empty (null)
    const CassValue* check_value = cass_iterator_get_value(iterator_.get());
    if (check_value) {
      is_null_ = false;
    }
  }
};

}} // namespace test::driver

#endif // __TEST_TUPLE_HPP__
