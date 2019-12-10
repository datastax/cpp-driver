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

#ifndef __TEST_COLLECTION_HPP__
#define __TEST_COLLECTION_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "objects/iterator.hpp"
#include "objects/statement.hpp"
#include "objects/tuple.hpp"
#include "objects/user_type.hpp"

#include <gtest/gtest.h>

namespace test { namespace driver {

/**
 * Wrapped collection object
 */
class Collection : public Object<CassCollection, cass_collection_free> {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };

  /**
   * Create the collection from a particular column
   *
   * @param column Column to retrieve collection from
   */
  Collection(const CassValue* column)
      : is_null_(true) {
    initialize(column);
  }

  /**
   * Append the collection to a collection
   *
   * @param collection Collection to append the value to
   */
  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_collection(collection.get(), get()));
  }

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
   * Set the collection to the tuple
   *
   * @param tuple Tuple to apply to the value to
   * @param index Index to place the value in the tuple
   */
  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_tuple_set_collection(tuple.get(), index, get()));
    }
  }

  /**
   * Set the collection to a field in the user type
   *
   * @param user_type User type to apply to the value to
   * @param name Name of the field in the user type
   */
  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
                cass_user_type_set_collection_by_name(user_type.get(), name.c_str(), get()));
    }
  }

  /**
   * Bind the collection to a statement at the given index
   *
   * @param statement The statement to bind the value to
   * @param index The index/position where the value will be bound in the
   *              statement
   */
  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_collection(statement.get(), index, get()));
  }

protected:
  /**
   * Iterator driver wrapped object
   */
  Iterator iterator_;
  /**
   * Collection type
   */
  CassCollectionType collection_type_;
  /**
   * Primary value type
   *
   * list/set - sub-type
   * map - key type
   */
  CassValueType primary_sub_type_;
  /**
   * Secondary value type
   *
   * list/set - undefined
   * map - value type
   */
  CassValueType secondary_sub_type_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  /**
   * Create an empty object
   *
   * @param collection_type Type of collection to create
   * @param count Size of the collection (default: 1)
   */
  Collection(CassCollectionType collection_type, size_t count = 1)
      : Object<CassCollection, cass_collection_free>(cass_collection_new(collection_type, count))
      , collection_type_(collection_type)
      , is_null_(true) {}

  /**
   * Append the value to the collection
   *
   * @param value Parameterized value to append to the collection
   * @throws Exception If collection is not able to have values added to it
   *         (e.g. The collection was generated from server result)
   */
  template <typename T>
  void append(T value) {
    ASSERT_TRUE(!value.is_null());
    value.append(*this);

    // Indicate the collection is no longer null
    is_null_ = false;
  }

  /**
   * Initialize the iterator from the CassValue
   *
   * @param value CassValue to initialize iterator from
   */
  virtual void initialize(const CassValue* value) {
    // Ensure the value is a collection
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    if (!cass_value_is_null(value)) {
      ASSERT_TRUE(cass_value_is_collection(value))
          << "Invalid CassValue: Value is not a collection";

      // Determine the type of collection from the value type
      CassValueType value_type = cass_value_type(value);
      if (value_type == CASS_VALUE_TYPE_LIST) {
        collection_type_ = CASS_COLLECTION_TYPE_LIST;
        primary_sub_type_ = secondary_sub_type_ = cass_value_primary_sub_type(value);
      } else if (value_type == CASS_VALUE_TYPE_MAP) {
        collection_type_ = CASS_COLLECTION_TYPE_MAP;
        primary_sub_type_ = cass_value_primary_sub_type(value);
        secondary_sub_type_ = cass_value_secondary_sub_type(value);
      } else if (value_type == CASS_VALUE_TYPE_SET) {
        collection_type_ = CASS_COLLECTION_TYPE_SET;
        primary_sub_type_ = secondary_sub_type_ = cass_value_primary_sub_type(value);
      } else {
        FAIL() << "Invalid CassValueType: Value type is not a valid collection";
      }

      // Initialize the iterator
      iterator_ = cass_iterator_from_collection(value);

      // Determine if the collection is empty (null)
      const CassValue* check_value = cass_iterator_get_value(iterator_.get());
      if (check_value) {
        is_null_ = false;
      }
    }
  }
};

}} // namespace test::driver

#endif // __TEST_COLLECTION_HPP__
