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

#ifndef __TEST_USER_DATA_TYPE_HPP__
#define __TEST_USER_DATA_TYPE_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "objects/iterator.hpp"
#include "objects/statement.hpp"

#include <gtest/gtest.h>

namespace test { namespace driver {

/**
 * User type object
 *
 * //TODO: Needs proper implementation
 */
class UserType : public Object<CassUserType, cass_user_type_free> {
public:
  /**
   * Create an empty user type object from the schema metadata
   *
   * @param data_type The data type that makes up the user type
   */
  UserType(const CassDataType* data_type)
      : is_null_(true) {
    initialize(data_type);
  }

  /**
   * Create the user type from a particular column
   *
   * @param column Column to retrieve user type from
   */
  UserType(const CassValue* column)
      : size_(0)
      , is_null_(true) {
    initialize(column);
  }

  /**
   * Determine if the user type is NULL (or unassigned)
   *
   * @return True if user type is NULL; false otherwise
   */
  bool is_null() { return is_null_; }

  /**
   * Set the value in the user type
   *
   * @param value Parameterized value to set on the user type
   * @param name Name of the field in the user type
   * @throws Exception If user type is not able to have values added to it (e.g.
   *         The user type was generated from server result)
   */
  template <typename T>
  void set(T value, const std::string& name) {
    value.set(*this, name);
    is_null_ = false;
  }

  /**
   * Get the size of the user type
   *
   * @return The number of elements in the user type
   */
  size_t size() { return size_; }

  /**
   * Get a field value from the user type
   *
   * @return Value of the field in the user type
   * @throws Exception If value is not available
   */
  template <typename T>
  T value(const std::string& name) {
    ValuesMap::iterator iterator = values_.find(name);
    if (iterator != values_.end()) {
      return T(iterator->second);
    }
    throw Exception("Unable to Locate Field [" + name + "] in User Type: Value is not available");
  }

  /**
   * Gets all the fields as a single type
   *
   * @return A map of all the fields where the value is a single type
   */
  template <typename T>
  std::map<std::string, T> values() {
    std::map<std::string, T> result;
    while (cass_iterator_next(iterator_.get())) {
      // Get the name and value
      const char* name;
      size_t name_length;
      if (cass_iterator_get_user_type_field_name(iterator_.get(), &name, &name_length) != CASS_OK) {
        throw Exception("Unable to get field name from iterator");
      }
      const CassValue* value = cass_iterator_get_user_type_field_value(iterator_.get());
      result[std::string(name, name_length)] = T(value);
    }
    return result;
  }

  /**
   * Bind the user type to a statement at the given index
   *
   * @param statement The statement to bind the value to
   * @param index The index/position where the value will be bound in the
   *              statement
   */
  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_user_type(statement.get(), index, get()));
  }

protected:
  typedef std::map<std::string, const CassValue*> ValuesMap;
  typedef std::pair<std::string, const CassValue*> ValuesPair;

  /**
   * Iterator driver wrapped object
   */
  Iterator iterator_;
  /**
   * Values retrieved from iterator
   */
  std::map<std::string, const CassValue*> values_;
  /**
   * Number of element in the user type
   */
  size_t size_;
  /**
   * Flag to determine if a user type is empty (null)
   */
  bool is_null_;

  /**
   * Initialize the iterator from the CassValue
   *
   * @param value CassValue to initialize iterator from
   */
  void initialize(const CassValue* value) {
    // Ensure the value is a user type
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    ASSERT_EQ(CASS_VALUE_TYPE_UDT, cass_value_type(value));

    // Initialize the iterator
    size_ = cass_value_item_count(value);
    iterator_ = cass_iterator_fields_from_user_type(value);
  }

  /**
   * Initialize the user type from the data type
   *
   * @param data_type The data type that makes up the user type
   */
  void initialize(const CassDataType* data_type) {
    ASSERT_TRUE(data_type != NULL) << "Invalid User Type: CassDataType should not be null";

    // Get the field names for the user type
    size_ = cass_data_type_sub_type_count(data_type);
    for (size_t i = 0; i < size_; ++i) {
      // Get the user type field name
      const char* name;
      size_t name_length;
      ASSERT_EQ(CASS_OK, cass_data_type_sub_type_name(data_type, i, &name, &name_length));

      // Create a temporary place holder for the field value
      std::string field(name, name_length);
      const CassValue* temp = NULL;
      values_.insert(ValuesPair(field, temp));
    }

    // Create the user type from the data type
    Object<CassUserType, cass_user_type_free>::set(cass_user_type_new_from_data_type(data_type));
  }
};

}} // namespace test::driver

#endif // __TEST_USER_DATA_TYPE_HPP__
