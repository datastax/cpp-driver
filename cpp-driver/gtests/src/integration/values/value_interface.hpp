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

#ifndef __TEST_VALUE_INTERFACE_HPP__
#define __TEST_VALUE_INTERFACE_HPP__
#include "cassandra.h"

#include <string>

#include "objects/collection.hpp"
#include "objects/statement.hpp"
#include "objects/tuple.hpp"
#include "objects/user_type.hpp"

#include <gtest/gtest.h>

#define COMPARABLE_VALUE_INTERFACE(native, value) test::driver::ValueInterface<native>, \
                                                  test::driver::Comparable<native>, \
                                                  test::driver::Comparable<value>

#define COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(native, value) test::driver::ValueInterface<native>, \
                                                             test::driver::Comparable<value>

namespace test {
namespace driver {

/**
 * Create a comparable template to act as an interface for comparing
 * values.
 */
template<typename T>
class Comparable {
  friend bool operator==(const T& lhs, const T& rhs) {
    return lhs.compare(rhs) == 0;
  }

  friend bool operator!=(const T& lhs, const T& rhs) {
    return lhs.compare(rhs) != 0;
  }

  friend bool operator<(const T& lhs, const T& rhs) {
    return lhs.compare(rhs) <= -1;
  }

  friend bool operator>(const T& lhs, const T& rhs) {
    return lhs.compare(rhs) >= -1;
  }
};

/**
 * Value is a common interface for all the data types provided by the
 * DataStax C/C++ driver. This interface will perform expectations on the
 * value type and other miscellaneous needs for testing.
 */
template<typename T>
class ValueInterface {
public:
  /**
   * Append the value to a collection
   *
   * @param collection Collection to append the value to
   */
  virtual void append(Collection collection) = 0;

  /**
   * Get the CQL type
   *
   * @return CQL type name
   */
  virtual std::string cql_type() const = 0;

  /**
   * Get the CQL value (for embedded simple statements)
   *
   * @return CQL type value
   */
  virtual std::string cql_value() const = 0;

  /**
   * Comparison operation for Comparable template
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  virtual int compare(const T& rhs) const = 0;

  /**
   * Determine if the value is NULL (or unassigned)
   *
   * @return True if value is NULL; false otherwise
   */
  virtual bool is_null() const = 0;

  /**
   * Set the value to a index in the tuple
   *
   * @param tuple Tuple to apply to the value to
   * @param index Index to place the value in the tuple
   */
  virtual void set(Tuple tuple, size_t index) = 0;

  /**
   * Set the value to a field in the user type
   *
   * @param user_type User type to apply to the value to
   * @param name Name of the field in the user type
   */
  virtual void set(UserType user_type, const std::string& name) = 0;

  /**
   * Bind the value to a statement at the given index
   *
   * @param statement The statement to bind the value to
   * @param index The index/position where the value will be bound in the
   *              statement
   */
  virtual void statement_bind(Statement statement, size_t index) = 0;

  /**
   * Convert the value to a standard string
   *
   * @return Standard string representation
   */
  virtual std::string str() const = 0;

  /**
   * Get the native driver value
   *
   * @return Native driver value
   */
  virtual T value() const = 0;

  /**
   * Get the type of value the native driver value is
   *
   * @return Value type of the native driver value
   */
  virtual CassValueType value_type() const = 0;

protected:
  /**
   * Initialize the Value from the CassValue
   *
   * @param value CassValue to initialize Value from
   */
  virtual void initialize(const CassValue* value) = 0;
};

/**
 * Value is a common interface for all the data types provided by the
 * DataStax C/C++ driver. This interface will perform expectations on the
 * value type and other miscellaneous needs for testing.
 */
template<typename K, typename V>
class KeyValueInterface {
public:
  /**
   * Append the value to a collection
   *
   * @param collection Collection to append the value to
   */
  virtual void append(Collection collection) = 0;

  /**
   * Get the CQL type
   *
   * @return CQL type name
   */
  virtual std::string cql_type() const = 0;

  /**
   * Get the CQL value (for embedded simple statements)
   *
   * @return CQL type value
   */
  virtual std::string cql_value() const = 0;

  /**
   * Determine if the value is NULL (or unassigned)
   *
   * @return True if value is NULL; false otherwise
   */
  virtual bool is_null() const = 0;

  /**
   * Get the native driver value (keys)
   *
   * @return Native driver value (keys)
   */
  virtual std::vector<K> keys() const = 0;

  /**
   * Get the type of value the native driver key value is
   *
   * @return Value type of the native driver key value
   */
  virtual CassValueType key_type() const = 0;

  /**
   * Set the value to a index in the tuple
   *
   * @param tuple Tuple to apply to the value to
   * @param index Index to place the value in the tuple
   */
  virtual void set(Tuple tuple, size_t index) = 0;

  /**
   * Set the value to a field in the user type
   *
   * @param user_type User type to apply to the value to
   * @param name Name of the field in the user type
   */
  virtual void set(UserType user_type, const std::string& name) = 0;

  /**
   * Bind the value to a statement at the given index
   *
   * @param statement The statement to bind the value to
   * @param index The index/position where the value will be bound in the
   *              statement
   */
  virtual void statement_bind(Statement statement, size_t index) = 0;

  /**
   * Convert the value to a standard string
   *
   * @return Standard string representation
   */
  virtual std::string str() const = 0;

  /**
   * Get the native driver value
   *
   * @return Native driver value
   */
  virtual std::map<K, V> value() const = 0;

  /**
   * Get the native driver value (values)
   *
   * @return Native driver value (values)
   */
  virtual std::vector<V> values() const = 0;

  /**
   * Get the type of value the native driver value is
   *
   * @return Value type of the native driver value
   */
  virtual CassValueType value_type() const = 0;

protected:
  /**
   * Initialize the Value from the CassValue
   *
   * @param value CassValue to initialize Value from
   */
  virtual void initialize(const CassValue* value) = 0;
};

} // namespace driver
} // namespace test

#endif // __TEST_VALUE_INTERFACE_HPP__
