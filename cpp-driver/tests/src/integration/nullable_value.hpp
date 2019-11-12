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

#ifndef __TEST_NULLBALE_VALUE_HPP__
#define __TEST_NULLBALE_VALUE_HPP__
#include "cassandra.h"

#include <string>

#include "objects/collection.hpp"
#include "objects/statement.hpp"
#include "objects/tuple.hpp"
#include "objects/user_type.hpp"

#include <gtest/gtest.h>

namespace test { namespace driver {

/**
 * Create a comparable template to act as an interface for comparing
 * values.
 */
template <typename T>
class Comparable {
  friend bool operator==(const T& lhs, const T& rhs) { return lhs.compare(rhs) == 0; }

  friend bool operator!=(const T& lhs, const T& rhs) { return lhs.compare(rhs) != 0; }

  friend bool operator<(const T& lhs, const T& rhs) { return lhs.compare(rhs) <= -1; }

  friend bool operator>(const T& lhs, const T& rhs) { return lhs.compare(rhs) >= -1; }

  friend bool operator<=(const T& lhs, const T& rhs) { return !operator>(lhs, rhs); }

  friend bool operator>=(const T& lhs, const T& rhs) { return !operator<(lhs, rhs); }
};

/**
 * NullableValue is a templated interface for all the server data types provided
 * by the DataStax C/C++ driver. This interface will perform expectations on the
 * value type and other miscellaneous needs for testing; while also allowing the
 * value to be NULL.
 */
template <typename T>
class NullableValue : public Comparable<NullableValue<T> > {
public:
  /**
   * Constructor for a NULL value
   */
  NullableValue()
      : is_null_(true){};

  /**
   * Constructor for a nullable value; convenience constructor
   *
   * @param value Typed value
   */
  explicit NullableValue(const typename T::ConvenienceType& value)
      : is_null_(false)
      , value_(value) {}

  /**
   * Constructor for a nullable value using the wrapped type
   *
   * @param value Wrapped type value
   */
  explicit NullableValue(const T& value)
      : is_null_(false)
      , value_(value) {}

  /**
   * Constructor for a nullable value using the drivers primitive/collection
   * value
   *
   * @param value CassValue from driver query
   */
  explicit NullableValue(const CassValue* value)
      : is_null_(false) {
    initialize(value);
  }

  /**
   * Append the value to a collection
   *
   * @param collection Collection to append the value to
   */
  void append(Collection collection) { value_.append(collection); }

  /**
   * Get the CQL type
   *
   * @return CQL type name
   */
  std::string cql_type() const { return value_.cql_type(); }

  /**
   * Get the CQL value (for embedded simple statements)
   *
   * @return CQL type value
   */
  std::string cql_value() const { return value_.cql_value(); }

  /**
   * Comparison operation for Comparable template
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const NullableValue<T>& rhs) const {
    // Ensure the value is not NULL
    if (is_null_ && rhs.is_null_) {
      return 0;
    } else if (is_null_) {
      return -1;
    } else if (rhs.is_null_) {
      return 1;
    }

    // Compare the value
    return value_.compare(rhs.value_);
  }

  /**
   * Initialize the wrapped value from the CassValue
   *
   * @param value CassValue to initialize wrapped value from
   */
  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Value should not be null";
    CassValueType expected_value_type = value_.value_type();
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(expected_value_type, value_type) << "Invalid value type";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(expected_value_type, value_type) << "Invalid data type";

    // Handle NULL cases or initialize the wrapped value
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      value_.initialize(value);
    }
  }

  /**
   * Determine if the value is NULL (or unassigned)
   *
   * @return True if value is NULL; false otherwise
   */
  bool is_null() const { return is_null_; }

  /**
   * Get the maximum value
   *
   * @return Maximum value
   */
  static NullableValue<T> max() { return NullableValue<T>(T::max()); }

  /**
   * Get the minimum value
   *
   * @return Minimum value
   */
  static NullableValue<T> min() { return NullableValue<T>(T::min()); }

  /**
   * Set the value to a index in the tuple
   *
   * @param tuple Tuple to apply to the value to
   * @param index Index to place the value in the tuple
   */
  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      value_.set(tuple, index);
    }
  }

  /**
   * Set the value to a field in the user type
   *
   * @param user_type User type to apply to the value to
   * @param name Name of the field in the user type
   */
  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      value_.set(user_type, name);
    }
  }

  /**
   * Bind the value to a statement at the given index
   *
   * @param statement The statement to bind the value to
   * @param index The index/position where the value will be bound in the
   *              statement
   */
  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      value_.statement_bind(statement, index);
    }
  }

  /**
   * Bind the value to a statement at the given column name
   *
   * @param statement The statement to bind the value to
   * @param name The column name where the value will be bound in the statement
   */
  void statement_bind(Statement statement, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null_by_name(statement.get(), name.c_str()));
    } else {
      value_.statement_bind(statement, name);
    }
  }

  /**
   * Convert the value to a standard string
   *
   * @return Standard string representation
   */
  std::string str() const {
    if (is_null_) return "null";
    return value_.str();
  }

  /**
   * The minimum supported version of the server that the value can be used
   * with.
   *
   * @return Minimum server version allowed for value
   */
  static std::string supported_server_version() { return T::supported_server_version(); }

  /**
   * Get the driver value
   *
   * @return Driver value
   */
  typename T::ValueType value() const { return value_.value(); }

  /**
   * Get the wrapped value
   *
   * @return Wrapped value type (templated type)
   */
  T wrapped_value() const { return value_; }

  /**
   * Get the type of value the native driver value is
   *
   * @return Value type of the native driver value
   */
  CassValueType value_type() const { return value_.value_type(); }

  NullableValue<T> operator-(const NullableValue<T>& rhs) {
    return NullableValue<T>(value_ - rhs.value_);
  }

  NullableValue<T> operator+(const NullableValue<T>& rhs) {
    return NullableValue<T>(value_ + rhs.value_);
  }

protected:
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;
  /**
   * Wrapped value
   */
  T value_;
};

template <typename T>
inline std::ostream& operator<<(std::ostream& output_stream, const NullableValue<T>& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}} // namespace test::driver

#endif // __TEST_NULLBALE_VALUE_HPP__
