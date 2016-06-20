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
#ifndef __TEST_FLOAT_HPP__
#define __TEST_FLOAT_HPP__
#include "value_interface.hpp"
#include "test_utils.hpp"

#include <limits>
#include <sstream>

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace test {
namespace driver {

/**
 * Float wrapped value
 */
class Float : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(cass_float_t, Float) {
public:
  Float()
    : float_(0.0f)
    , is_null_(true) {
    set_float_string();
  }

  Float(cass_float_t float_value)
    : float_(float_value)
    , is_null_(false) {
    set_float_string();
  }

  Float(const CassValue* value)
    : float_(0.0f)
    , is_null_(false) {
    initialize(value);
    set_float_string();
  }

  Float(const std::string& value)
    : float_(0.0f)
    , is_null_(false) {
    std::string value_trim = Utils::trim(value);

    // Determine if the value is NULL or valid (default is 0.0 otherwise)
    if (value_trim.empty() ||
        value_trim.compare("null") == 0) {
      is_null_ = true;
    } else {
      //Convert the value
      std::stringstream valueStream(value_trim);
      if ((valueStream >> float_).fail()) {
        LOG_ERROR("Invalid Float " << value_trim << ": Using default "
          << float_);
      }
    }

    set_float_string();
  }

  Float(const CassRow* row, size_t column_index)
    : float_(0.0f)
    , is_null_(false) {
    initialize(row, column_index);
    set_float_string();
  }

  const char* c_str() const {
    return float_string_.c_str();
  }

  std::string cql_type() const {
    return std::string("float");
  }

  std::string cql_value() const {
    return float_string_;
  }

  /**
   * Comparison operation for driver floats
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_float_t& rhs) const {
    if (float_ < rhs) return -1;
    if (float_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver floats
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Float& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.float_);
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_float(statement.get(), index, float_));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  /**
   * Get the minimum value allowed for a float
   *
   * @return Minimum value for float
   */
  static Float min() {
    return Float(std::numeric_limits<cass_float_t>::min());
  }

  /**
   * Get the maximum value allowed for a float
   *
   * @return Maximum value for float
   */
  static Float max() {
    return Float(std::numeric_limits<cass_float_t>::max());
  }

  std::string str() const {
    return float_string_;
  }

  cass_float_t value() const {
    return float_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_FLOAT;
  }

protected:
  /**
   * Native driver value
   */
  cass_float_t float_;
  /**
   * Native driver value as string
   */
  std::string float_string_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_FLOAT, value_type)
      << "Invalid Value Type: Value is not a float [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_FLOAT, value_type)
      << "Invalid Data Type: Value->DataType is not a float";

    // Get the float
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_float(value, &float_))
        << "Unable to Get Float: Invalid error code returned";
      is_null_ = false;
    }
  }

  void initialize(const CassRow* row, size_t column_index) {
    ASSERT_TRUE(row != NULL) << "Invalid Row: Row should not be null";
    initialize(cass_row_get_column(row, column_index));
  }

  /**
   * Set the string value of the float
   */
  void set_float_string() {
    if (is_null_) {
      float_string_ = "null";
    } else {
      std::stringstream float_string;
      float_string << float_;
      float_string_ = float_string.str();
    }
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_FLOAT_HPP__
