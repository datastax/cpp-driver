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
#ifndef __TEST_INTEGER_HPP__
#define __TEST_INTEGER_HPP__
#include "value_interface.hpp"

#include <sstream>

namespace test {
namespace driver {

/**
 * Integer wrapped value
 */
class Integer : public COMPARABLE_VALUE_INTERFACE_OBJECT_ONLY(cass_int32_t, Integer) {
public:
  Integer(cass_int32_t integer)
    : integer_(integer) {
    set_integer_string();
  }

  Integer(const CassValue* value) {
    initialize(value);
    set_integer_string();
  }

  Integer(const std::string& value) {
    //Convert the value
    if (!value.empty()) {
      std::stringstream valueStream(value);
      if ((valueStream >> integer_).fail()) {
        integer_ = 0;
        std::cerr << "Invalid Integer: Using default [" << integer_ << "]";
      }
    }
    set_integer_string();
  }

  Integer(const CassRow* row, size_t column_index) {
    initialize(row, column_index);
    set_integer_string();
  }

  const char* c_str() const {
    return integer_string_.c_str();
  }

  std::string cql_type() const {
    return std::string("int");
  }

  std::string cql_value() const {
    return integer_string_;
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_int32_t& rhs) const {
    if (integer_ < rhs) return -1;
    if (integer_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Integer& rhs) const {
    return compare(rhs.integer_);
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_int32(statement.get(), index, integer_));
  }

  std::string str() const {
    return integer_string_;
  }

  cass_int32_t value() const {
    return integer_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_INT;
  }

protected:
  /**
   * Native driver value
   */
  cass_int32_t integer_;
  /**
   * Native driver value as string
   */
  std::string integer_string_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_INT, value_type)
      << "Invalid Value Type: Value is not a integer [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_INT, value_type)
      << "Invalid Data Type: Value->DataType is not a integer";

    // Get the 32-bit integer
    ASSERT_EQ(CASS_OK, cass_value_get_int32(value, &integer_))
      << "Unable to Get 32-bit Integer: Invalid error code returned";
  }

  void initialize(const CassRow* row, size_t column_index) {
    ASSERT_TRUE(row != NULL) << "Invalid Row: Row should not be null";
    initialize(cass_row_get_column(row, column_index));
  }

  /**
   * Set the string value of the integer
   */
  void set_integer_string() {
    std::stringstream integer_string;
    integer_string << integer_;
    integer_string_ = integer_string.str();
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_INTEGER_HPP__
