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
#ifndef __TEST_VARCHAR_HPP__
#define __TEST_VARCHAR_HPP__
#include "value_interface.hpp"

namespace test {
namespace driver {

/**
 * Varchar wrapped value
 */
class Varchar : public COMPARABLE_VALUE_INTERFACE(std::string, Varchar) {
public:
  Varchar()
    : varchar_("")
    , is_null_(true) {
    update_value_if_null();
  }

  Varchar(const std::string& varchar)
    : varchar_(varchar)
    , is_null_(false) {

    // Determine if the value is NULL
    if (varchar.compare("null") == 0) {
      is_null_ = true;
    }

    update_value_if_null();
  }

  Varchar(const CassValue* value)
    : varchar_("")
    , is_null_(false) {
    initialize(value);
    update_value_if_null();
  }

  Varchar(const CassRow* row, size_t column_index)
    : varchar_("")
    , is_null_(false) {
    initialize(row, column_index);
    update_value_if_null();
  }

  virtual const char* c_str() const {
    return varchar_.c_str();
  }

  virtual std::string cql_type() const {
    return std::string("varchar");
  }

  virtual std::string cql_value() const {
    if (is_null_) return varchar_;
    return "'" + varchar_ + "'";
  }

  /**
   * Comparison operation for driver string
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  virtual int compare(const std::string& rhs) const {
    return varchar_.compare(rhs.c_str());
  }

  /**
   * Comparison operation for Varchar
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  virtual int compare(const Varchar& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.varchar_);
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_string(statement.get(), index, varchar_.c_str()));
    }
  }

  virtual bool is_null() const {
    return is_null_;
  }

  virtual std::string str() const {
    return varchar_;
  }

  virtual std::string value() const {
    return varchar_;
  }

  virtual CassValueType value_type() const {
    return CASS_VALUE_TYPE_VARCHAR;
  }

  /**
   * Update the value if value is NULL; sets native driver value appropriately
   */
  virtual void update_value_if_null() {
    if (is_null_) {
      varchar_ = "null";
    }
  }

protected:
  /**
   * Native driver value
   */
  std::string varchar_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  virtual void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_VARCHAR, value_type)
      << "Invalid Value Type: Value is not a Varchar [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_VARCHAR, value_type)
      << "Invalid Data Type: Value->DataType is not a Varchar";

    // Get the Varchar
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      const char* string = NULL;
      size_t length;
      ASSERT_EQ(CASS_OK, cass_value_get_string(value, &string, &length))
        << "Unable to Get Varchar: Invalid error code returned";
      varchar_ = std::string(string, length);
      is_null_ = false;
    }
  }

  virtual void initialize(const CassRow* row, size_t column_index) {
    ASSERT_TRUE(row != NULL) << "Invalid Row: Row should not be null";
    initialize(cass_row_get_column(row, column_index));
  }
};

/**
 * Text wrapped value
 */
class Text : public Varchar {
public:
  Text(const std::string& text)
    : Varchar(text) {}

  Text(const char* text)
    : Varchar(text) {}

  Text(const CassValue* value)
    : Varchar(value) {}

  Text(Varchar varchar)
    : Varchar(varchar) {}

  Text(const CassRow* row, size_t column_index)
    : Varchar(row, column_index) {}

  std::string cql_type() const {
    return std::string("text");
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_TEXT;
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_VARCHAR_HPP__
