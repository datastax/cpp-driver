/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_BOOLEAN_HPP__
#define __TEST_BOOLEAN_HPP__
#include "value_interface.hpp"
#include "test_utils.hpp"

namespace test {
namespace driver {

/**
 * Boolean wrapped value
 */
class Boolean : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(cass_bool_t, Boolean) {
public:
  Boolean()
    : boolean_(cass_false)
    , is_null_(true) {
    set_boolean_string();
  }

  Boolean(cass_bool_t boolean)
    : boolean_(boolean)
    , is_null_(false) {
    set_boolean_string();
  }

  Boolean(const CassValue* value)
    : boolean_(cass_false)
    , is_null_(false) {
    initialize(value);
    set_boolean_string();
  }

  Boolean(const std::string& value)
    : boolean_(cass_false)
    , is_null_(false) {
    std::string value_trim = Utils::trim(Utils::to_lower(value));

    // Determine if the value is NULL, true, or false (default is false otherwise)
    if (value_trim.empty() ||
        value_trim.compare("null") == 0) {
      is_null_ = true;
    } else if (value_trim.compare("true") == 0 ||
               value_trim.compare("yes") == 0 ||
               value_trim.compare("1") == 0) {
      boolean_ = cass_true;
    } else if (value_trim.compare("false") == 0 ||
               value_trim.compare("no") == 0 ||
               value_trim.compare("0") == 0) {
    } else {
      LOG_ERROR("Invalid Boolean " << value_trim << ": Using default "
        << (boolean_ == cass_true ? "true" : "false"));
    }

    set_boolean_string();
  }

  Boolean(const CassRow* row, size_t column_index)
    : boolean_(cass_false)
    , is_null_(false) {
    initialize(row, column_index);
    set_boolean_string();
  }

  const char* c_str() const {
    return boolean_string_.c_str();
  }

  std::string cql_type() const {
    return std::string("boolean");
  }

  std::string cql_value() const {
    return boolean_string_;
  }

  /**
   * Comparison operation for driver booleans
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_bool_t& rhs) const {
    if (boolean_ < rhs) return -1;
    if (boolean_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver booleans
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Boolean& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.boolean_);
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_bool(statement.get(), index, boolean_));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  std::string str() const {
    return boolean_string_;
  }

  cass_bool_t value() const {
    return boolean_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_BOOLEAN;
  }

protected:
  /**
   * Native driver value
   */
  cass_bool_t boolean_;
  /**
   * Native driver value as string
   */
  std::string boolean_string_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_BOOLEAN, value_type)
      << "Invalid Value Type: Value is not a boolean [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_BOOLEAN, value_type)
      << "Invalid Data Type: Value->DataType is not a boolean";

    // Get the boolean
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_bool(value, &boolean_))
        << "Unable to Get Boolean: Invalid error code returned";
      is_null_ = false;
    }
  }

  void initialize(const CassRow* row, size_t column_index) {
    ASSERT_TRUE(row != NULL) << "Invalid Row: Row should not be null";
    initialize(cass_row_get_column(row, column_index));
  }

  /**
   * Set the string value of the boolean
   */
  void set_boolean_string() {
    if (is_null_) {
      boolean_string_ = "null";
    } else {
      boolean_string_ = (boolean_ == cass_true ? "true" : "false");
    }
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_BOOLEAN_HPP__
