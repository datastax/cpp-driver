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
    , is_null_(true) {}

  Boolean(cass_bool_t boolean)
    : boolean_(boolean)
    , is_null_(false) {}

  Boolean(const CassValue* value)
    : boolean_(cass_false)
    , is_null_(false) {
    initialize(value);
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
      TEST_LOG_ERROR("Invalid Boolean " << value_trim << ": Using default "
        << (boolean_ == cass_true ? "true" : "false"));
    }
  }

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK,
      cass_collection_append_bool(collection.get(), boolean_));
  }

  std::string cql_type() const {
    return std::string("boolean");
  }

  std::string cql_value() const {
    return str();
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

  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_tuple_set_bool(tuple.get(), index, boolean_));
    }
  }

  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_bool_by_name(user_type.get(), name.c_str(), boolean_));
    }
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
    if (is_null_) {
      return "null";
    } else {
      return (boolean_ == cass_true ? "true" : "false");
    }
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
};

} // namespace driver
} // namespace test

#endif // __TEST_BOOLEAN_HPP__
