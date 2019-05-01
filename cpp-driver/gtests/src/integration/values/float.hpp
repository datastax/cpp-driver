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

#ifndef __TEST_FLOAT_HPP__
#define __TEST_FLOAT_HPP__
#include "nullable_value.hpp"
#include "test_utils.hpp"

#include <limits>
#include <sstream>

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace test { namespace driver { namespace values {

/**
 * Float wrapped value
 */
class Float {
public:
  typedef cass_float_t ConvenienceType;
  typedef cass_float_t ValueType;

  Float()
      : float_(0.0f) {}

  Float(const ConvenienceType float_value)
      : float_(float_value) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_float(collection.get(), float_));
  }

  std::string cql_type() const { return "float"; }

  std::string cql_value() const { return str(); }

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
  int compare(const Float& rhs) const { return compare(rhs.float_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_float(value, &float_))
        << "Unable to Get Float: Invalid error code returned";
  }

  static Float max() { return Float(std::numeric_limits<cass_float_t>::max()); }

  static Float min() { return Float(std::numeric_limits<cass_float_t>::min()); }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_float(tuple.get(), index, float_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_float_by_name(user_type.get(), name.c_str(), float_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_float(statement.get(), index, float_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_float_by_name(statement.get(), name.c_str(), float_));
  }

  std::string str() const {
    std::stringstream float_string;
    float_string << float_;
    return float_string.str();
  }

  static std::string supported_server_version() { return "1.2.0"; }

  ValueType value() const { return float_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_FLOAT; }

protected:
  /**
   * Native driver value
   */
  cass_float_t float_;
};

}}} // namespace test::driver::values

#endif // __TEST_FLOAT_HPP__
