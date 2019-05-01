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

#ifndef __TEST_DOUBLE_HPP__
#define __TEST_DOUBLE_HPP__
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
 * Double wrapped value
 */
class Double {
public:
  typedef cass_double_t ConvenienceType;
  typedef cass_double_t ValueType;

  Double()
      : double_(0.0) {}

  Double(const ConvenienceType double_value)
      : double_(double_value) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_double(collection.get(), double_));
  }

  std::string cql_type() const { return "double"; }

  std::string cql_value() const { return str(); }

  /**
   * Comparison operation for driver doubles
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_double_t& rhs) const {
    if (double_ < rhs) return -1;
    if (double_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver doubles
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Double& rhs) const { return compare(rhs.double_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_double(value, &double_))
        << "Unable to Get Double: Invalid error code returned";
  }

  static Double max() { return Double(std::numeric_limits<cass_double_t>::max()); }

  static Double min() { return Double(std::numeric_limits<cass_double_t>::min()); }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_double(tuple.get(), index, double_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_double_by_name(user_type.get(), name.c_str(), double_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_double(statement.get(), index, double_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_double_by_name(statement.get(), name.c_str(), double_));
  }

  std::string str() const {
    std::stringstream double_string;
    double_string << double_;
    return double_string.str();
  }

  static std::string supported_server_version() { return "1.2.0"; }

  cass_double_t value() const { return double_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_DOUBLE; }

protected:
  /**
   * Native driver value
   */
  cass_double_t double_;
};

}}} // namespace test::driver::values

#endif // __TEST_DOUBLE_HPP__
