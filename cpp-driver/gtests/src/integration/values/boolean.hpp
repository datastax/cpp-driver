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
#include "nullable_value.hpp"
#include "test_utils.hpp"

namespace test { namespace driver { namespace values {

/**
 * Boolean wrapped value
 */
class Boolean {
public:
  typedef bool ConvenienceType;
  typedef cass_bool_t ValueType;

  Boolean()
      : boolean_(cass_false) {}

  Boolean(const ConvenienceType boolean)
      : boolean_(boolean ? cass_true : cass_false) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_bool(collection.get(), boolean_));
  }

  std::string cql_type() const { return "boolean"; }

  std::string cql_value() const { return str(); }

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
  int compare(const Boolean& rhs) const { return compare(rhs.boolean_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_bool(value, &boolean_))
        << "Unable to Get Boolean: Invalid error code returned";
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_bool(tuple.get(), index, boolean_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_bool_by_name(user_type.get(), name.c_str(), boolean_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_bool(statement.get(), index, boolean_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_bool_by_name(statement.get(), name.c_str(), boolean_));
  }

  std::string str() const { return (boolean_ == cass_true ? "true" : "false"); }

  static std::string supported_server_version() { return "1.2.0"; }

  ValueType value() const { return boolean_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_BOOLEAN; }

protected:
  /**
   * Native driver value
   */
  cass_bool_t boolean_;
};

inline std::ostream& operator<<(std::ostream& output_stream, const Boolean& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_BOOLEAN_HPP__
