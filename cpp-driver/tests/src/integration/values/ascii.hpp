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

#ifndef __TEST_ASCII_HPP__
#define __TEST_ASCII_HPP__
#include "nullable_value.hpp"

namespace test { namespace driver { namespace values {

/**
 * ASCII wrapped value
 */
class Ascii {
public:
  typedef std::string ConvenienceType;
  typedef std::string ValueType;

  Ascii() {}

  Ascii(const ConvenienceType& ascii)
      : ascii_(ascii) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_string(collection.get(), ascii_.c_str()));
  }

  std::string cql_type() const { return "ascii"; }

  std::string cql_value() const { return "'" + str() + "'"; }

  /**
   * Comparison operation for driver string
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const std::string& rhs) const { return ascii_.compare(rhs); }

  /**
   * Comparison operation for ASCII
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Ascii& rhs) const { return compare(rhs.ascii_); }

  void initialize(const CassValue* value) {
    const char* string = NULL;
    size_t length;
    ASSERT_EQ(CASS_OK, cass_value_get_string(value, &string, &length))
        << "Unable to Get ASCII: Invalid error code returned";
    ascii_.assign(string, length);
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_string(tuple.get(), index, ascii_.c_str()));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK,
              cass_user_type_set_string_by_name(user_type.get(), name.c_str(), ascii_.c_str()));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_string(statement.get(), index, ascii_.c_str()));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK,
              cass_statement_bind_string_by_name(statement.get(), name.c_str(), ascii_.c_str()));
  }

  std::string str() const { return ascii_; }

  static std::string supported_server_version() { return "1.2.0"; }

  ValueType value() const { return ascii_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_ASCII; }

protected:
  /**
   * Native driver value
   */
  std::string ascii_;
};

inline std::ostream& operator<<(std::ostream& output_stream, const Ascii& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_ASCII_HPP__
