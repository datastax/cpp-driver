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

#ifndef __TEST_VARCHAR_HPP__
#define __TEST_VARCHAR_HPP__
#include "nullable_value.hpp"

namespace test { namespace driver { namespace values {

/**
 * Varchar wrapped value
 */
class Varchar {
public:
  typedef std::string ConvenienceType;
  typedef std::string ValueType;

  Varchar() {}

  Varchar(const ConvenienceType& varchar)
      : varchar_(varchar) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_string(collection.get(), varchar_.c_str()));
  }

  std::string cql_type() const { return "varchar"; }

  std::string cql_value() const { return "'" + str() + "'"; }

  /**
   * Comparison operation for driver string
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const std::string& rhs) const { return varchar_.compare(rhs.c_str()); }

  /**
   * Comparison operation for Varchar
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Varchar& rhs) const { return compare(rhs.varchar_); }

  void initialize(const CassValue* value) {
    const char* string = NULL;
    size_t length;
    ASSERT_EQ(CASS_OK, cass_value_get_string(value, &string, &length))
        << "Unable to Get Varchar: Invalid error code returned";
    varchar_.assign(string, length);
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_string(tuple.get(), index, varchar_.c_str()));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK,
              cass_user_type_set_string_by_name(user_type.get(), name.c_str(), varchar_.c_str()));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_string(statement.get(), index, varchar_.c_str()));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK,
              cass_statement_bind_string_by_name(statement.get(), name.c_str(), varchar_.c_str()));
  }

  std::string str() const { return varchar_; }

  static std::string supported_server_version() { return "1.2.0"; }

  ValueType value() const { return varchar_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_VARCHAR; }

protected:
  /**
   * Native driver value
   */
  std::string varchar_;
};

/**
 * Text wrapped value
 */
class Text : public Varchar {
public:
  Text() {}

  Text(const ConvenienceType& text)
      : Varchar(text) {}

  std::string cql_type() const { return "text"; }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_VARCHAR; // Text (alias is returned as varchar from server)
  }
};

}}} // namespace test::driver::values

#endif // __TEST_VARCHAR_HPP__
