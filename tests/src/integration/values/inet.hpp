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

#ifndef __TEST_INET_HPP__
#define __TEST_INET_HPP__
#include "nullable_value.hpp"
#include "test_utils.hpp"

namespace test { namespace driver { namespace values {

/**
 * Inet wrapped value
 */
class Inet {
public:
  typedef std::string ConvenienceType;
  typedef CassInet ValueType;

  Inet() {}

  Inet(const ConvenienceType& address) {
    std::string address_trim = Utils::trim(address);

    // Determine if the value is valid
    CassError error_code = cass_inet_from_string(address_trim.c_str(), &inet_);
    if (error_code != CASS_OK) {
      EXPECT_TRUE(false) << "Invalid Inet " << address_trim << ": Value will be NULL";
    }
  }

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_inet(collection.get(), inet_));
  }

  std::string cql_type() const { return "inet"; }

  std::string cql_value() const { return str(); }

  /**
   * Comparison operation for driver addresses
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const CassInet& rhs) const {
    if (inet_.address_length < rhs.address_length) return -1;
    if (inet_.address_length > rhs.address_length) return 1;

    return memcmp(inet_.address, rhs.address, inet_.address_length);
  }

  /**
   * Comparison operation for driver inet
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Inet& rhs) const { return compare(rhs.inet_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_inet(value, &inet_))
        << "Unable to Get Inet: Invalid error code returned";
  }

  static Inet max() {
    Inet inet;
    inet.inet_.address_length = CASS_INET_V6_LENGTH;
    memset(inet.inet_.address, 0xFF, sizeof(inet.inet_.address));
    return inet;
  }

  static Inet min() {
    Inet inet;
    inet.inet_.address_length = CASS_INET_V6_LENGTH;
    memset(inet.inet_.address, 0x0, sizeof(inet.inet_.address));
    return inet;
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_inet(tuple.get(), index, inet_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_inet_by_name(user_type.get(), name.c_str(), inet_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_inet(statement.get(), index, inet_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_inet_by_name(statement.get(), name.c_str(), inet_));
  }

  std::string str() const {
    char inet_string[CASS_INET_STRING_LENGTH];
    cass_inet_string(inet_, inet_string);
    return inet_string;
  }

  static std::string supported_server_version() { return "1.2.0"; }

  ValueType value() const { return inet_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_INET; }

protected:
  /**
   * Native driver value
   */
  CassInet inet_;
};

inline std::ostream& operator<<(std::ostream& output_stream, const Inet& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_INET_HPP__
