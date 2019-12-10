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

#ifndef __TEST_VARINT_HPP__
#define __TEST_VARINT_HPP__
#include "nullable_value.hpp"

#include "bignumber.hpp"

namespace test { namespace driver { namespace values {

/**
 * Varint wrapped value
 */
class Varint {
public:
  typedef std::string ConvenienceType;
  typedef BigNumber ValueType;

  Varint() {}

  Varint(const ConvenienceType& varint)
      : varint_(varint) {}

  void append(Collection collection) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK, cass_collection_append_bytes(collection.get(), bytes.data(), bytes.size()));
  }

  std::string cql_type() const { return "varint"; }

  std::string cql_value() const { return "'" + str() + "'"; }

  /**
   * Comparison operation for driver bytes
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const BigNumber& rhs) const { return varint_.compare(rhs); }

  /**
   * Comparison operation for varint
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Varint& rhs) const { return compare(rhs.varint_); }

  void initialize(const CassValue* value) {
    const cass_byte_t* bytes = NULL;
    size_t size;
    ASSERT_EQ(CASS_OK, cass_value_get_bytes(value, &bytes, &size))
        << "Unable to Get Varint: Invalid error code returned";
    varint_ = BigNumber(bytes, size, 0);
  }

  void set(Tuple tuple, size_t index) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK, cass_tuple_set_bytes(tuple.get(), index, bytes.data(), bytes.size()));
  }

  void set(UserType user_type, const std::string& name) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK, cass_user_type_set_bytes_by_name(user_type.get(), name.c_str(), bytes.data(),
                                                        bytes.size()));
  }

  void statement_bind(Statement statement, size_t index) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK,
              cass_statement_bind_bytes(statement.get(), index, bytes.data(), bytes.size()));
  }

  void statement_bind(Statement statement, const std::string& name) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK, cass_statement_bind_bytes_by_name(statement.get(), name.c_str(),
                                                         bytes.data(), bytes.size()));
  }

  std::string str() const { return varint_.str(); }

  static std::string supported_server_version() { return "1.2.0"; }

  ValueType value() const { return varint_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_VARINT; }

protected:
  /**
   * Native driver value (wrapped in BigNumber implementation)
   */
  BigNumber varint_;
};

inline std::ostream& operator<<(std::ostream& output_stream, const Varint& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_VARINT_HPP__
