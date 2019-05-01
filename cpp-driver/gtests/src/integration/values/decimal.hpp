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

#ifndef __TEST_DECIMAL_HPP__
#define __TEST_DECIMAL_HPP__
#include "varint.hpp"

namespace test { namespace driver { namespace values {

/**
 * Decimal wrapped value
 */
class Decimal : public Varint {
public:
  using Varint::compare;

  Decimal() {}

  Decimal(const ConvenienceType& decimal)
      : Varint(decimal) {}

  void append(Collection collection) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK, cass_collection_append_decimal(collection.get(), bytes.data(), bytes.size(),
                                                      varint_.scale()));
  }

  std::string cql_type() const { return "decimal"; }

  /**
   * Comparison operation for varint
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Decimal& rhs) const { return Varint::compare(rhs.varint_); }

  void initialize(const CassValue* value) {
    const cass_byte_t* bytes = NULL;
    size_t size;
    cass_int32_t scale;
    ASSERT_EQ(CASS_OK, cass_value_get_decimal(value, &bytes, &size, &scale))
        << "Unable to Get Decimal: Invalid error code returned";
    varint_ = BigNumber(bytes, size, scale);
  }

  void set(Tuple tuple, size_t index) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK, cass_tuple_set_decimal(tuple.get(), index, bytes.data(), bytes.size(),
                                              varint_.scale()));
  }

  void set(UserType user_type, const std::string& name) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK,
              cass_user_type_set_decimal_by_name(user_type.get(), name.c_str(), bytes.data(),
                                                 bytes.size(), varint_.scale()));
  }

  void statement_bind(Statement statement, size_t index) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK, cass_statement_bind_decimal(statement.get(), index, bytes.data(),
                                                   bytes.size(), varint_.scale()));
  }

  void statement_bind(Statement statement, const std::string& name) {
    const std::vector<cass_byte_t>& bytes = varint_.encode_varint();
    ASSERT_EQ(CASS_OK,
              cass_statement_bind_decimal_by_name(statement.get(), name.c_str(), bytes.data(),
                                                  bytes.size(), varint_.scale()));
  }

  CassValueType value_type() const { return CASS_VALUE_TYPE_DECIMAL; }
};

inline std::ostream& operator<<(std::ostream& output_stream, const Decimal& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_DECIMAL_HPP__
