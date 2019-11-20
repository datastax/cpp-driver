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

#ifndef __TEST_TIMESTAMP_HPP__
#define __TEST_TIMESTAMP_HPP__
#include "integer.hpp"

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace test { namespace driver { namespace values {

/**
 * Timestamp wrapped value
 */
class Timestamp : public BigInteger {
public:
  using BigInteger::compare;

  Timestamp() {}

  Timestamp(const ConvenienceType integer)
      : BigInteger(integer) {}

  std::string cql_type() const { return "timestamp"; }

  /**
   * Comparison operation for driver time
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Timestamp& rhs) const { return compare(rhs.integer_); }

  static Timestamp max() { return Timestamp(std::numeric_limits<cass_int64_t>::max()); }

  static Timestamp min() { return Timestamp(std::numeric_limits<cass_int64_t>::min()); }

  CassValueType value_type() const { return CASS_VALUE_TYPE_TIMESTAMP; }
};

inline std::ostream& operator<<(std::ostream& output_stream, const Timestamp& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_TIMESTAMP_HPP__
