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

#ifndef __TEST_TIME_HPP__
#define __TEST_TIME_HPP__
#include "integer.hpp"

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace test { namespace driver { namespace values {

/**
 * Time wrapped value
 */
class Time : public BigInteger {
public:
  using BigInteger::compare;

  Time() {}

  Time(const ConvenienceType integer)
      : BigInteger(integer) {}

  std::string cql_type() const { return "time"; }

  std::string cql_value() const { return "'" + str() + "'"; }

  /**
   * Comparison operation for driver time
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Time& rhs) const { return compare(rhs.integer_); }

  static Time max() { return Time(86399999999999); }

  static Time min() { return Time(static_cast<cass_int64_t>(0)); }

  std::string str() const {
    // Convert the time to a human readable format
    char temp[32];
    time_t epoch_secs = static_cast<time_t>(cass_date_time_to_epoch(2147483648, integer_));
    struct tm* time = gmtime(&epoch_secs);
    strftime(temp, sizeof(temp), "%H:%M:%S", time);
    std::string time_string = temp;
    cass_int64_t diff = integer_ - epoch_secs * 1000000000;
    sprintf(temp, "%09u", (unsigned int)diff);
    time_string.append(".");
    time_string.append(temp);
    return time_string;
  }

  static std::string supported_server_version() { return "2.2.3"; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_TIME; }
};

inline std::ostream& operator<<(std::ostream& output_stream, const Time& value) {
  output_stream << value.cql_value() << " [ = " << value.value() << "]";
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_TIME_HPP__
