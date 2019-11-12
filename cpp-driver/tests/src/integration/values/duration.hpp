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

#ifndef __TEST_DURATION_HPP__
#define __TEST_DURATION_HPP__
#include "nullable_value.hpp"

namespace test { namespace driver {

/**
 * Simple structure making up the months, days, and nanos for the duration
 * value
 */
struct CassDuration {
  CassDuration()
      : months(0)
      , days(0)
      , nanos(0) {}
  CassDuration(cass_int32_t months, cass_int32_t days, cass_int64_t nanos)
      : months(months)
      , days(days)
      , nanos(nanos) {}
  cass_int32_t months;
  cass_int32_t days;
  cass_int64_t nanos;
};

namespace values {

/**
 * Duration wrapped value
 */
class Duration {
public:
  typedef CassDuration ConvenienceType;
  typedef CassDuration ValueType;

  Duration() {}

  Duration(const ConvenienceType duration)
      : duration_(duration) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_duration(collection.get(), duration_.months,
                                                       duration_.days, duration_.nanos));
  }

  std::string cql_type() const { return "duration"; }

  std::string cql_value() const { return "'" + str() + "'"; }

  /**
   * Comparison operation for driver duration integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const CassDuration& rhs) const {
    if (duration_.months < rhs.months) return -1;
    if (duration_.months > rhs.months) return 1;
    if (duration_.days < rhs.days) return -1;
    if (duration_.days > rhs.days) return 1;
    if (duration_.nanos < rhs.nanos) return -1;
    if (duration_.nanos > rhs.nanos) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver duration
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Duration& rhs) const { return compare(rhs.duration_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK,
              cass_value_get_duration(value, &duration_.months, &duration_.days, &duration_.nanos))
        << "Unable to Get Duration: Invalid error code returned";
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_duration(tuple.get(), index, duration_.months, duration_.days,
                                               duration_.nanos));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK,
              cass_user_type_set_duration_by_name(user_type.get(), name.c_str(), duration_.months,
                                                  duration_.days, duration_.nanos));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_duration(statement.get(), index, duration_.months,
                                                    duration_.days, duration_.nanos));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK,
              cass_statement_bind_duration_by_name(statement.get(), name.c_str(), duration_.months,
                                                   duration_.days, duration_.nanos));
  }

  std::string str() const {
    // Determine if any portion of the duration are negative
    bool is_negative = false;
    is_negative = (duration_.months < 0 || duration_.days < 0 || duration_.nanos < 0);

    // Convert the duration to a human readable format
    cass_int32_t months = duration_.months < 0 ? -duration_.months : duration_.months;
    cass_int32_t days = duration_.days < 0 ? -duration_.days : duration_.days;
    cass_int64_t nanos = duration_.nanos < 0 ? -duration_.nanos : duration_.nanos;
    std::ostringstream duration_string;
    duration_string << (is_negative ? "-1" : "") << months << "mo" << days << "d" << nanos << "ns";

    return duration_string.str();
  }

  static std::string supported_server_version() { return "3.10"; }

  ValueType value() const { return duration_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_DURATION; }

protected:
  /**
   * Native driver value
   */
  CassDuration duration_;
};

inline std::ostream& operator<<(std::ostream& output_stream, const Duration& value) {
  output_stream << value.cql_value();
  return output_stream;
}

} // namespace values
}} // namespace test::driver

#endif // __TEST_DURATION_HPP__
