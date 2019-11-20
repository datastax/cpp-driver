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

#ifndef __TEST_DATE_HPP__
#define __TEST_DATE_HPP__
#include "nullable_value.hpp"

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace test { namespace driver { namespace values {

/**
 * Date wrapped value
 */
class Date {
public:
  typedef cass_uint32_t ConvenienceType;
  typedef cass_uint32_t ValueType;

  Date()
      : date_(0) {}

  Date(const ConvenienceType date)
      : date_(date) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_uint32(collection.get(), date_));
  }

  std::string cql_type() const { return "date"; }

  std::string cql_value() const { return "'" + str() + "'"; }

  /**
   * Comparison operation for driver unsigned integers (e.g. date)
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_uint32_t& rhs) const {
    if (date_ < rhs) return -1;
    if (date_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver date
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Date& rhs) const { return compare(rhs.date_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_uint32(value, &date_))
        << "Unable to Get Date: Invalid error code returned";
  }

  static Date max() {
    return Date(2147533357u); // Maximum value supported by strftime()
  }

  static Date min() {
    return Date(2147483648u); // Minimum value supported by strftime()
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_uint32(tuple.get(), index, date_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_uint32_by_name(user_type.get(), name.c_str(), date_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_uint32(statement.get(), index, date_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_uint32_by_name(statement.get(), name.c_str(), date_));
  }

  std::string str() const {
    // Convert the date to a human readable format
    char date_string[32];
    time_t epoch_secs = static_cast<time_t>(cass_date_time_to_epoch(date_, 0));
    strftime(date_string, sizeof(date_string), "%Y-%m-%d", gmtime(&epoch_secs));
    return date_string;
  }

  static std::string supported_server_version() { return "2.2.3"; }

  cass_uint32_t value() const { return date_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_DATE; }

protected:
  /**
   * Native driver value
   */
  cass_uint32_t date_;
};

inline std::ostream& operator<<(std::ostream& output_stream, const Date& value) {
  // Output both values
  output_stream << value.cql_value() << " [ = " << value.value() << "]";
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_DATE_HPP__
