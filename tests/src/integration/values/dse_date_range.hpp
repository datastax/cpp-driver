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

#ifndef __TEST_DSE_DATE_RANGE_HPP__
#define __TEST_DSE_DATE_RANGE_HPP__

#include "dse.h"

#include "exception.hpp"
#include "strptime.hpp"

#include <ctime>
#include <vector>

#ifdef _WIN32
#ifndef snprintf
#define snprintf _snprintf
#endif
#endif

namespace test { namespace driver { namespace values { namespace dse {

/**
 * Internal helper functions for handling date/time stuff
 */
namespace internal {

/**
 * Get the number of seconds difference for the current timezone. This is
 * useful for converting times to GMT after using `mktime()` which uses
 * the local machine's timezone.
 *
 * This is a portable version of the global var `::timezone` that's included
 * in some versions of <time.h>.
 *
 * @return The number of seconds for localtime()
 */
inline time_t timezone() {
  struct tm tm;
  memset(&tm, 0, sizeof(tm));
  // 01/01/1970
  tm.tm_year = 70;
  tm.tm_mday = 1;
  return mktime(&tm);
}

/**
 * Convert a time struct to milliseconds since the Epoch in GMT
 *
 * @param The time struct to convert
 * @return The number of milliseconds since the Epoch GMT
 */
inline cass_int64_t to_milliseconds(const struct tm* tm) {
  struct tm temp_tm = *tm;
  time_t t = mktime(&temp_tm);
  return (t - timezone()) * 1000; // Subtract the timezone to convert to GMT
}

/**
 * Is a leap year?
 *
 * @param The year
 * @return `true` if the provided year is a leap year, otherwise `false`
 */
inline bool is_leap_year(int year) {
  if (year % 400 == 0) return true;
  if (year % 100 == 0) return false;
  if (year % 4 == 0) return true;
  return false;
}

/**
 * Get the maximum number of days in a month of a specific year
 *
 * @param month The month [0 - 11] where January = 0 and December = 12
 * @param year The year e.g. 1970
 * @return The maximum number of days in the provided month
 */
inline int max_days_in_month(int month, int year) {
  // January = 0, February = 1, ..., December = 11
  const int days_in_month[] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  if (month == 1 && is_leap_year(year)) { // If it is February and a leap year then add a day
    return days_in_month[month] + 1;
  }
  return days_in_month[month];
}

} // namespace internal

/**
 * A wrapper around DSE date range bound
 */
class DateRangeBound : public DseDateRangeBound {
public:
  /**
   * Create a bound from milliseconds
   *
   * @param ms The number of milliseconds since the epoch
   */
  DateRangeBound(cass_int64_t ms) {
    this->precision = DSE_DATE_RANGE_PRECISION_MILLISECOND;
    this->time_ms = ms;
  }

  /**
   * A copy constructor to convert from the wrapped type.
   *
   * @param rhs
   */
  DateRangeBound(const DseDateRangeBound& rhs) {
    this->precision = rhs.precision;
    this->time_ms = rhs.time_ms;
  }

  /**
   * Create an unbounded date range bound
   *
   * @return An unbounded bound
   */
  static DateRangeBound unbounded() { return dse_date_range_bound_unbounded(); }

  /**
   * Create a lower bound given a precision and date/time string. This rounds the date/time
   * down to the nearest precision unit.
   *
   * @param precision The precision
   * @param str A date/time string e.g. "1970", "01/1970", "01:00:01 01/01/1970"
   * @return A lower date range bound rounded to the near precision unit.
   */
  static DateRangeBound lower(DseDateRangePrecision precision, const std::string& str) {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    from_string(precision, str, &tm);
    return to_lower(precision, &tm);
  }

  /**
   * Create an upper bound given a precision and date/time string. This rounds the date/time
   * down up to the nearest precision and subtracts 1 millisecond (so not inclusive).
   *
   * @param precision The precision
   * @param str A date/time string e.g. "1970", "01/1970", "01:00:01 01/01/1970"
   * @return An upper date range bound rounded to the near precision unit.
   */
  static DateRangeBound upper(DseDateRangePrecision precision, const std::string& str) {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    from_string(precision, str, &tm);
    return to_upper(precision, &tm);
  }

private:
  /**
   * Convert a date/time string to a time struct with the given precision
   *
   * @param precision The precision to consider the date/time string
   * @param str A date/time string e.g. "1970", "01/1970", "01:00:01 01/01/1970"
   * @param result The converted time struct
   *
   * @throw Exception if the format is valid for the provided precision
   */
  static void from_string(DseDateRangePrecision precision, const std::string& str,
                          struct tm* result) {
    switch (precision) {
      case DSE_DATE_RANGE_PRECISION_YEAR:
        if (test::strptime(str.c_str(), "%Y", result) == NULL) {
          throw Exception("Invalid string value for year format");
        }
        break;
      case DSE_DATE_RANGE_PRECISION_MONTH:
        if (test::strptime(str.c_str(), "%m/%Y", result) == NULL) {
          throw Exception("Invalid string value for month format");
        }
        break;
      case DSE_DATE_RANGE_PRECISION_DAY:
        if (test::strptime(str.c_str(), "%m/%d/%Y", result) == NULL) {
          throw Exception("Invalid string value for day format");
        }
        break;
      case DSE_DATE_RANGE_PRECISION_HOUR:
        if (test::strptime(str.c_str(), "%H:00 %m/%d/%Y", result) == NULL) {
          throw Exception("Invalid string value for hour format");
        }
        break;
      case DSE_DATE_RANGE_PRECISION_MINUTE:
        if (test::strptime(str.c_str(), "%H:%M %m/%d/%Y", result) == NULL) {
          throw Exception("Invalid string value for minute format");
        }
        break;
      case DSE_DATE_RANGE_PRECISION_SECOND:
        if (test::strptime(str.c_str(), "%H:%M:%S %m/%d/%Y", result) == NULL) {
          throw Exception("Invalid string value for second format");
        }
        break;
      case DSE_DATE_RANGE_PRECISION_MILLISECOND:
        throw Exception("Millisecond and unbounded are not supported");
      case DSE_DATE_RANGE_PRECISION_UNBOUNDED:
        break;
    }
  }

  /**
   * Create a bound by rounding a time struct down to the near precision unit.
   *
   * @param precision The precision of the lower bound
   * @param bound_tm The time struct to convert and round to the lower bound
   * @return A lower date range bound
   */
  static DateRangeBound to_lower(DseDateRangePrecision precision, const struct tm* bound_tm) {
    struct tm rounded_tm = *bound_tm;

    switch (precision) {
      case DSE_DATE_RANGE_PRECISION_YEAR:
        rounded_tm.tm_hour = 0;
        rounded_tm.tm_min = 0;
        rounded_tm.tm_sec = 0;
        rounded_tm.tm_mday = 1;
        rounded_tm.tm_mon = 0;
        break;
      case DSE_DATE_RANGE_PRECISION_MONTH:
        rounded_tm.tm_hour = 0;
        rounded_tm.tm_min = 0;
        rounded_tm.tm_sec = 0;
        rounded_tm.tm_mday = 1;
        break;
      case DSE_DATE_RANGE_PRECISION_DAY:
        rounded_tm.tm_hour = 0;
        rounded_tm.tm_min = 0;
        rounded_tm.tm_sec = 0;
        break;
      case DSE_DATE_RANGE_PRECISION_HOUR:
        rounded_tm.tm_min = 0;
        rounded_tm.tm_sec = 0;
        break;
      case DSE_DATE_RANGE_PRECISION_MINUTE:
        rounded_tm.tm_sec = 0;
        break;
      case DSE_DATE_RANGE_PRECISION_SECOND:
        break;
      case DSE_DATE_RANGE_PRECISION_MILLISECOND:
        break;
      case DSE_DATE_RANGE_PRECISION_UNBOUNDED:
        break;
    }

    return DateRangeBound(precision, internal::to_milliseconds(&rounded_tm));
  }

  /**
   * Create a bound by rounding a time struct up to the near precision unit
   *
   * @param precision The precision of the upper bound
   * @param bound_tm The time struct to convert and round to the upper bound
   * @return An upper date range bound
   */
  static DateRangeBound to_upper(DseDateRangePrecision precision, const struct tm* bound_tm) {
    struct tm rounded_tm = *bound_tm;

    switch (precision) {
      case DSE_DATE_RANGE_PRECISION_YEAR:
        rounded_tm.tm_hour = 23;
        rounded_tm.tm_min = 59;
        rounded_tm.tm_sec = 60;
        rounded_tm.tm_mday = 31; // December has 31 days
        rounded_tm.tm_mon = 11;
        break;
      case DSE_DATE_RANGE_PRECISION_MONTH:
        rounded_tm.tm_hour = 23;
        rounded_tm.tm_min = 59;
        rounded_tm.tm_sec = 60;
        rounded_tm.tm_mday =
            internal::max_days_in_month(rounded_tm.tm_mon, rounded_tm.tm_year + 1900);
        break;
      case DSE_DATE_RANGE_PRECISION_DAY:
        rounded_tm.tm_hour = 23;
        rounded_tm.tm_min = 59;
        rounded_tm.tm_sec = 60;
        break;
      case DSE_DATE_RANGE_PRECISION_HOUR:
        rounded_tm.tm_min = 59;
        rounded_tm.tm_sec = 60;
        break;
      case DSE_DATE_RANGE_PRECISION_MINUTE:
        rounded_tm.tm_sec = 60;
        break;
      case DSE_DATE_RANGE_PRECISION_SECOND:
        break;
      case DSE_DATE_RANGE_PRECISION_MILLISECOND:
        break;
      case DSE_DATE_RANGE_PRECISION_UNBOUNDED:
        break;
    }

    return DateRangeBound(precision, internal::to_milliseconds(&rounded_tm) - 1);
  }

private:
  /**
   * Create a date range bound from a precision and a timestamp
   *
   * @param precision
   * @param time_ms
   */
  DateRangeBound(DseDateRangePrecision precision, cass_int64_t time_ms) {
    this->precision = precision;
    this->time_ms = time_ms;
  }
};

/**
 * A comparison function for two date range bounds
 *
 * @param lhs
 * @param rhs
 * @return -1 if less than, 1 if greater than, otherwise 0
 */
inline int compare(const DateRangeBound& lhs, const DateRangeBound& rhs) {
  // Unbounded bounds have to be compared specially because the their `time_ms`
  // fields are meaningless.
  if (lhs.precision == DSE_DATE_RANGE_PRECISION_UNBOUNDED &&
      rhs.precision == DSE_DATE_RANGE_PRECISION_UNBOUNDED) {
    return 0;
  } else if (lhs.precision == DSE_DATE_RANGE_PRECISION_UNBOUNDED) {
    return -1;
  } else if (rhs.precision == DSE_DATE_RANGE_PRECISION_UNBOUNDED) {
    return 1;
  }
  if (lhs.time_ms != rhs.time_ms) {
    return lhs.time_ms < rhs.time_ms ? -1 : 1;
  }
  if (lhs.precision != rhs.precision) {
    return lhs.precision < rhs.precision ? -1 : 1;
  }
  return 0;
}

/**
 * Convert a date range bound to a string
 *
 * @param bound The bound to convert
 * @return A string representation of the date range bound
 */
inline std::string str(const DateRangeBound& bound) {
  if (bound.precision == DSE_DATE_RANGE_PRECISION_UNBOUNDED) {
    return "*";
  } else {
    std::string result;
    char buf[20];

    time_t time_secs = static_cast<time_t>(bound.time_ms / 1000);

    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", gmtime(&time_secs));
    result.append(buf);

    snprintf(buf, sizeof(buf), ".%03d", static_cast<int>(bound.time_ms % 1000));
    result.append(buf);

    result.append("(");
    std::stringstream ss;
    ss << bound.time_ms;
    result.append(ss.str());
    result.append(" ");
    switch (bound.precision) {
      case DSE_DATE_RANGE_PRECISION_YEAR:
        result.append("YEAR");
        break;
      case DSE_DATE_RANGE_PRECISION_MONTH:
        result.append("MONTH");
        break;
      case DSE_DATE_RANGE_PRECISION_DAY:
        result.append("DAY");
        break;
      case DSE_DATE_RANGE_PRECISION_HOUR:
        result.append("HOUR");
        break;
      case DSE_DATE_RANGE_PRECISION_MINUTE:
        result.append("MINUTE");
        break;
      case DSE_DATE_RANGE_PRECISION_SECOND:
        result.append("SECOND");
        break;
      case DSE_DATE_RANGE_PRECISION_MILLISECOND:
        result.append("MILLISECOND");
        break;
      case DSE_DATE_RANGE_PRECISION_UNBOUNDED:
        assert(false);
        result.append("UNBOUNDED");
        break;
    }
    result.append(")");

    return result;
  }
}

/**
 * A wrapper around DSE date range
 */
class DateRange {
public:
  typedef DseDateRange Native;
  typedef DseDateRange ConvenienceType;
  typedef DseDateRange ValueType;

  DateRange(const ConvenienceType& date_range)
      : date_range_(date_range) {}

  /**
   * Create a single date range
   *
   * @param single_date The bound representing the single date (default: unbounded)
   */
  DateRange(DateRangeBound single_date = DateRangeBound::unbounded()) {
    date_range_.is_single_date = cass_true;
    date_range_.lower_bound = single_date;
  }

  /**
   * Create a single date range from a date/time string
   *
   * @param precision The precision of the date/time string
   * @param str The date/time string
   */
  DateRange(DseDateRangePrecision precision, const std::string& date_time) {
    date_range_.is_single_date = cass_true;
    date_range_.lower_bound = DateRangeBound::lower(precision, date_time);
  }

  /**
   * Create a date range using an lower and upper bound
   *
   * @param lower_bound The lower bound of the range
   * @param upper_bound The upper bound of the range
   */
  DateRange(DateRangeBound lower_bound, DateRangeBound upper_bound) {
    date_range_.is_single_date = cass_false;
    date_range_.lower_bound = lower_bound;
    date_range_.upper_bound = upper_bound;
  }

  /**
   * Create a date range using a lower and upper bound provided as data/time
   * strings
   *
   * @param lower_bound_precision The precision of the lower bound date/time
   *                              string
   * @param lower_bound_date_time The lower date/time string
   * @param upper_bound_precision The precision of the upper bound date/time]
   *                              string
   * @param upper_bound_date_time The upper date/time string
   */
  DateRange(DseDateRangePrecision lower_bound_precision, const std::string& lower_bound_date_time,
            DseDateRangePrecision upper_bound_precision, const std::string& upper_bound_date_time) {
    date_range_.is_single_date = cass_false;
    date_range_.lower_bound = DateRangeBound::lower(lower_bound_precision, lower_bound_date_time);
    date_range_.upper_bound = DateRangeBound::upper(upper_bound_precision, upper_bound_date_time);
  }

  /**
   * Append the date range to a collection
   *
   * @param collection
   */
  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_dse_date_range(collection.get(), &date_range_));
  }

  /**
   * Compare this date range to another date range
   *
   * @param rhs
   * @return -1 if less than, 1 if greater than, otherwise 0
   */
  int compare(const DateRange& rhs) const {
    int result = dse::compare(date_range_.lower_bound, rhs.date_range_.lower_bound);
    if (result != 0) return result;
    if (date_range_.is_single_date) {
      return rhs.date_range_.is_single_date ? 0 : -1;
    }
    if (rhs.date_range_.is_single_date) {
      return 1;
    }
    return dse::compare(date_range_.upper_bound, rhs.date_range_.upper_bound);
  }

  std::string cql_type() const { return "'DateRangeType'"; }

  std::string cql_value() const { return str(); }

  void initialize(const CassValue* value) {
    CassError error_code = cass_value_get_dse_date_range(value, &date_range_);
    ASSERT_EQ(CASS_OK, cass_value_get_dse_date_range(value, &date_range_))
        << "Unable to Get DSE Date Range: Invalid error code returned "
        << "[ " << cass_error_desc(error_code) << "]";
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_dse_date_range(tuple.get(), index, &date_range_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_dse_date_range_by_name(user_type.get(), name.c_str(),
                                                                 &date_range_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_dse_date_range(statement.get(), index, &date_range_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_dse_date_range_by_name(statement.get(), name.c_str(),
                                                                  &date_range_));
  }

  std::string str() const {
    if (date_range_.is_single_date) {
      return dse::str(date_range_.lower_bound);
    } else {
      return dse::str(date_range_.lower_bound) + " TO " + dse::str(date_range_.upper_bound);
    }
  }

  static std::string supported_server_version() { return "5.1.0"; }

  Native to_native() const { return date_range_; }

  ValueType value() const { return date_range_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_CUSTOM; }

protected:
  /**
   * Native driver value
   */
  DseDateRange date_range_;
};

typedef std::vector<DateRange> DateRangeVec;

inline bool operator==(const DateRange& lhs, const DateRange& rhs) { return lhs.compare(rhs) == 0; }

inline bool operator<(const DateRange& lhs, const DateRange& rhs) { return lhs.compare(rhs) < 0; }

inline std::ostream& operator<<(std::ostream& os, const DateRange& date_range) {
  os << date_range.cql_value();
  return os;
}

}}}} // namespace test::driver::values::dse

#endif // __TEST_DSE_DATE_RANGE_HPP__
