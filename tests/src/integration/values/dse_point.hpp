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

#ifndef __TEST_DSE_POINT_HPP__
#define __TEST_DSE_POINT_HPP__
#include "dse_nullable_value.hpp"
#include "exception.hpp"

namespace test { namespace driver { namespace values { namespace dse {

/**
 * DSE point wrapped value
 */
class Point : public Comparable<Point> {
public:
  /**
   * Simplified structure  making up the x and y for a point value
   */
  typedef struct PointType_ {
    /**
     * X coordinate
     */
    cass_double_t x;
    /**
     * y coordinate
     */
    cass_double_t y;
  } PointType;

public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };
  typedef PointType Native;
  typedef std::string ConvenienceType;
  typedef PointType ValueType;

  Point() {}

  /**
   * @throws DsePoint::Exception
   */
  Point(const ConvenienceType& value) {
    // Strip all value information markup for a DSE point
    std::string value_trim = Utils::trim(Utils::to_lower(value));
    std::string point_value = Utils::replace_all(value_trim, "point", "");
    point_value = Utils::replace_all(point_value, "(", "");
    point_value = Utils::trim(Utils::replace_all(point_value, ")", ""));

    // Make sure the DSE point value is valid and convert into wrapped object
    std::vector<std::string> point_values = Utils::explode(point_value);
    if (point_values.size() == 2) {
      std::stringstream x(Utils::trim(point_values[0]));
      std::stringstream y(Utils::trim(point_values[1]));
      if ((x >> point_.x).fail()) {
        std::stringstream message;
        message << "Invalid X Value: " << x.str() << " is not valid for a point";
        throw Exception(message.str());
      }
      if ((y >> point_.y).fail()) {
        std::stringstream message;
        message << "Invalid Y Value: " << y.str() << " is not valid for a point";
        throw Exception(message.str());
      }
    } else {
      std::stringstream message;
      message << "Invalid Number of Coordinates: " << point_value << " is not valid for a point";
      throw Exception(message.str());
    }
  }

  /**
   * Constructor using simple point type
   *
   * @param point Simple point type value
   */
  Point(PointType point)
      : point_(point) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_dse_point(collection.get(), point_.x, point_.y));
  }

  std::string cql_type() const { return "'PointType'"; }

  std::string cql_value() const { return "'POINT(" + str() + ")'"; }

  /**
   * Comparison operation for driver value DSE point
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const PointType& rhs) const {
    if (point_.x < rhs.x) return -1;
    if (point_.x > rhs.x) return 1;

    if (point_.y < rhs.y) return -1;
    if (point_.y > rhs.y) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver value DSE point
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Point& rhs) const { return compare(rhs.point_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_dse_point(value, &point_.x, &point_.y))
        << "Unable to Get DSE Point: Invalid error code returned";
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_dse_point(tuple.get(), index, point_.x, point_.y));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_dse_point_by_name(user_type.get(), name.c_str(), point_.x,
                                                            point_.y));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_dse_point(statement.get(), index, point_.x, point_.y));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_dse_point_by_name(statement.get(), name.c_str(),
                                                             point_.x, point_.y));
  }

  std::string str() const {
    std::stringstream point_string;
    point_string << point_.x << " " << point_.y;
    return point_string.str();
  }

  static std::string supported_server_version() { return "5.0.0"; }

  Native to_native() const { return point_; }

  ValueType value() const { return point_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_CUSTOM; }

private:
  /**
   * Simple point value
   */
  PointType point_;
};

inline std::ostream& operator<<(std::ostream& os, const Point& point) {
  os << point.cql_value();
  return os;
}

}}}} // namespace test::driver::values::dse

#endif // __TEST_DSE_POINT_HPP__
