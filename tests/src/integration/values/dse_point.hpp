/*
  Copyright (c) 2014-2016 DataStax
*/
#ifndef __TEST_DSE_POINT_HPP__
#define __TEST_DSE_POINT_HPP__
#include "value_interface.hpp"

#include "dse.h"

#include "dse_objects.hpp"
#include "dse_integration.hpp"

namespace test {
namespace driver {

/**
 * Simplified type for a point value
 */
typedef struct Point_ {
  /**
   * X coordinate
   */
  cass_double_t x;
  /**
   * y coordinate
   */
  cass_double_t y;
} Point;

/**
 * DSE point wrapped value
 */
class DsePoint : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(Point, DsePoint) {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
      : test::Exception(message) {};
  };

  DsePoint(cass_double_t x, cass_double_t y) {
    point_.x = x;
    point_.y = y;
    set_point_string();
  }

  DsePoint(Point point)
    : point_(point) {
    set_point_string();
  }

  DsePoint(const CassValue* value) {
    initialize(value);
    set_point_string();
  }

  /**
   * @throws DsePoint::Exception
   */
  DsePoint(const std::string& value) {
    //Convert the value
    if (!value.empty()) {
      // Strip all value information markup for a DSE point
      std::string point_value = Utils::replace_all(value, "POINT", "");
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
    set_point_string();
  }

  DsePoint(const CassRow* row, size_t column_index) {
    initialize(row, column_index);
    set_point_string();
  }

  const char* c_str() const {
    return point_string_.c_str();
  }

  std::string cql_type() const {
    return std::string("'PointType'");
  }

  std::string cql_value() const {
    return "'POINT(" + point_string_ + ")'";
  }

  /**
   * Comparison operation for driver value DSE point
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Point& rhs) const {
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
  int compare(const DsePoint& rhs) const {
    return compare(rhs.point_);
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_dse_point(statement.get(), index, point_.x, point_.y));
  }

  std::string str() const {
    return point_string_;
  }

  Point value() const {
    return point_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_CUSTOM;
  }

private:
  /**
   * Simple point value
   */
  Point point_;
  /**
   * Wrapped native driver value as string
   */
  std::string point_string_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_CUSTOM, value_type)
      << "Invalid Value Type: Value is not a DSE point (custom) [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_CUSTOM, value_type)
      << "Invalid Data Type: Value->DataType is not a DSE point (custom)";

    // Get the DSE type
    ASSERT_EQ(CASS_OK, cass_value_get_dse_point(value, &point_.x, &point_.y))
      << "Unable to Get DSE Point: Invalid error code returned";
  }

  void initialize(const CassRow* row, size_t column_index) {
    ASSERT_TRUE(row != NULL) << "Invalid Row: Row should not be null";
    initialize(cass_row_get_column(row, column_index));
  }

  /**
   * Set the string value of the DSE point
   */
  void set_point_string() {
    std::stringstream point_string;
    point_string << point_.x << " " << point_.y;
    point_string_ = point_string.str();
  }
};

} // namespace driver
} // namespace test

#endif //  __TEST_DSE_POINT_HPP__
