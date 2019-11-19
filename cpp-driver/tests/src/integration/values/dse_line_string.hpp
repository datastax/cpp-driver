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

#ifndef __TEST_DSE_LINE_STRING_HPP__
#define __TEST_DSE_LINE_STRING_HPP__
#include "test_utils.hpp"

#include "dse_point.hpp"

#include <algorithm>

namespace test { namespace driver { namespace values { namespace dse {

/**
 * DSE line string wrapped value
 */
class LineString : public Comparable<LineString> {
public:
  typedef Object<DseLineString, dse_line_string_free> Native;
  typedef Object<DseLineStringIterator, dse_line_string_iterator_free> Iterator;
  typedef std::string ConvenienceType;
  typedef std::vector<Point> ValueType;

  LineString() {}

  /**
   * @throws Point::Exception
   */
  LineString(const ConvenienceType& value) {
    // Strip all value information markup for a DSE line string
    std::string value_trim = Utils::trim(Utils::to_lower(value));
    std::string line_string_value = Utils::replace_all(value_trim, "linestring empty", "");
    line_string_value = Utils::replace_all(line_string_value, "linestring", "");
    line_string_value = Utils::replace_all(line_string_value, "(", "");
    line_string_value = Utils::trim(Utils::replace_all(line_string_value, ")", ""));

    // Make sure the DSE line string value is valid and convert into wrapped object
    std::vector<std::string> points = Utils::explode(line_string_value, ',');
    for (std::vector<std::string>::const_iterator iterator = points.begin();
         iterator < points.end(); ++iterator) {
      Point point(*iterator);
      points_.push_back(point);
    }
  }

  /**
   * Constructor using vector of points
   *
   * @param points Vector of points
   */
  LineString(const std::vector<Point>& points)
      : points_(points) {}

  void append(Collection collection) {
    Native line_string = to_native();
    ASSERT_EQ(CASS_OK, cass_collection_append_dse_line_string(collection.get(), line_string.get()));
  }

  std::string cql_type() const { return "'LineStringType'"; }

  std::string cql_value() const {
    if (points_.empty()) {
      return "'LINESTRING EMPTY'";
    }
    return "'LINESTRING(" + str() + ")'";
  }

  /**
   * Comparison operation for driver value DSE line string. This comparison
   * is performed in lexicographical order.
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const std::vector<Point>& rhs) const {
    // Ensure they are the same size
    if (points_.size() < rhs.size()) return -1;
    if (points_.size() > rhs.size()) return 1;

    // Sort the points for lexicographical comparison
    std::vector<Point> lhs_sorted(points_);
    std::vector<Point> rhs_sorted(rhs);
    std::sort(lhs_sorted.begin(), lhs_sorted.end());
    std::sort(rhs_sorted.begin(), rhs_sorted.end());

    // Iterate and compare
    for (size_t i = 0; i < lhs_sorted.size(); ++i) {
      int comparison = lhs_sorted[i].compare(rhs_sorted[i]);
      if (comparison != 0) return comparison;
    }
    return 0;
  }

  /**
   * Comparison operation for driver value DSE line string. This comparison
   * is performed in lexicographical order.
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const LineString& rhs) const { return compare(rhs.points_); }

  void initialize(const CassValue* value) {
    // Get the line string from the value
    Iterator iterator(dse_line_string_iterator_new());
    ASSERT_EQ(CASS_OK, dse_line_string_iterator_reset(iterator.get(), value))
        << "Unable to Reset DSE Line String Iterator: Invalid error code returned";
    assign_points(iterator);
  }

  void set(Tuple tuple, size_t index) {
    Native line_string = to_native();
    ASSERT_EQ(CASS_OK, cass_tuple_set_dse_line_string(tuple.get(), index, line_string.get()));
  }

  void set(UserType user_type, const std::string& name) {
    Native line_string = to_native();
    ASSERT_EQ(CASS_OK, cass_user_type_set_dse_line_string_by_name(user_type.get(), name.c_str(),
                                                                  line_string.get()));
  }

  /**
   * Get the size of the line string
   *
   * @return The number of points in the line string
   */
  size_t size() const { return points_.size(); }

  void statement_bind(Statement statement, size_t index) {
    Native line_string = to_native();
    ASSERT_EQ(CASS_OK,
              cass_statement_bind_dse_line_string(statement.get(), index, line_string.get()));
  }

  void statement_bind(Statement statement, const std::string& name) {
    Native line_string = to_native();
    ASSERT_EQ(CASS_OK, cass_statement_bind_dse_line_string_by_name(statement.get(), name.c_str(),
                                                                   line_string.get()));
  }

  std::string str() const {
    std::stringstream line_string;
    for (std::vector<Point>::const_iterator iterator = points_.begin(); iterator < points_.end();
         ++iterator) {
      line_string << (*iterator).str();

      // Add a comma separation to the line string (unless the last element)
      if ((iterator + 1) != points_.end()) {
        line_string << ", ";
      }
    }
    return line_string.str();
  }

  static std::string supported_server_version() { return "5.0.0"; }

  Native to_native() const {
    // Create the native line string object
    Native line_string = dse_line_string_new();

    // Ensure the line string has sufficient point(s)
    if (points_.size() > 0) {
      // Initialize the line string
      dse_line_string_reserve(line_string.get(), static_cast<cass_uint32_t>(points_.size()));

      // Add all the points to the native driver object
      for (std::vector<Point>::const_iterator iterator = points_.begin(); iterator < points_.end();
           ++iterator) {
        Point::PointType point = (*iterator).value();
        EXPECT_EQ(CASS_OK, dse_line_string_add_point(line_string.get(), point.x, point.y))
            << "Unable to Add DSE Point to DSE Line String: Invalid error code returned";
      }
      EXPECT_EQ(CASS_OK, dse_line_string_finish(line_string.get()))
          << "Unable to Complete DSE Line String: Invalid error code returned";
    }

    // Return the generated line string
    return line_string;
  }

  ValueType value() const { return points_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_CUSTOM; }

private:
  /**
   * DSE Points used in the DSE line string
   */
  std::vector<Point> points_;

  /**
   * Assign the points from the native iterator
   *
   * @param iterator Native driver iterator
   */
  void assign_points(Iterator iterator) {
    // Get the number of points in the line string
    cass_uint32_t size = dse_line_string_iterator_num_points(iterator.get());

    // Utilize the iterator to assign the points from the line string
    for (cass_uint32_t i = 0; i < size; ++i) {
      Point::PointType point = { 0.0, 0.0 };
      ASSERT_EQ(CASS_OK, dse_line_string_iterator_next_point(iterator.get(), &point.x, &point.y))
          << "Unable to Get DSE Point from DSE Line String: Invalid error code returned";
      points_.push_back(Point(point));
    }
  }
};

inline std::ostream& operator<<(std::ostream& os, const LineString& line_string) {
  os << line_string.cql_value();
  return os;
}

}}}} // namespace test::driver::values::dse

#endif // __TEST_DSE_LINE_STRING_HPP__
