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

#ifndef __TEST_DSE_POLYGON_HPP__
#define __TEST_DSE_POLYGON_HPP__

#include "tlog.hpp"
#include "values/dse_line_string.hpp"

#include <algorithm>

namespace test { namespace driver { namespace values { namespace dse {

/**
 * DSE polygon wrapped value
 */
class Polygon {
public:
  typedef Object<DsePolygon, dse_polygon_free> Native;
  typedef Object<DsePolygonIterator, dse_polygon_iterator_free> Iterator;
  typedef std::string ConvenienceType;
  typedef std::vector<LineString> ValueType;

  Polygon() {}

  /**
   * @throws Point::Exception
   */
  Polygon(const ConvenienceType& value) {
    // Strip all value information markup for a DSE polygon
    std::string value_trim = Utils::trim(Utils::to_lower(value));
    std::string polygon_value = Utils::replace_all(value_trim, "polygon empty", "");
    polygon_value = Utils::replace_all(polygon_value, "polygon", "");

    // Parse and add the line string(s) from the polygon string value
    parse_and_add_line_strings(polygon_value);
  }

  void append(Collection collection) {
    Native polygon = to_native();
    ASSERT_EQ(CASS_OK, cass_collection_append_dse_polygon(collection.get(), polygon.get()));
  }

  std::string cql_type() const { return "'PolygonType'"; }

  std::string cql_value() const {
    if (line_strings_.empty()) {
      return "'POLYGON EMPTY'";
    }
    return "'POLYGON(" + str() + ")'";
  }

  /**
   * Comparison operation for driver value DSE polygon. This comparison is
   * performed in lexicographical order.
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const std::vector<LineString>& rhs) const {
    // Ensure they are the same size
    if (line_strings_.size() < rhs.size()) return -1;
    if (line_strings_.size() > rhs.size()) return 1;

    // Sort the line string for lexicographical comparison
    std::vector<LineString> lhs_sorted(line_strings_);
    std::vector<LineString> rhs_sorted(rhs);
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
   * Comparison operation for driver value DSE polygon. This comparison is
   * performed in lexicographical order.
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Polygon& rhs) const { return compare(rhs.line_strings_); }

  void initialize(const CassValue* value) {
    // Get the polygon from the value
    Iterator iterator(dse_polygon_iterator_new());
    ASSERT_EQ(CASS_OK, dse_polygon_iterator_reset(iterator.get(), value))
        << "Unable to Reset DSE Polygon Iterator: Invalid error code returned";
    assign_line_strings(iterator);
  }

  void set(Tuple tuple, size_t index) {
    Native polygon = to_native();
    ASSERT_EQ(CASS_OK, cass_tuple_set_dse_polygon(tuple.get(), index, polygon.get()));
  }

  void set(UserType user_type, const std::string& name) {
    Native polygon = to_native();
    ASSERT_EQ(CASS_OK,
              cass_user_type_set_dse_polygon_by_name(user_type.get(), name.c_str(), polygon.get()));
  }

  void statement_bind(Statement statement, size_t index) {
    Native polygon = to_native();
    ASSERT_EQ(CASS_OK, cass_statement_bind_dse_polygon(statement.get(), index, polygon.get()));
  }

  void statement_bind(Statement statement, const std::string& name) {
    Native polygon = to_native();
    ASSERT_EQ(CASS_OK, cass_statement_bind_dse_polygon_by_name(statement.get(), name.c_str(),
                                                               polygon.get()));
  }

  std::string str() const {
    std::stringstream polygon_string;
    for (std::vector<LineString>::const_iterator iterator = line_strings_.begin();
         iterator < line_strings_.end(); ++iterator) {
      try {
        polygon_string << "(" << (*iterator).str() << ")";

        // Add a comma separation to the polygon (unless the last element)
        if ((iterator + 1) != line_strings_.end()) {
          polygon_string << ", ";
        }
      } catch (Point::Exception& e) {
        TEST_LOG_ERROR(e.what());
      }
    }
    return polygon_string.str();
  }

  static std::string supported_server_version() { return "5.0.0"; }

  Native to_native() const {
    // Create the native polygon object
    Native polygon = dse_polygon_new();

    // Ensure the polygon has sufficient line string(s)
    if (line_strings_.size() > 0) {
      // Initialize the polygon
      polygon = dse_polygon_new();
      size_t total_points = 0;
      for (std::vector<LineString>::const_iterator iterator = line_strings_.begin();
           iterator < line_strings_.end(); ++iterator) {
        total_points += (*iterator).size();
      }
      dse_polygon_reserve(polygon.get(), static_cast<cass_uint32_t>(line_strings_.size()),
                          static_cast<cass_uint32_t>(total_points));

      // Add all the line strings to the native driver object
      for (std::vector<LineString>::const_iterator line_strings_iterator = line_strings_.begin();
           line_strings_iterator < line_strings_.end(); ++line_strings_iterator) {
        // Add each ring of line strings to the polygon
        std::vector<Point> points = (*line_strings_iterator).value();
        dse_polygon_start_ring(polygon.get());
        for (std::vector<Point>::const_iterator points_iterator = points.begin();
             points_iterator < points.end(); ++points_iterator) {
          Point::PointType point = (*points_iterator).value();
          EXPECT_EQ(CASS_OK, dse_polygon_add_point(polygon.get(), point.x, point.y))
              << "Unable to Add DSE Point to DSE Polygon: Invalid error code returned";
        }
      }
      EXPECT_EQ(CASS_OK, dse_polygon_finish(polygon.get()))
          << "Unable to Complete DSE Polygon: Invalid error code returned";
    }

    // Return the generated line string
    return polygon;
  }

  ValueType value() const { return line_strings_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_CUSTOM; }

private:
  /**
   * DSE line strings used in the DSE polygon
   */
  std::vector<LineString> line_strings_;

  /**
   * Assign the line strings from the native iterator
   *
   * @param iterator Native driver iterator
   */
  void assign_line_strings(Iterator iterator) {
    // Get the number of rings in the polygon
    cass_uint32_t total_rings = dse_polygon_iterator_num_rings(iterator.get());

    // Utilize the iterator to assign the line string from the points
    for (cass_uint32_t i = 0; i < total_rings; ++i) {
      // Add each ring of line strings to the polygon
      cass_uint32_t total_points = 0;
      ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_num_points(iterator.get(), &total_points))
          << "Unable to Get Number of Points from DSE Polygon: Invalid error code returned";
      std::vector<Point> points;
      for (cass_uint32_t j = 0; j < total_points; ++j) {
        Point::PointType point = { 0.0, 0.0 };
        ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_point(iterator.get(), &point.x, &point.y))
            << "Unable to Get DSE Point from DSE Polygon: Invalid error code returned";
        points.push_back(Point(point));
      }
      line_strings_.push_back(LineString(points));
    }
  }

  /**
   * Add the line string
   *
   * @param value Line string value to add
   * @throws value::DsePoint::Exception
   */
  void add_line_string(const std::string& value) {
    // Strip all value information markup
    std::string line_string_value = Utils::replace_all(value, "(", "");
    line_string_value = Utils::trim(Utils::replace_all(line_string_value, ")", ""));

    // Add the line string
    line_strings_.push_back(LineString(line_string_value));
  }

  /**
   * Get/Parse and add the line string(s) from a polygon string value
   *
   * @param value String value to parse line string(s) from
   * @return Parsed line string value; might be original value
   * @throws value::DsePoint::Exception
   */
  void parse_and_add_line_strings(const std::string& value) {
    // Determine if the value contains multiple line strings
    std::string line_strings = value;
    size_t close_paren = line_strings.find(')');
    if (close_paren != std::string::npos) {
      // Iterate over the line strings
      while (close_paren != std::string::npos) {
        // Add the parsed line string value
        std::string line_string_value = line_strings.substr(0, close_paren + 1);
        add_line_string(line_string_value);

        // Move to the next line string
        line_strings = line_strings.substr(close_paren + 1);
        close_paren = line_strings.find(')');
      }
    }
  }
};

inline std::ostream& operator<<(std::ostream& os, const Polygon& polygon) {
  os << polygon.cql_value();
  return os;
}

}}}} // namespace test::driver::values::dse

#endif // __TEST_DSE_POLYGON_HPP__
