/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_POLYGON_HPP__
#define __TEST_DSE_POLYGON_HPP__
#include "value_interface.hpp"

#include "values/dse_line_string.hpp"

#include <algorithm>

namespace test {
namespace driver {

/**
 * DSE line string wrapped value
 */
class DsePolygon : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(std::vector<DseLineString>, DsePolygon) {
public:
  typedef Object< ::DsePolygon, dse_polygon_free> Native;
  typedef Object< ::DsePolygonIterator, dse_polygon_iterator_free> Iterator;

  DsePolygon()
    : is_null_(true) {
    set_polygon_string();
  }

  DsePolygon(const std::vector<DseLineString>& line_strings)
    : line_strings_(line_strings)
    , is_null_(false) {
    set_polygon_string();
  }

  DsePolygon(const CassValue* value)
    : is_null_(false) {
    initialize(value);
    set_polygon_string();
  }

  /**
   * @throws DsePoint::Exception
   */
  DsePolygon(const std::string& value)
    : is_null_(false) {
    std::string value_trim = Utils::trim(Utils::to_lower(value));

    // Determine if the value is NULL or valid
    if (value_trim.compare("null") == 0) {
      is_null_ = true;
    } else {
      // Strip all value information markup for a DSE polygon
      std::string polygon_value = Utils::replace_all(value_trim, "polygon empty", "");
      polygon_value = Utils::replace_all(polygon_value, "polygon", "");

      // Parse and add the line string(s) from the polygon string value
      parse_and_add_line_strings(polygon_value);
    }

    set_polygon_string();
  }

  DsePolygon(const CassRow* row, size_t column_index)
    : is_null_(false) {
    initialize(row, column_index);
    set_polygon_string();
  }

  const char* c_str() const {
    return polygon_string_.c_str();
  }

  std::string cql_type() const {
    return std::string("'PolygonType'");
  }

  std::string cql_value() const {
    // Ensure the polygon is not NULL or empty
    if (is_null_) {
      return polygon_string_;
    } else {
      if (line_strings_.empty()) {
        return "'POLYGON EMPTY'";
      }
    }
    return "'POLYGON(" + polygon_string_ + ")'";
  }

  /**
   * Comparison operation for driver value DSE polygon. This comparison is
   * performed in lexicographical order.
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const std::vector<DseLineString>& rhs) const {
    // Ensure they are the same size
    if (line_strings_.size() < rhs.size()) return -1;
    if (line_strings_.size() > rhs.size()) return 1;

    // Sort the line string for lexicographical comparison
    std::vector<DseLineString> lhs_sorted(line_strings_);
    std::vector<DseLineString> rhs_sorted(rhs);
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
  int compare(const DsePolygon& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.line_strings_);
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      Native polygon = generate_native_polygon();
      ASSERT_EQ(CASS_OK, cass_statement_bind_dse_polygon(statement.get(), index, polygon.get()));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  std::string str() const {
    return polygon_string_;
  }

  std::vector<DseLineString> value() const {
    return line_strings_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_CUSTOM;
  }

private:
  /**
   * DSE line strings used in the DSE polygon
   */
  std::vector<DseLineString> line_strings_;
  /**
   * Wrapped native driver value as string
   */
  std::string polygon_string_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_CUSTOM, value_type)
      << "Invalid Value Type: Value is not a DSE polygon (custom) [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_CUSTOM, value_type)
      << "Invalid Data Type: Value->DataType is not a DSE polygon (custom)";

    // Ensure the DSE polygon  is not NULL
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      // Create the iterator
      Iterator iterator(dse_polygon_iterator_new());
      ASSERT_EQ(CASS_OK, dse_polygon_iterator_reset(iterator.get(), value))
        << "Unable to Reset DSE Polygon Iterator: Invalid error code returned";
      cass_uint32_t total_rings = dse_polygon_iterator_num_rings(iterator.get());

      // Utilize the iterator to assign the line string from the points
      for (cass_uint32_t i = 0; i < total_rings; ++i) {
        // Add each ring of line strings to the polygon
        cass_uint32_t total_points = 0;
        ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_num_points(iterator.get(), &total_points))
          << "Unable to Get Number of Points from DSE Polygon: Invalid error code returned";
        std::vector<DsePoint> points;
        for (cass_uint32_t j = 0; j < total_points; ++j) {
          Point point = { 0.0, 0.0 };
          ASSERT_EQ(CASS_OK, dse_polygon_iterator_next_point(iterator.get(), &point.x, &point.y))
            << "Unable to Get DSE Point from DSE Polygon: Invalid error code returned";
          points.push_back(DsePoint(point));
        }
        line_strings_.push_back(DseLineString(points));
      }
    }
  }

  void initialize(const CassRow* row, size_t column_index) {
    ASSERT_TRUE(row != NULL) << "Invalid Row: Row should not be null";
    initialize(cass_row_get_column(row, column_index));
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
    line_strings_.push_back(DseLineString(line_string_value));
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

  /**
   * Generate the native DsePolygon object from the list of line strings
   *
   * @return Generated native DsePolygon reference object; polygon may be
   *         empty
   */
  Native generate_native_polygon() {
    // Create the native polygon object
    Native polygon = dse_polygon_new();

    // Ensure the polygon has sufficient line string(s)
    if (line_strings_.size() > 0) {
      // Initialize the polygon
      polygon = dse_polygon_new();
      size_t total_points = 0;
      for (std::vector<DseLineString>::const_iterator iterator = line_strings_.begin();
        iterator < line_strings_.end(); ++iterator) {
        total_points += (*iterator).size();
      }
      dse_polygon_reserve(polygon.get(), static_cast<cass_uint32_t>(line_strings_.size()), static_cast<cass_uint32_t>(total_points));

      // Add all the line strings to the native driver object
      for (std::vector<DseLineString>::const_iterator line_strings_iterator = line_strings_.begin();
        line_strings_iterator < line_strings_.end(); ++line_strings_iterator) {
        // Add each ring of line strings to the polygon
        std::vector<DsePoint> points = (*line_strings_iterator).value();
        dse_polygon_start_ring(polygon.get());
        for (std::vector<DsePoint>::const_iterator points_iterator = points.begin();
          points_iterator < points.end(); ++points_iterator) {
          Point point = (*points_iterator).value();
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

  /**
   * Set the string value of the DSE polygon
   */
  void set_polygon_string() {
    if (is_null_) {
      polygon_string_ = "null";
    } else {
      std::stringstream polygon_string;
      for (std::vector<DseLineString>::const_iterator iterator = line_strings_.begin();
        iterator < line_strings_.end(); ++iterator) {
        try {
          polygon_string << "(" << (*iterator).str() << ")";

          // Add a comma separation to the polygon (unless the last element)
          if ((iterator + 1) != line_strings_.end()) {
            polygon_string << ", ";
          }
        } catch (DsePoint::Exception& e) {
          LOG_ERROR(e.what());
        }
      }
      polygon_string_ = polygon_string.str();
    }
  }
};

} // namespace driver
} // namespace test

#endif //  __TEST_DSE_POLYGON_HPP__
