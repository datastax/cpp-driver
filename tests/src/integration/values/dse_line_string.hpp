/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_LINE_STRING_HPP__
#define __TEST_DSE_LINE_STRING_HPP__
#include "test_utils.hpp"

#include "dse_point.hpp"

#include <algorithm>

namespace test {
namespace driver {

/**
 * DSE line string wrapped value
 */
class DseLineString : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(std::vector<DsePoint>, DseLineString) {
public:
  /**
   * Get the minimum DSE version that supports this type
   *
   * @return A version string
   */
  static const char* supported_version() { return "5.0.0"; }

public:
  typedef Object< ::DseLineString, dse_line_string_free> Native;
  typedef Object< ::DseLineStringIterator, dse_line_string_iterator_free> Iterator;

  DseLineString()
    : is_null_(true) {}

  DseLineString(const std::vector<DsePoint>& points)
    : points_(points)
    , is_null_(false) {}

  DseLineString(const CassValue* value)
    : is_null_(false) {
    initialize(value);
  }

  /**
   * @throws DsePoint::Exception
   */
  DseLineString(const std::string& value)
    : is_null_(false) {
    // Determine if the value is NULL or valid
    std::string value_trim = Utils::trim(Utils::to_lower(value));
    if (value_trim.compare("null") == 0) {
      is_null_ = true;
    } else {
      // Strip all value information markup for a DSE line string
      std::string line_string_value = Utils::replace_all(value_trim, "linestring empty", "");
      line_string_value = Utils::replace_all(line_string_value, "linestring", "");
      line_string_value = Utils::replace_all(line_string_value, "(", "");
      line_string_value = Utils::trim(Utils::replace_all(line_string_value, ")", ""));

      // Make sure the DSE line string value is valid and convert into wrapped object
      std::vector<std::string> points = Utils::explode(line_string_value, ',');
      for (std::vector<std::string>::const_iterator iterator = points.begin();
        iterator < points.end(); ++iterator) {
        DsePoint point(*iterator);
        points_.push_back(point);
      }
    }
  }

  DseLineString(const ::DseGraphResult* result)
    : is_null_(false) {
    initialize(result);
  }

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK,
      cass_collection_append_dse_line_string(collection.get(), to_native().get()));
  }

  std::string cql_type() const {
    return std::string("'LineStringType'");
  }

  std::string cql_value() const {
    // Ensure the line string is not NULL or empty
    if (is_null_) {
      return "null";
    } else {
      if (points_.empty()) {
        return "'LINESTRING EMPTY'";
      }
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
  int compare(const std::vector<DsePoint>& rhs) const {
    // Ensure they are the same size
    if (points_.size() < rhs.size()) return -1;
    if (points_.size() > rhs.size()) return 1;

    // Sort the points for lexicographical comparison
    std::vector<DsePoint> lhs_sorted(points_);
    std::vector<DsePoint> rhs_sorted(rhs);
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
  int compare(const DseLineString& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.points_);
  }

  /**
   * Generate the native DseLineString object from the list of points
   *
   * @return Generated native DseLineString reference object; line string
   *         may be empty
   */
  Native to_native() const {
    // Create the native line string object
    Native line_string = dse_line_string_new();

    // Ensure the line string has sufficient point(s)
    if (points_.size() > 0) {
      // Initialize the line string
      dse_line_string_reserve(line_string.get(), static_cast<cass_uint32_t>(points_.size()));

      // Add all the points to the native driver object
      for (std::vector<DsePoint>::const_iterator iterator = points_.begin();
        iterator < points_.end(); ++iterator) {
        Point point = (*iterator).value();
        EXPECT_EQ(CASS_OK, dse_line_string_add_point(line_string.get(), point.x, point.y))
          << "Unable to Add DSE Point to DSE Line String: Invalid error code returned";
      }
      EXPECT_EQ(CASS_OK, dse_line_string_finish(line_string.get()))
        << "Unable to Complete DSE Line String: Invalid error code returned";
    }

    // Return the generated line string
    return line_string;
  }

  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_tuple_set_dse_line_string(tuple.get(), index, to_native().get()));
    }
  }

  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_dse_line_string_by_name(user_type.get(),
        name.c_str(), to_native().get()));
    }
  }

  /**
   * Get the size of the line string
   *
   * @return The number of points in the line string
   */
  size_t size() const {
    return points_.size();
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      Native line_string = to_native();
      ASSERT_EQ(CASS_OK, cass_statement_bind_dse_line_string(statement.get(), index, line_string.get()));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  std::string str() const {
    if (is_null_) {
      return "null";
    } else {
      std::stringstream line_string;
      for (std::vector<DsePoint>::const_iterator iterator = points_.begin();
        iterator < points_.end(); ++iterator) {
        line_string << (*iterator).str();

        // Add a comma separation to the line string (unless the last element)
        if ((iterator + 1) != points_.end()) {
          line_string << ", ";
        }
      }
      return line_string.str();
    }
  }


  std::vector<DsePoint> value() const {
    return points_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_CUSTOM;
  }

private:
  /**
   * DSE Points used in the DSE line string
   */
  std::vector<DsePoint> points_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

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
      Point point = { 0.0, 0.0 };
      ASSERT_EQ(CASS_OK, dse_line_string_iterator_next_point(iterator.get(), &point.x, &point.y))
        << "Unable to Get DSE Point from DSE Line String: Invalid error code returned";
      points_.push_back(DsePoint(point));
    }
  }

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_CUSTOM, value_type)
      << "Invalid Value Type: Value is not a DSE line string (custom) [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_CUSTOM, value_type)
      << "Invalid Data Type: Value->DataType is not a DSE line string (custom)";

    // Ensure the DSE line string is not NULL
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      // Indicate the line string is not null
      is_null_ = false;

      // Get the line string from the value
      Iterator iterator(dse_line_string_iterator_new());
      ASSERT_EQ(CASS_OK, dse_line_string_iterator_reset(iterator.get(), value))
        << "Unable to Reset DSE Line String Iterator: Invalid error code returned";
      assign_points(iterator);
    }
  }

  void initialize(const ::DseGraphResult* result) {
    if (dse_graph_result_is_null(result)) {
      is_null_ = true;
    } else {
      // Get the line string iterator from the result and assign the points
      Iterator iterator(dse_line_string_iterator_new());
      ASSERT_EQ(CASS_OK, dse_graph_result_as_line_string(result, iterator.get()));
      assign_points(iterator);
    }
  }
};

inline std::ostream& operator<<(std::ostream& os, const DseLineString& line_string) {
  os << line_string.cql_value();
  return os;
}

} // namespace driver
} // namespace test

#endif // __TEST_DSE_LINE_STRING_HPP__
