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

#ifndef __TEST_DSE_GRAPH_RESULT_HPP__
#define __TEST_DSE_GRAPH_RESULT_HPP__
#include "dse.h"

#include "exception.hpp"
#include "values.hpp"

#include "objects/dse_graph_array.hpp"
#include "objects/dse_graph_edge.hpp"
#include "objects/dse_graph_object.hpp"
#include "objects/dse_graph_path.hpp"
#include "objects/dse_graph_vertex.hpp"

#include <gtest/gtest.h>

#define INDENT_INCREMENT 2

namespace test { namespace driver { namespace dse {

/**
 * Wrapped DSE graph result object
 */
class GraphResult {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };

  /**
   * Create the DSE graph result object from the native driver DSE graph result
   * result object
   *
   * @param result Native driver pointer
   * @throws test::Exception if DSE graph result is NULL
   */
  GraphResult(const DseGraphResult* result)
      : result_(result) {
    if (!result) {
      throw Exception("Unable to Create DseGraphResult: Native pointer is NULL");
    }
  }

  /**
   * Get the DSE graph result element at the specified index
   *
   * @param index The element index to retrieve DSE graph result
   * @return DSE graph result at the specified element
   */
  GraphResult element(size_t index) { return dse_graph_result_element(result_, index); }

  /**
   * Get the number of elements from the DSE graph result
   *
   * @return The number of elements in the DSE graph result
   */
  size_t element_count() { return dse_graph_result_element_count(result_); }

  /**
   * Get the DSE graph result member key at the specified index
   *
   * @param index The member index to retrieve key
   * @return Key at the specified member index
   */
  const std::string key(size_t index) {
    size_t length;
    const char* key = dse_graph_result_member_key(result_, index, &length);
    return std::string(key, length);
  }

  /**
   * Get the DSE graph result member value at the specified index
   *
   * @param index The member index to retrieve DSE graph result
   * @return DSE graph result at the specified member index
   */
  GraphResult member(size_t index) { return dse_graph_result_member_value(result_, index); }

  /**
   * Get the number of members from the DSE graph result
   *
   * @return The number of members in the DSE graph result
   */
  size_t member_count() { return dse_graph_result_member_count(result_); }

  /**
   * Get the DSE graph result type
   *
   * @return DSE graph result type
   */
  DseGraphResultType type() { return dse_graph_result_type(result_); }

  /**
   * Get the DSE graph result as a graph edge
   *
   * @return DSE graph edge
   */
  GraphEdge edge() {
    // Validate this is a edge result
    EXPECT_EQ(8u, member_count());
    EXPECT_EQ("id", key(0));
    EXPECT_EQ("label", key(1));
    EXPECT_EQ("type", key(2));
    EXPECT_EQ("inVLabel", key(3));
    EXPECT_EQ("outVLabel", key(4));
    EXPECT_EQ("inV", key(5));
    EXPECT_EQ("outV", key(6));
    EXPECT_EQ("properties", key(7));

    // Get and return the edge result
    DseGraphEdgeResult edge;
    EXPECT_EQ(CASS_OK, dse_graph_result_as_edge(result_, &edge));
    return edge;
  }

  /**
   * Get the DSE graph result as a graph path
   *
   * @return DSE graph path
   */
  GraphPath path() {
    // Validate this is a path result
    EXPECT_EQ(2u, member_count());
    EXPECT_EQ("labels", key(0));
    EXPECT_EQ("objects", key(1));

    // Get and return the path result
    DseGraphPathResult path;
    EXPECT_EQ(CASS_OK, dse_graph_result_as_path(result_, &path));
    return path;
  }

  /**
   * Get the DSE graph result as a graph vertex
   *
   * @return DSE graph vertex
   */
  GraphVertex vertex() {
    // Validate this is a vertex result
    EXPECT_EQ(4u, member_count());
    EXPECT_EQ("id", key(0));
    EXPECT_EQ("label", key(1));
    EXPECT_EQ("type", key(2));
    EXPECT_EQ("properties", key(3));

    // Get and return the vertex result
    DseGraphVertexResult vertex;
    EXPECT_EQ(CASS_OK, dse_graph_result_as_vertex(result_, &vertex));
    return vertex;
  }

  /**
   * Primary template definition for validating type
   *
   * @return True is DSE graph result is valid for the class; false otherwise
   */
  template <class C>
  bool is_type();

  /**
   * Primary template definition for retrieving value
   *
   * @return Value as typename type
   * @throws DseGraphResult::Exception if DSE graph result is not valid for the
   *         class
   */
  template <class C>
  C value();

  /**
   * Generate a JSON style string for the DSE graph result
   *
   * @param indent Number of characters/spaces to indent
   * @return JSON style string representing the DSE graph result
   */
  const std::string str(unsigned int indent = 0);

private:
  /**
   * Native driver pointer instance
   */
  const DseGraphResult* result_;
};

/**
 * Determine if the DSE graph result is a DSE graph array object
 *
 * @return True if DSE graph result is a DSE graph array object; false
 *         otherwise
 */
template <>
inline bool GraphResult::is_type<GraphArray>() {
  return (dse_graph_result_is_array(result_) != cass_false);
}

/**
 * Determine if the DSE graph result is a boolean
 *
 * @return True if DSE graph result is a boolean; false otherwise
 */
template <>
inline bool GraphResult::is_type<Boolean>() {
  return (dse_graph_result_is_bool(result_) != cass_false);
}

/**
 * Determine if the DSE graph result is a double
 *
 * @return True if DSE graph result is a double; false otherwise
 */
template <>
inline bool GraphResult::is_type<Double>() {
  return (dse_graph_result_is_double(result_) != cass_false);
}

/**
 * Determine if the DSE graph result is a 32-bit integer
 *
 * @return True if DSE graph result is a 32-bit integer; false otherwise
 */
template <>
inline bool GraphResult::is_type<Integer>() {
  return (dse_graph_result_is_int32(result_) != cass_false);
}

/**
 * Determine if the DSE graph result is a 64-bit integer
 *
 * @return True if DSE graph result is a 64-bit integer; false otherwise
 */
template <>
inline bool GraphResult::is_type<BigInteger>() {
  return (dse_graph_result_is_int64(result_) != cass_false);
}

/**
 * Determine if the DSE graph result is a DSE graph object
 *
 * @return True if DSE graph result is a DSE graph object; false otherwise
 */
template <>
inline bool GraphResult::is_type<GraphObject>() {
  return (dse_graph_result_is_object(result_) != cass_false);
}

/**
 * Determine if the DSE graph result is a string
 *
 * @return True if DSE graph result is a string; false otherwise
 */
template <>
inline bool GraphResult::is_type<Varchar>() {
  return (dse_graph_result_is_string(result_) != cass_false);
}
template <>
inline bool GraphResult::is_type<Text>() {
  return is_type<Varchar>();
}
template <>
inline bool GraphResult::is_type<std::string>() {
  return is_type<Varchar>();
}

/**
 * Get the boolean from the DSE graph result
 *
 * @return Boolean value from the DSE graph result
 * @throws DseGraphResult::Exception if DSE graph result is not a boolean
 */
template <>
inline Boolean GraphResult::value<Boolean>() {
  if (!this->is_type<Boolean>()) {
    throw Exception("Unable to get Value: DSE graph result is not a boolean");
  }
  return Boolean(dse_graph_result_get_bool(result_) == cass_true ? true : false);
}

/**
 * Get the double from the DSE graph result
 *
 * @return Double value from the DSE graph result
 * @throws DseGraphResult::Exception if DSE graph result is not a double
 */
template <>
inline Double GraphResult::value<Double>() {
  if (!this->is_type<Double>()) {
    throw Exception("Unable to get Value: DSE graph result is not a double");
  }
  return Double(dse_graph_result_get_double(result_));
}

/**
 * Get the 32-bit integer from the DSE graph result
 *
 * @return 32-bit integer value from the DSE graph result
 * @throws DseGraphResult::Exception if DSE graph result is not a 32-bit
 *         integer
 */
template <>
inline Integer GraphResult::value<Integer>() {
  if (!this->is_type<Integer>()) {
    throw Exception("Unable to get Value: DSE graph result is not a integer");
  }
  return Integer(dse_graph_result_get_int32(result_));
}

/**
 * Get the 64-bit integer from the DSE graph result
 *
 * @return 64-bit integer value from the DSE graph result
 * @throws DseGraphResult::Exception if DSE graph result is not a 64-bit
 *         integer
 */
template <>
inline BigInteger GraphResult::value<BigInteger>() {
  if (!this->is_type<BigInteger>()) {
    throw Exception("Unable to get Value: DSE graph result is not a big integer");
  }
  return BigInteger(dse_graph_result_get_int64(result_));
}

/**
 * Get the string from the DSE graph result
 *
 * @return String value from the DSE graph result
 * @throws DseGraphResult::Exception if DSE graph result is not a string
 */
template <>
inline Varchar GraphResult::value<Varchar>() {
  if (!this->is_type<Varchar>()) {
    throw Exception("Unable to get Value: DSE graph result is not a string");
  }
  size_t length;
  const char* value = dse_graph_result_get_string(result_, &length);
  return Varchar(std::string(value, length));
}
template <>
inline Text GraphResult::value<Text>() {
  return Text(value<Varchar>().value());
}
template <>
inline std::string GraphResult::value<std::string>() {
  return value<Varchar>().value();
}

/**
 * Get the line string from the DSE graph result
 *
 * @return LineString value from the DSE graph result
 * @throws DseGraphResult::Exception if DSE graph result is not a line string
 */
template <>
inline LineString GraphResult::value<LineString>() {
  LineString line_string(result_);
  if (testing::Test::HasFailure()) {
    throw Exception("Unable to get Value: DSE graph result is not a line string");
  }
  return line_string;
}

/**
 * Get the point from the DSE graph result
 *
 * @return Point value from the DSE graph result
 * @throws DseGraphResult::Exception if DSE graph result is not a point
 */
template <>
inline Point GraphResult::value<Point>() {
  Point point(result_);
  if (testing::Test::HasFailure()) {
    throw Exception("Unable to get Value: DSE graph result is not a point");
  }
  return point;
}

/**
 * Get the polygon from the DSE graph result
 *
 * @return Polygon value from the DSE graph result
 * @throws DseGraphResult::Exception if DSE graph result is not a polygon
 */
template <>
inline Polygon GraphResult::value<Polygon>() {
  Polygon polygon(result_);
  if (testing::Test::HasFailure()) {
    throw Exception("Unable to get Value: DSE graph result is not a polygon");
  }
  return polygon;
}

inline const std::string GraphResult::str(unsigned int indent) {
  std::stringstream output;

  // Determine how to output the result value
  switch (type()) {
    case DSE_GRAPH_RESULT_TYPE_ARRAY:
      output << Utils::indent("[", indent);
      for (size_t i = 0; i < element_count(); ++i) {
        output << std::endl << element(i).str(indent + INDENT_INCREMENT);
        if (i != (element_count() - 1)) {
          output << ",";
        }
      }
      output << std::endl << Utils::indent("]", indent);
      break;
    case DSE_GRAPH_RESULT_TYPE_BOOL:
      output << Utils::indent(value<Boolean>().str(), indent);
      break;
    case DSE_GRAPH_RESULT_TYPE_NULL:
      output << Utils::indent("null", indent);
      break;
    case DSE_GRAPH_RESULT_TYPE_NUMBER:
      if (is_type<BigInteger>()) {
        output << Utils::indent(value<BigInteger>().str(), indent);
      } else if (is_type<Double>()) {
        output << Utils::indent(value<Double>().str(), indent);
      } else {
        output << Utils::indent(value<Integer>().str(), indent);
      }
      break;
    case DSE_GRAPH_RESULT_TYPE_OBJECT:
      output << Utils::indent("{", indent);
      for (size_t i = 0; i < member_count(); ++i) {
        output << std::endl
               << Utils::indent("\"" + key(i) + "\"", indent + INDENT_INCREMENT) << ":";
        GraphResult member(this->member(i));
        if (member.type() == DSE_GRAPH_RESULT_TYPE_ARRAY ||
            member.type() == DSE_GRAPH_RESULT_TYPE_OBJECT) {
          output << std::endl << member.str(indent + INDENT_INCREMENT);
        } else {
          output << " " << member.str();
        }
        if (i != (member_count() - 1)) {
          output << ",";
        }
      }
      output << std::endl << Utils::indent("}", indent);
      break;
    case DSE_GRAPH_RESULT_TYPE_STRING:
      output << Utils::indent("\"" + value<std::string>() + "\"", indent);
      break;
  }

  // Return the formated output string
  return output.str();
}

}}} // namespace test::driver::dse

#endif // __TEST_DSE_GRAPH_RESULT_HPP__
