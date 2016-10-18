/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_GRAPH_OBJECT_HPP__
#define __TEST_DSE_GRAPH_OBJECT_HPP__
#include "dse.h"

#include "objects/object_base.hpp"
#include "objects/dse_graph_array.hpp"

#include "dse_values.hpp"

#include <gtest/gtest.h>

#define ADD_OBJECT_NULL(name, value) \
  if (value.is_null()) { \
    ASSERT_EQ(CASS_OK, dse_graph_object_add_null(get(), name.c_str())); \
  }

#define ADD_OBJECT_VALUE(add_function, name, value) \
   ADD_OBJECT_NULL(name, value) \
    else { \
    ASSERT_EQ(CASS_OK, add_function(get(), name.c_str(), value.value())); \
  }

#define ADD_OBJECT_VALUE_C_STR(add_function, name, value) \
  ADD_OBJECT_NULL(name, value) \
   else { \
    ASSERT_EQ(CASS_OK, add_function(get(), name.c_str(), value.c_str())); \
  }

namespace test {
namespace driver {

// Forward declaration for circular dependency
class DseGraphArray;

/**
 * Wrapped DSE graph object
 */
class DseGraphObject : public Object< ::DseGraphObject, dse_graph_object_free> {
public:
  /**
   * Create the empty DSE graph object
   */
  DseGraphObject()
    : Object< ::DseGraphObject, dse_graph_object_free>(dse_graph_object_new()) {}

  /**
   * Create the DSE graph object from the native driver DSE graph object
   *
   * @param object Native driver object
   */
  DseGraphObject(::DseGraphObject* object)
    : Object< ::DseGraphObject, dse_graph_object_free>(object) {}

  /**
   * Create the DSE graph object from the shared reference
   *
   * @param object Shared reference
   */
  DseGraphObject(Ptr object)
    : Object< ::DseGraphObject, dse_graph_object_free>(object) {}

  /**
   * Destructor to clean up the shared reference pointers that may be associated
   * with the graph object
   */
  ~DseGraphObject() {
    line_strings_.clear();
    polygons_.clear();
  }

  /**
   * Finish (Complete/Close) a DSE graph object
   */
  void finish() {
    dse_graph_object_finish(get());
  }

  /**
   * Reset/Reuse a DSE graph object
   */
  void reset() {
    finish();
    line_strings_.clear();
    polygons_.clear();
    dse_graph_object_reset(get());
  }

  /**
   * Primary template definition for add value to DSE graph object
   *
   * @param name Name to apply value to
   * @param value Value to apply
   */
  template<class C>
  void add(const std::string& name, C value) {
    add_value(name, value.get());
  }

private:
  /**
   * Line strings associated with the graph object
   */
  std::vector<DseLineString::Native> line_strings_;
  /**
   * Polygons associated with the graph object
   */
  std::vector<DsePolygon::Native> polygons_;

  /**
   * Add a array to an object with the specified name
   *
   * @param name Name to apply array value to
   * @param value Array value to apply
   */
  void add_value(const std::string& name, ::DseGraphArray* value)  {
    dse_graph_array_finish(value);
    ASSERT_EQ(CASS_OK, dse_graph_object_add_array(get(), name.c_str(), value));
  }
};

/**
 * Add boolean to an object with the specified name
 *
 * @param name Name to apply boolean value to
 * @param value Boolean value to apply
 */
template<>
inline void DseGraphObject::add<Boolean>(const std::string& name, Boolean value) {
  ADD_OBJECT_VALUE(dse_graph_object_add_bool, name, value);
}

/**
 * Add a double to an object with the specified name
 *
 * @param name Name to apply double value to
 * @param value Double value to apply
 */
template<>
inline void DseGraphObject::add<Double>(const std::string& name, Double value) {
  ADD_OBJECT_VALUE(dse_graph_object_add_double, name, value);
}

/**
 * Add a 32-bit integer to an object with the specified name
 *
 * @param name Name to apply 32-bit integer value to
 * @param value 32-bit integer value to apply
 */
template<>
inline void DseGraphObject::add<Integer>(const std::string& name, Integer value) {
  ADD_OBJECT_VALUE(dse_graph_object_add_int32, name, value);
}

/**
 * Add a 64-bit integer (e.g. BigInteger) to an object with the specified
 * name
 *
 * @param name Name to apply 64-bit integer value to
 * @param value 64-bit integer value to apply
 */
template<>
inline void DseGraphObject::add<BigInteger>(const std::string& name, BigInteger value) {
  ADD_OBJECT_VALUE(dse_graph_object_add_int64, name, value);
}

/**
 * Add a object to an object with the specified name
 *
 * @param name Name to apply object value to
 * @param value Object value to apply
 */
template<>
inline void DseGraphObject::add<DseGraphObject>(const std::string& name, DseGraphObject value) {
  value.finish();
  ASSERT_EQ(CASS_OK, dse_graph_object_add_object(get(), name.c_str(),
    const_cast< ::DseGraphObject*>(value.get())));
}

/**
 * Add a string to an object with the specified name
 *
 * @param name Name to apply string value to
 * @param value String value to apply
 */
template<>
inline void DseGraphObject::add<Varchar>(const std::string& name, Varchar value) {
  ADD_OBJECT_VALUE_C_STR(dse_graph_object_add_string, name, value);
}
template<>
inline void DseGraphObject::add<Text>(const std::string& name, Text value) {
  add<Text>(name, value);
}
template<>
inline void DseGraphObject::add<std::string>(const std::string& name, std::string value) {
  add<Varchar>(name, Varchar(value));
}

/**
 * Add a line string to an object with the specified name
 *
 * @param name Name to apply object value to
 * @param value Line string value to apply
 */
template<>
inline void DseGraphObject::add<DseLineString>(const std::string& name, DseLineString value) {
  DseLineString::Native line_string = value.get();
  line_strings_.push_back(line_string);
  ASSERT_EQ(CASS_OK, dse_graph_object_add_line_string(get(), name.c_str(),
    line_string.get()));
}

/**
 * Add a point to an object with the specified name
 *
 * @param name Name to apply object value to
 * @param value Point value to apply
 */
template<>
inline void DseGraphObject::add<DsePoint>(const std::string& name, DsePoint value) {
  ASSERT_EQ(CASS_OK, dse_graph_object_add_point(get(), name.c_str(),
    value.value().x, value.value().y));
}

/**
 * Add a polygon to an object with the specified name
 *
 * @param name Name to apply object value to
 * @param value Polygon value to apply
 */
template<>
inline void DseGraphObject::add<DsePolygon>(const std::string& name, DsePolygon value) {
  DsePolygon::Native polygon = value.get();
  polygons_.push_back(polygon);
  ASSERT_EQ(CASS_OK, dse_graph_object_add_polygon(get(), name.c_str(),
    polygon.get()));
}

} // namespace driver
} // namespace test

#endif // __TEST_DSE_GRAPH_OBJECT_HPP__
