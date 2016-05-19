/*
  Copyright (c) 2014-2016 DataStax
*/
#ifndef __TEST_DSE_GRAPH_ARRAY_HPP__
#define __TEST_DSE_GRAPH_ARRAY_HPP__
#include "dse.h"

#include "objects/object_base.hpp"

#include "values.hpp"

#include <gtest/gtest.h>

#define ADD_ARRAY_NULL(value) \
  if (value.is_null()) { \
    ASSERT_EQ(CASS_OK, dse_graph_array_add_null(get())); \
  }

#define ADD_ARRAY_VALUE(add_function, value) \
   ADD_ARRAY_NULL(value) \
    else { \
    ASSERT_EQ(CASS_OK, ##add_function(get(), value.value())); \
  }

#define ADD_ARRAY_VALUE_C_STR(add_function, value) \
  ADD_ARRAY_NULL(value) \
   else { \
    ASSERT_EQ(CASS_OK, ##add_function(get(), value.c_str())); \
  }

namespace test {
namespace driver {

// Forward declaration for circular dependency
class DseGraphObject;

/**
 * Wrapped DSE graph array object
 */
class DseGraphArray : public Object<::DseGraphArray, dse_graph_array_free> {
public:
  /**
   * Create the empty DSE graph array object
   */
  DseGraphArray()
    : Object<::DseGraphArray, dse_graph_array_free>(dse_graph_array_new()) {}

  /**
   * Create the DSE graph array object from the native driver DSE graph
   * array object
   *
   * @param array Native driver object
   */
  DseGraphArray(::DseGraphArray* array)
    : Object<::DseGraphArray, dse_graph_array_free>(array) {}

  /**
   * Create the DSE graph array object from the shared reference
   *
   * @param array Shared reference
   */
  DseGraphArray(Ptr array)
    : Object<::DseGraphArray, dse_graph_array_free>(array) {}

  /**
   * Finish (Complete/Close) a DSE graph array object
   */
  void finish() {
    dse_graph_array_finish(get());
  }

  /**
   * Reset/Reuse a DSE graph array object
   */
  void reset() {
    finish();
    dse_graph_array_reset(get());
  }

  /**
   * Primary template definition for adding a value to DSE graph array
   *
   * @param value Value to apply
   */
  template<class C>
  void add(C value) {
    add_value(value.get());
  }


  /**
   * Add a array object to an existing array object
   *
   * @param value Array value to add
   */
  template<>
  void add<DseGraphArray>(DseGraphArray value) {
    value.finish();
    ASSERT_EQ(CASS_OK, dse_graph_array_add_array(get(), const_cast<::DseGraphArray*>(value.get())));
  }

  /**
   * Add a boolean to an array object
   *
   * @param value Boolean value to add
   */
  template<>
  void add<Boolean>(Boolean value) {
    ADD_ARRAY_VALUE(dse_graph_array_add_bool, value);
  }

  /**
   * Add a double to an array object
   *
   * @param value Double value to add
   */
  template<>
  void add<Double>(Double value) {
    ADD_ARRAY_VALUE(dse_graph_array_add_double, value);
  }

  /**
   * Add a 32-bit integer to an array object
   *
   * @param value 32-bit integer value to add
   */
  template<>
  void add<Integer>(Integer value) {
    ADD_ARRAY_VALUE(dse_graph_array_add_int32, value);
  }

  /**
   * Add a 64-bit integer (e.g. BigInteger) to an array object
   *
   * @param value 64-bit integer value to add
   */
  template<>
  void add<BigInteger>(BigInteger value) {
    ADD_ARRAY_VALUE(dse_graph_array_add_int64, value);
  }

  /**
   * Add a string to an array object
   *
   * @param value String value to add
   */
  template<>
  void add<Varchar>(Varchar value) {
    ADD_ARRAY_VALUE_C_STR(dse_graph_array_add_string, value);
  }
  template<>
  void add<Text>(Text value) {
    add<Varchar>(value);
  }
  template<>
  void add<std::string>(std::string value) {
    add<Varchar>(Varchar(value));
  }

private:
  /**
   * Add a object to an array object
   *
   * @param value Object value to add
   */
  void add_value(::DseGraphObject* value) {
    dse_graph_object_finish(value);
    ASSERT_EQ(CASS_OK, dse_graph_array_add_object(get(), value));
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_DSE_GRAPH_ARRAY_HPP__
