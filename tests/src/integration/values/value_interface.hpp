/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_VALUE_INTERFACE_HPP__
#define __TEST_VALUE_INTERFACE_HPP__
#include "cassandra.h"

#include <string>

#include "objects/statement.hpp"

#include <gtest/gtest.h>

#define COMPARABLE_VALUE_INTERFACE(native, value) test::driver::ValueInterface<native>, \
                                                  test::driver::Comparable<native>, \
                                                  test::driver::Comparable<value>

#define COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(native, value) test::driver::ValueInterface<native>, \
                                                             test::driver::Comparable<value>

namespace test {
namespace driver {

/**
 * Create a comparable template to act as an interface for comparing
 * values.
 */
template<typename T>
class Comparable {
  friend bool operator==(const T& lhs, const T& rhs) {
    return lhs.compare(rhs) == 0;
  }

  friend bool operator!=(const T& lhs, const T& rhs) {
    return lhs.compare(rhs) != 0;
  }

  friend bool operator<(const T& lhs, const T& rhs) {
    return lhs.compare(rhs) <= -1;
  }

  friend bool operator>(const T& lhs, const T& rhs) {
    return lhs.compare(rhs) >= -1;
  }
};

/**
 * Value is a common interface for all the data types provided by the
 * DataStax C/C++ driver. This interface will perform expectations on the
 * value type and other miscellaneous needs for testing.
 */
template<typename T>
class ValueInterface {
public:
  /**
   * Convert the value to a c-string
   *
   * @return C style string representation
   */
  virtual const char* c_str() const = 0;

  /**
   * Get the CQL type
   *
   * @return CQL type name
   */
  virtual std::string cql_type() const = 0;

  /**
   * Get the CQL value (for embedded simple statements)
   *
   * @return CQL type value
   */
  virtual std::string cql_value() const = 0;

  /**
   * Comparison operation for Comparable template
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  virtual int compare(const T& rhs) const = 0;

  /**
   * Determine if the value is NULL (or unassigned)
   *
   * @return True if value is NULL; false otherwise
   */
  virtual bool is_null() const = 0;

  /**
   * Bind the value to a statement at the given index
   *
   * @param statement The statement to bind the value to
   * @param index The index/position where the value will be bound in the
   *              statement
   */
  virtual void statement_bind(Statement statement, size_t index) = 0;

  /**
   * Convert the value to a standard string
   *
   * @return Standard string representation
   */
  virtual std::string str() const = 0;

  /**
   * Get the native driver value
   *
   * @return Native driver value
   */
  virtual T value() const = 0;

  /**
   * Get the type of value the native driver value is
   *
   * @return Value type of the native driver value
   */
  virtual CassValueType value_type() const = 0;

protected:
  /**
   * Initialize the Value from the CassValue
   *
   * @param value CassValue to initialize Value from
   */
  virtual void initialize(const CassValue* value) = 0;

  /**
   * Initialize the Value from an element in a CassRow
   *
   * @param row CassRow to initialize Value from
   * @param column_index Index where the Value exists
   */
  virtual void initialize(const CassRow* row, size_t column_index) = 0;
};

} // namespace driver
} // namespace test

#endif // __TEST_VALUE_INTERFACE_HPP__
