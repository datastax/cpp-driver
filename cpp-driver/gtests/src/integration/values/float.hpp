/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_FLOAT_HPP__
#define __TEST_FLOAT_HPP__
#include "value_interface.hpp"
#include "test_utils.hpp"

#include <limits>
#include <sstream>

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace test {
namespace driver {

/**
 * Float wrapped value
 */
class Float : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(cass_float_t, Float) {
public:
  Float()
    : float_(0.0f)
    , is_null_(true) {}

  Float(cass_float_t float_value)
    : float_(float_value)
    , is_null_(false) {}

  Float(const CassValue* value)
    : float_(0.0f)
    , is_null_(false) {
    initialize(value);
  }

  Float(const std::string& value)
    : float_(0.0f)
    , is_null_(false) {
    std::string value_trim = Utils::trim(value);

    // Determine if the value is NULL or valid (default is 0.0 otherwise)
    if (value_trim.empty() ||
        value_trim.compare("null") == 0) {
      is_null_ = true;
    } else {
      //Convert the value
      std::stringstream valueStream(value_trim);
      if ((valueStream >> float_).fail()) {
        TEST_LOG_ERROR("Invalid Float " << value_trim << ": Using default "
          << float_);
      }
    }
  }

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK,
      cass_collection_append_float(collection.get(), float_));
  }

  std::string cql_type() const {
    return std::string("float");
  }

  std::string cql_value() const {
    return str();
  }

  /**
   * Comparison operation for driver floats
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_float_t& rhs) const {
    if (float_ < rhs) return -1;
    if (float_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver floats
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Float& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.float_);
  }

  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_tuple_set_float(tuple.get(), index, float_));
    }
  }

  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_float_by_name(user_type.get(), name.c_str(), float_));
    }
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_float(statement.get(), index, float_));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  /**
   * Get the minimum value allowed for a float
   *
   * @return Minimum value for float
   */
  static Float min() {
    return Float(std::numeric_limits<cass_float_t>::min());
  }

  /**
   * Get the maximum value allowed for a float
   *
   * @return Maximum value for float
   */
  static Float max() {
    return Float(std::numeric_limits<cass_float_t>::max());
  }

  std::string str() const {
      return "null";
    } else {
      std::stringstream float_string;
      float_string << float_;
      return float_string.str();
    }
  }

  cass_float_t value() const {
    return float_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_FLOAT;
  }

protected:
  /**
   * Native driver value
   */
  cass_float_t float_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_FLOAT, value_type)
      << "Invalid Value Type: Value is not a float [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_FLOAT, value_type)
      << "Invalid Data Type: Value->DataType is not a float";

    // Get the float
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_float(value, &float_))
        << "Unable to Get Float: Invalid error code returned";
      is_null_ = false;
    }
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_FLOAT_HPP__
