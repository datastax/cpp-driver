/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DOUBLE_HPP__
#define __TEST_DOUBLE_HPP__
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
 * Double wrapped value
 */
class Double : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(cass_double_t, Double) {
public:
  Double()
    : double_(0.0)
    , is_null_(true) {}

  Double(cass_double_t double_value)
    : double_(double_value)
    , is_null_(false) {}

  Double(const CassValue* value)
    : double_(0.0)
    , is_null_(false) {
    initialize(value);
  }

  Double(const std::string& value)
    : double_(0.0)
    , is_null_(false) {
    std::string value_trim = Utils::trim(value);

    // Determine if the value is NULL or valid (default is 0.0 otherwise)
    if (value_trim.empty() ||
        value_trim.compare("null") == 0) {
      is_null_ = true;
    } else {
      //Convert the value
      std::stringstream valueStream(value_trim);
      if ((valueStream >> double_).fail()) {
        TEST_LOG_ERROR("Invalid Double " << value_trim << ": Using default "
          << double_);
      }
    }
  }

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK,
      cass_collection_append_double(collection.get(), double_));
  }

  std::string cql_type() const {
    return std::string("double");
  }

  std::string cql_value() const {
    return str();
  }

  /**
   * Comparison operation for driver doubles
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_double_t& rhs) const {
    if (double_ < rhs) return -1;
    if (double_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver doubles
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Double& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.double_);
  }

  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_tuple_set_double(tuple.get(), index, double_));
    }
  }

  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_double_by_name(user_type.get(), name.c_str(), double_));
    }
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_double(statement.get(), index, double_));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  /**
   * Get the minimum value allowed for a double
   *
   * @return Minimum value for double
   */
  static Double min() {
    return Double(std::numeric_limits<cass_double_t>::min());
  }

  /**
   * Get the maximum value allowed for a double
   *
   * @return Maximum value for double
   */
  static Double max() {
    return Double(std::numeric_limits<cass_double_t>::max());
  }

  std::string str() const {
    if (is_null_) {
      return "null";
    } else {
      std::stringstream double_string;
      double_string << double_;
      return double_string.str();
    }
  }

  cass_double_t value() const {
    return double_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_DOUBLE;
  }

protected:
  /**
   * Native driver value
   */
  cass_double_t double_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_DOUBLE, value_type)
      << "Invalid Value Type: Value is not a double [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_DOUBLE, value_type)
      << "Invalid Data Type: Value->DataType is not a double";

    // Get the double
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_double(value, &double_))
        << "Unable to Get Double: Invalid error code returned";
      is_null_ = false;
    }
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_DOUBLE_HPP__
