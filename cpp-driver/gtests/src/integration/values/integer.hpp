/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_INTEGER_HPP__
#define __TEST_INTEGER_HPP__
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
 * 8-bit integer wrapped value
 */
class TinyInteger : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(cass_int8_t, TinyInteger) {
public:
  TinyInteger()
    : integer_(0)
    , is_null_(true) {}

  TinyInteger(cass_int8_t integer)
    : integer_(integer)
    , is_null_(false) {}

  TinyInteger(const CassValue* value)
    : integer_(0)
    , is_null_(false) {
    initialize(value);
  }

  TinyInteger(const std::string& value)
    : integer_(0)
    , is_null_(false) {
    std::string value_trim = Utils::trim(value);

    // Determine if the value is NULL or valid (default is 0 otherwise)
    if (value_trim.empty() ||
        value_trim.compare("null") == 0) {
      is_null_ = true;
    } else {
    //Convert the value
      std::stringstream valueStream(value_trim);
      if ((valueStream >> integer_).fail()) {
        LOG_ERROR("Invalid Tiny Integer " << value_trim << ": Using default "
          << integer_);
      }
    }
  }

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK,
      cass_collection_append_int8(collection.get(), integer_));
  }

  std::string cql_type() const {
    return std::string("tinyint");
  }

  std::string cql_value() const {
    return str();
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_int8_t& rhs) const {
    if (integer_ < rhs) return -1;
    if (integer_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const TinyInteger& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.integer_);
  }

  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_tuple_set_int8(tuple.get(), index, integer_));
    }
  }

  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_int8_by_name(user_type.get(), name.c_str(),
        integer_));
    }
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_int8(statement.get(), index, integer_));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  /**
   * Get the minimum value allowed for a tiny integer
   *
   * @return Minimum value for tiny integer
   */
  static TinyInteger min() {
    return TinyInteger(std::numeric_limits<cass_int8_t>::min());
  }

  /**
   * Get the maximum value allowed for a tiny integer
   *
   * @return Maximum value for tiny integer
   */
  static TinyInteger max() {
    return TinyInteger(std::numeric_limits<cass_int8_t>::max());
  }

  std::string str() const {
    if (is_null_) {
      return "null";
    } else {
      std::stringstream integer_string;
      integer_string << integer_;
      return integer_string.str();
    }
  }

  cass_int8_t value() const {
    return integer_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_TINY_INT;
  }

protected:
  /**
   * Native driver value
   */
  cass_int8_t integer_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_TINY_INT, value_type)
      << "Invalid Value Type: Value is not a tiny integer [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_TINY_INT, value_type)
      << "Invalid Data Type: Value->DataType is not a tiny integer";

    // Get the 16-bit integer
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_int8(value, &integer_))
        << "Unable to Get 8-bit Integer: Invalid error code returned";
      is_null_ = false;
    }
  }
};

/**
 * 16-bit integer wrapped value
 */
class SmallInteger : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(cass_int16_t, SmallInteger) {
public:
  SmallInteger()
    : integer_(0)
    , is_null_(true) {}

  SmallInteger(cass_int16_t integer)
    : integer_(integer)
    , is_null_(false) {}

  SmallInteger(const CassValue* value)
    : integer_(0)
    , is_null_(false) {
    initialize(value);
  }

  SmallInteger(const std::string& value)
    : integer_(0)
    , is_null_(false) {
    std::string value_trim = Utils::trim(value);

    // Determine if the value is NULL or valid (default is 0 otherwise)
    if (value_trim.empty() ||
        value_trim.compare("null") == 0) {
      is_null_ = true;
    } else {
    //Convert the value
      std::stringstream valueStream(value_trim);
      if ((valueStream >> integer_).fail()) {
        LOG_ERROR("Invalid Small Integer " << value_trim << ": Using default "
          << integer_);
      }
    }
  }

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK,
      cass_collection_append_int16(collection.get(), integer_));
  }

  std::string cql_type() const {
    return std::string("smallint");
  }

  std::string cql_value() const {
    return str();
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_int16_t& rhs) const {
    if (integer_ < rhs) return -1;
    if (integer_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const SmallInteger& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.integer_);
  }

  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_tuple_set_int16(tuple.get(), index, integer_));
    }
  }

  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_int16_by_name(user_type.get(), name.c_str(),
        integer_));
    }
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_int16(statement.get(), index, integer_));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  /**
   * Get the minimum value allowed for a small integer
   *
   * @return Minimum value for small integer
   */
  static SmallInteger min() {
    return SmallInteger(std::numeric_limits<cass_int16_t>::min());
  }

  /**
   * Get the maximum value allowed for a small integer
   *
   * @return Maximum value for small integer
   */
  static SmallInteger max() {
    return SmallInteger(std::numeric_limits<cass_int16_t>::max());
  }

  std::string str() const {
    if (is_null_) {
      return "null";
    } else {
      std::stringstream integer_string;
      integer_string << integer_;
      return integer_string.str();
    }
  }

  cass_int16_t value() const {
    return integer_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_SMALL_INT;
  }

protected:
  /**
   * Native driver value
   */
  cass_int16_t integer_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_SMALL_INT, value_type)
      << "Invalid Value Type: Value is not a small integer [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_SMALL_INT, value_type)
      << "Invalid Data Type: Value->DataType is not a small integer";

    // Get the 16-bit integer
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_int16(value, &integer_))
        << "Unable to Get 16-bit Integer: Invalid error code returned";
      is_null_ = false;
    }
  }
};

/**
 * 32-bit integer wrapped value
 */
class Integer : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(cass_int32_t, Integer) {
public:
  Integer()
    : integer_(0)
    , is_null_(true) {}

  Integer(cass_int32_t integer)
    : integer_(integer)
    , is_null_(false) {}

  Integer(const CassValue* value)
    : integer_(0)
    , is_null_(false) {
    initialize(value);
  }

  Integer(const std::string& value)
    : integer_(0)
    , is_null_(false) {
    std::string value_trim = Utils::trim(value);

    // Determine if the value is NULL or valid (default is 0 otherwise)
    if (value_trim.empty() ||
        value_trim.compare("null") == 0) {
      is_null_ = true;
    } else {
    //Convert the value
      std::stringstream valueStream(value_trim);
      if ((valueStream >> integer_).fail()) {
        LOG_ERROR("Invalid Integer " << value_trim << ": Using default "
          << integer_);
      }
    }
  }

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK,
      cass_collection_append_int32(collection.get(), integer_));
  }

  std::string cql_type() const {
    return std::string("int");
  }

  std::string cql_value() const {
    return str();
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_int32_t& rhs) const {
    if (integer_ < rhs) return -1;
    if (integer_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Integer& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.integer_);
  }

  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_tuple_set_int32(tuple.get(), index, integer_));
    }
  }

  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_int32_by_name(user_type.get(), name.c_str(),
        integer_));
    }
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_int32(statement.get(), index, integer_));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  /**
   * Get the minimum value allowed for a integer
   *
   * @return Minimum value for integer
   */
  static Integer min() {
    return Integer(std::numeric_limits<cass_int32_t>::min());
  }

  /**
   * Get the maximum value allowed for a integer
   *
   * @return Maximum value for integer
   */
  static Integer max() {
    return Integer(std::numeric_limits<cass_int32_t>::max());
  }

  std::string str() const {
    if (is_null_) {
      return "null";
    } else {
      std::stringstream integer_string;
      integer_string << integer_;
      return integer_string.str();
    }
  }

  cass_int32_t value() const {
    return integer_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_INT;
  }

protected:
  /**
   * Native driver value
   */
  cass_int32_t integer_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_INT, value_type)
      << "Invalid Value Type: Value is not a integer [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_INT, value_type)
      << "Invalid Data Type: Value->DataType is not a integer";

    // Get the 32-bit integer
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_int32(value, &integer_))
        << "Unable to Get 32-bit Integer: Invalid error code returned";
      is_null_ = false;
    }
  }
};

/**
 * 64-bit integer wrapped value (e.g. bigint)
 */
class BigInteger : public COMPARABLE_VALUE_INTERFACE_VALUE_ONLY(cass_int64_t, BigInteger) {
public:
  BigInteger()
    : integer_(0)
    , is_null_(true) {}

  BigInteger(cass_int64_t integer)
    : integer_(integer)
    , is_null_(false) {}

  BigInteger(const CassValue* value)
    : integer_(0)
    , is_null_(false) {
    initialize(value);
  }

  BigInteger(const std::string& value)
    : integer_(0)
    , is_null_(false) {
    std::string value_trim = Utils::trim(value);

    // Determine if the value is NULL or valid (default is 0 otherwise)
    if (value_trim.empty() ||
        value_trim.compare("null") == 0) {
      is_null_ = true;
    } else {
      //Convert the value
      std::stringstream valueStream(value_trim);
      if ((valueStream >> integer_).fail()) {
        LOG_ERROR("Invalid Big Integer " << value_trim << ": Using default "
          << integer_);
      }
    }
  }

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK,
      cass_collection_append_int64(collection.get(), integer_));
  }

  std::string cql_type() const {
    return std::string("bigint");
  }

  std::string cql_value() const {
    return str();
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const cass_int64_t& rhs) const {
    if (integer_ < rhs) return -1;
    if (integer_ > rhs) return 1;

    return 0;
  }

  /**
   * Comparison operation for driver integers
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const BigInteger& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.integer_);
  }

  void set(Tuple tuple, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_tuple_set_null(tuple.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_tuple_set_int64(tuple.get(), index, integer_));
    }
  }

  void set(UserType user_type, const std::string& name) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_null_by_name(user_type.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
        cass_user_type_set_int64_by_name(user_type.get(), name.c_str(),
        integer_));
    }
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_int64(statement.get(), index, integer_));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  /**
   * Get the minimum value allowed for a big integer
   *
   * @return Minimum value for big integer
   */
  static BigInteger min() {
    return BigInteger(std::numeric_limits<cass_int64_t>::min());
  }

  /**
   * Get the maximum value allowed for a big integer
   *
   * @return Maximum value for big integer
   */
  static BigInteger max() {
    return BigInteger(std::numeric_limits<cass_int64_t>::max());
  }

  std::string str() const {
    if (is_null_) {
      return "null";
    } else {
      std::stringstream integer_string;
      integer_string << integer_;
      return integer_string.str();
    }
  }

  cass_int64_t value() const {
    return integer_;
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_BIGINT;
  }

protected:
  /**
   * Native driver value
   */
  cass_int64_t integer_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_BIGINT, value_type)
      << "Invalid Value Type: Value is not a big integer [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_BIGINT, value_type)
      << "Invalid Data Type: Value->DataType is not a big integer";

    // Get the 64-bit integer
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_int64(value, &integer_))
        << "Unable to Get 64-bit Integer: Invalid error code returned";
      is_null_ = false;
    }
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_INTEGER_HPP__
