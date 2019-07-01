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

#ifndef __TEST_INTEGER_HPP__
#define __TEST_INTEGER_HPP__
#include "nullable_value.hpp"
#include "test_utils.hpp"

#include <limits>
#include <sstream>

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace test { namespace driver { namespace values {

/**
 * 8-bit integer wrapped value
 */
class TinyInteger {
public:
  typedef cass_int8_t ConvenienceType;
  typedef cass_int8_t ValueType;

  TinyInteger()
      : integer_(0) {}

  TinyInteger(ConvenienceType integer)
      : integer_(integer) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_int8(collection.get(), integer_));
  }

  std::string cql_type() const { return "tinyint"; }

  std::string cql_value() const { return str(); }

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
  int compare(const TinyInteger& rhs) const { return compare(rhs.integer_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_int8(value, &integer_))
        << "Unable to Get 8-bit Integer: Invalid error code returned";
  }

  static TinyInteger max() { return TinyInteger(std::numeric_limits<cass_int8_t>::max()); }

  static TinyInteger min() { return TinyInteger(std::numeric_limits<cass_int8_t>::min()); }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_int8(tuple.get(), index, integer_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_int8_by_name(user_type.get(), name.c_str(), integer_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_int8(statement.get(), index, integer_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_int8_by_name(statement.get(), name.c_str(), integer_));
  }

  std::string str() const {
    std::stringstream integer_string;
    integer_string << static_cast<int32_t>(integer_);
    return integer_string.str();
  }

  static std::string supported_server_version() { return "2.2.3"; }

  ValueType value() const { return integer_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_TINY_INT; }

protected:
  /**
   * Native driver value
   */
  cass_int8_t integer_;
};

/**
 * 16-bit integer wrapped value
 */
class SmallInteger {
public:
  typedef cass_int16_t ConvenienceType;
  typedef cass_int16_t ValueType;

  SmallInteger()
      : integer_(0) {}

  SmallInteger(ConvenienceType integer)
      : integer_(integer) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_int16(collection.get(), integer_));
  }

  std::string cql_type() const { return "smallint"; }

  std::string cql_value() const { return str(); }

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
  int compare(const SmallInteger& rhs) const { return compare(rhs.integer_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_int16(value, &integer_))
        << "Unable to Get 16-bit Integer: Invalid error code returned";
  }

  static SmallInteger max() { return SmallInteger(std::numeric_limits<cass_int16_t>::max()); }

  static SmallInteger min() { return SmallInteger(std::numeric_limits<cass_int16_t>::min()); }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_int16(tuple.get(), index, integer_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_int16_by_name(user_type.get(), name.c_str(), integer_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_int16(statement.get(), index, integer_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_int16_by_name(statement.get(), name.c_str(), integer_));
  }

  std::string str() const {
    std::stringstream integer_string;
    integer_string << integer_;
    return integer_string.str();
  }

  static std::string supported_server_version() { return "2.2.3"; }

  ValueType value() const { return integer_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_SMALL_INT; }

protected:
  /**
   * Native driver value
   */
  cass_int16_t integer_;
};

/**
 * 32-bit integer wrapped value
 */
class Integer {
public:
  typedef cass_int32_t ConvenienceType;
  typedef cass_int32_t ValueType;

  Integer()
      : integer_(0) {}

  Integer(ConvenienceType integer)
      : integer_(integer) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_int32(collection.get(), integer_));
  }

  std::string cql_type() const { return "int"; }

  std::string cql_value() const { return str(); }

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
  int compare(const Integer& rhs) const { return compare(rhs.integer_); }

  static Integer max() { return Integer(std::numeric_limits<cass_int32_t>::max()); }

  static Integer min() { return Integer(std::numeric_limits<cass_int32_t>::min()); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_int32(value, &integer_))
        << "Unable to Get 32-bit Integer: Invalid error code returned";
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_int32(tuple.get(), index, integer_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_int32_by_name(user_type.get(), name.c_str(), integer_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_int32(statement.get(), index, integer_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_int32_by_name(statement.get(), name.c_str(), integer_));
  }

  std::string str() const {
    std::stringstream integer_string;
    integer_string << integer_;
    return integer_string.str();
  }

  static std::string supported_server_version() { return "1.2.0"; }

  ValueType value() const { return integer_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_INT; }

protected:
  /**
   * Native driver value
   */
  cass_int32_t integer_;
};

/**
 * 64-bit integer wrapped value (e.g. bigint)
 */
class BigInteger {
public:
  typedef cass_int64_t ConvenienceType;
  typedef cass_int64_t ValueType;

  BigInteger()
      : integer_(0) {}

  BigInteger(ConvenienceType integer)
      : integer_(integer) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_int64(collection.get(), integer_));
  }

  std::string cql_type() const { return "bigint"; }

  std::string cql_value() const { return str(); }

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
  int compare(const BigInteger& rhs) const { return compare(rhs.integer_); }

  void initialize(const CassValue* value) {
    ASSERT_EQ(CASS_OK, cass_value_get_int64(value, &integer_))
        << "Unable to Get 64-bit Integer: Invalid error code returned";
  }

  static BigInteger max() { return BigInteger(std::numeric_limits<cass_int64_t>::max()); }

  static BigInteger min() { return BigInteger(std::numeric_limits<cass_int64_t>::min()); }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_int64(tuple.get(), index, integer_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_int64_by_name(user_type.get(), name.c_str(), integer_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_int64(statement.get(), index, integer_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_int64_by_name(statement.get(), name.c_str(), integer_));
  }

  std::string str() const {
    std::stringstream integer_string;
    integer_string << integer_;
    return integer_string.str();
  }

  static std::string supported_server_version() { return "1.2.0"; }

  ValueType value() const { return integer_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_BIGINT; }

  BigInteger operator-(const BigInteger& rhs) { return integer_ - rhs.integer_; }

  BigInteger operator+(const BigInteger& rhs) { return integer_ + rhs.integer_; }

protected:
  /**
   * Native driver value
   */
  cass_int64_t integer_;
};

/**
 * Counter (e.g. bigint)
 */
class Counter : public BigInteger {
public:
  Counter()
      : BigInteger(0) {}

  Counter(ConvenienceType integer)
      : BigInteger(integer) {}

  CassValueType value_type() const { return CASS_VALUE_TYPE_COUNTER; }
};

inline std::ostream& operator<<(std::ostream& output_stream, const TinyInteger& value) {
  output_stream << value.cql_value();
  return output_stream;
}

inline std::ostream& operator<<(std::ostream& output_stream, const SmallInteger& value) {
  output_stream << value.cql_value();
  return output_stream;
}

inline std::ostream& operator<<(std::ostream& output_stream, const Integer& value) {
  output_stream << value.cql_value();
  return output_stream;
}

inline std::ostream& operator<<(std::ostream& output_stream, const BigInteger& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_INTEGER_HPP__
