/*
  Copyright (c) 2014-2016 DataStax

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
#ifndef __TEST_UUID_HPP__
#define __TEST_UUID_HPP__
#include "value_interface.hpp"
#include "exception.hpp"
#include "test_utils.hpp"

#include <limits>

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace test {
namespace driver {

/**
 * UUID wrapped value
 */
class Uuid : public COMPARABLE_VALUE_INTERFACE(CassUuid, Uuid) {
public:
  Uuid()
    : is_null_(true) {
    set_uuid_string();
  }

  Uuid(CassUuid uuid)
    : uuid_(uuid)
    , is_null_(false) {
    set_uuid_string();
  }

  Uuid(const CassValue* value)
    : is_null_(false) {
    initialize(value);
    set_uuid_string();
  }

  Uuid(const std::string& value)
    : is_null_(false) {
    std::string value_trim = Utils::trim(value);
    bool is_valid = true;

    // Determine if the value is NULL or valid (default is 0 otherwise)
    if (value_trim.empty() ||
      value_trim.compare("null") == 0) {
      is_null_ = true;
    } else if (cass_uuid_from_string(value_trim.c_str(), &uuid_) != CASS_OK) {
      is_valid = false;
    }

    set_uuid_string();
    if (!is_valid) {
      LOG_ERROR("Invalid UUID " << value_trim << ": Using default " << str());
    }
  }

  Uuid(const CassRow* row, size_t column_index)
    : is_null_(false) {
    initialize(row, column_index);
    set_uuid_string();
  }

  virtual const char* c_str() const {
    return uuid_string_.c_str();
  }

  virtual std::string cql_type() const {
    return std::string("uuid");
  }

  virtual std::string cql_value() const {
    return uuid_string_;
  }

  /**
   * Comparison operation for driver UUIDs
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  virtual int compare(const CassUuid& rhs) const {
    if (uuid_.clock_seq_and_node < rhs.clock_seq_and_node) return -1;
    if (uuid_.clock_seq_and_node > rhs.clock_seq_and_node) return 1;

    if (uuid_.time_and_version < rhs.time_and_version) return -1;
    if (uuid_.time_and_version > rhs.time_and_version) return 1;

    return 0;
  }

  /**
   * Comparison operation for UUIDs
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  virtual int compare(const Uuid& rhs) const {
    if (is_null_ && rhs.is_null_) return 0;
    return compare(rhs.uuid_);
  }

  void statement_bind(Statement statement, size_t index) {
    if (is_null_) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_uuid(statement.get(), index, uuid_));
    }
  }

  bool is_null() const {
    return is_null_;
  }

  /**
   * Get the minimum value allowed for a UUID
   *
   * @return Minimum value for UUID
   */
  static Uuid min() {
    // Create the minimum native driver value for UUID
    CassUuid value;
    value.clock_seq_and_node = 0;
    value.time_and_version = 0;

    // Return the wrapped value
    return Uuid(value);
  }

  /**
   * Get the maximum value allowed for a UUID
   *
   * @return Maximum value for UUID
   */
  static Uuid max() {
    // Create the maximum native driver value for UUID
    CassUuid value;
    value.clock_seq_and_node = std::numeric_limits<cass_uint64_t>::max();
    value.time_and_version = std::numeric_limits<cass_uint64_t>::max();

    // Return the wrapped value
    return Uuid(value);
  }

  virtual std::string str() const {
    return uuid_string_;
  }

  virtual CassUuid value() const {
    return uuid_;
  }

  virtual CassValueType value_type() const {
    return CASS_VALUE_TYPE_UUID;
  }

  /**
   * Get the version of the UUID
   *
   * @return The version of the UUID (v1 or v4)
   */
  cass_uint8_t version() {
    return cass_uuid_version(uuid_);
  }

protected:
  /**
   * Native driver value
   */
  CassUuid uuid_;
  /**
   * Native driver value as string
   */
  std::string uuid_string_;
  /**
   * Flag to determine if value is NULL
   */
  bool is_null_;

  virtual void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_UUID, value_type)
      << "Invalid Value Type: Value is not a UUID [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_UUID, value_type)
      << "Invalid Data Type: Value->DataType is not a UUID";

    // Get the UUID
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_uuid(value, &uuid_))
        << "Unable to Get Uuid: Invalid error code returned";
      is_null_ = false;
    }
  }

  virtual void initialize(const CassRow* row, size_t column_index) {
    ASSERT_TRUE(row != NULL) << "Invalid Row: Row should not be null";
    initialize(cass_row_get_column(row, column_index));
  }

  /**
   * Set the string value of the UUID
   */
  void set_uuid_string() {
    if (is_null_) {
      uuid_string_ = "null";
    } else {
      char buffer[CASS_UUID_STRING_LENGTH];
      cass_uuid_string(uuid_, buffer);
      uuid_string_ = buffer;
    }
  }
};

/**
 * v1 UUID (time based) wrapped value
 */
class TimeUuid : public Uuid {
public:
  TimeUuid()
    : Uuid() {}

  TimeUuid(CassUuid uuid)
    : Uuid(uuid) {}

  TimeUuid(const CassValue* value)
    : Uuid() {
    is_null_ = false;
    initialize(value);
    set_uuid_string();
  }

  TimeUuid(const std::string& value)
    : Uuid(value) {}

  TimeUuid(const CassRow* row, size_t column_index)
    : Uuid() {
    is_null_ = false;
    initialize(row, column_index);
    set_uuid_string();
  }

  std::string cql_type() const {
    return std::string("timeuuid");
  }

  /**
   * Sets the TimeUuid to the maximum value for the specified timestamp
   *
   * @param timestamp Timestamp to set maximum v1 UUID value
   */
  void max(cass_uint64_t timestamp) {
    cass_uuid_max_from_time(timestamp, &uuid_);
    set_uuid_string();
  }

  /**
   * Sets the TimeUuid to the minimum value for the specified timestamp
   *
   * @param timestamp Timestamp to set minimum v1 UUID value
   */
  void min(cass_uint64_t timestamp) {
    cass_uuid_min_from_time(timestamp, &uuid_);
    set_uuid_string();
  }

  /**
   * Get the timestamp
   *
   * @return The timestamp in milliseconds since the epoch
   */
  cass_uint64_t timestamp() {
    return cass_uuid_timestamp(uuid_);
  }

  CassValueType value_type() const {
    return CASS_VALUE_TYPE_TIMEUUID;
  }

protected:
  void initialize(const CassValue* value) {
    // Ensure the value types
    ASSERT_TRUE(value != NULL) << "Invalid CassValue: Value should not be null";
    CassValueType value_type = cass_value_type(value);
    ASSERT_EQ(CASS_VALUE_TYPE_TIMEUUID, value_type)
      << "Invalid Value Type: Value is not a time UUID [" << value_type << "]";
    const CassDataType* data_type = cass_value_data_type(value);
    value_type = cass_data_type_type(data_type);
    ASSERT_EQ(CASS_VALUE_TYPE_TIMEUUID, value_type)
      << "Invalid Data Type: Value->DataType is not a time UUID";

    // Get the time UUID
    if (cass_value_is_null(value)) {
      is_null_ = true;
    } else {
      ASSERT_EQ(CASS_OK, cass_value_get_uuid(value, &uuid_))
        << "Unable to Get Uuid: Invalid error code returned";
      is_null_ = false;
    }
  }

  void initialize(const CassRow* row, size_t column_index) {
    ASSERT_TRUE(row != NULL) << "Invalid Row: Row should not be null";
    initialize(cass_row_get_column(row, column_index));
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_UUID_HPP__
