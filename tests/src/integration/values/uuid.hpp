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

#ifndef __TEST_UUID_HPP__
#define __TEST_UUID_HPP__
#include "nullable_value.hpp"
#include "test_utils.hpp"

#include <limits>

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

namespace test { namespace driver { namespace values {

/**
 * UUID wrapped value
 */
class Uuid {
public:
  typedef std::string ConvenienceType;
  typedef CassUuid ValueType;

  Uuid() {}

  Uuid(const ConvenienceType& value) {
    std::string value_trim = Utils::trim(value);

    // Determine if the value is valid (default is 0 otherwise)
    CassError error_code = cass_uuid_from_string(value_trim.c_str(), &uuid_);
    EXPECT_EQ(CASS_OK, error_code) << "Invalid UUID " << value_trim << ": Using default " << str();
  }

  /**
   * Constructor for native driver type
   *
   * @param uuid Native driver value
   */
  Uuid(CassUuid uuid)
      : uuid_(uuid) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_uuid(collection.get(), uuid_));
  }

  static std::string cql_type() { return "uuid"; }

  std::string cql_value() const { return str(); }

  /**
   * Comparison operation for driver UUIDs
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const CassUuid& rhs) const {
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
  int compare(const Uuid& rhs) const { return compare(rhs.uuid_); }

  void initialize(const CassValue* value) {
    // Get the UUID
    ASSERT_EQ(CASS_OK, cass_value_get_uuid(value, &uuid_))
        << "Unable to Get Uuid: Invalid error code returned";
  }

  static Uuid max() {
    Uuid uuid;
    uuid.uuid_.clock_seq_and_node = std::numeric_limits<cass_uint64_t>::max();
    uuid.uuid_.time_and_version = std::numeric_limits<cass_uint64_t>::max();
    return uuid;
  }

  static Uuid min() {
    Uuid uuid;
    uuid.uuid_.clock_seq_and_node = 0;
    uuid.uuid_.time_and_version = 0;
    return uuid;
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_uuid(tuple.get(), index, uuid_));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_user_type_set_uuid_by_name(user_type.get(), name.c_str(), uuid_));
  }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_uuid(statement.get(), index, uuid_));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_uuid_by_name(statement.get(), name.c_str(), uuid_));
  }

  std::string str() const {
    char buffer[CASS_UUID_STRING_LENGTH];
    cass_uuid_string(uuid_, buffer);
    return buffer;
  }

  static std::string supported_server_version() { return "1.2.0"; }

  CassUuid value() const { return uuid_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_UUID; }

  /**
   * Get the version of the UUID
   *
   * @return The version of the UUID (v1 or v4)
   */
  cass_uint8_t version() { return cass_uuid_version(uuid_); }

protected:
  /**
   * Native driver value
   */
  CassUuid uuid_;
};

/**
 * v1 UUID (time based) wrapped value
 */
class TimeUuid : public Uuid {
public:
  TimeUuid() {}

  TimeUuid(const ConvenienceType& value)
      : Uuid(value) {}

  /**
   * Constructor for native driver type
   *
   * @param uuid Native driver value
   */
  TimeUuid(CassUuid uuid)
      : Uuid(uuid) {}

  std::string cql_type() const { return "timeuuid"; }

  void initialize(const CassValue* value) {
    // Get the time UUID
    ASSERT_EQ(CASS_OK, cass_value_get_uuid(value, &uuid_))
        << "Unable to Get Uuid: Invalid error code returned";
  }

  static TimeUuid max() { return TimeUuid::max(std::numeric_limits<uint64_t>::max()); }

  /**
   * Gets a TimeUuid maximum value for the specified timestamp
   *
   * @param timestamp Timestamp to set maximum v1 UUID value
   * @return TimeUuid max for timestamp value
   */
  static TimeUuid max(uint64_t timestamp) {
    TimeUuid timeuuid;
    cass_uuid_max_from_time(timestamp, &timeuuid.uuid_);
    return timeuuid;
  }

  static TimeUuid min() { return TimeUuid::min(std::numeric_limits<uint64_t>::min()); }

  /**
   * Gets a TimeUuid minimum value for the specified timestamp
   *
   * @param timestamp Timestamp to set minimum v1 UUID value
   * @return TimeUuid min for timestamp value
   */
  static TimeUuid min(uint64_t timestamp) {
    TimeUuid timeuuid;
    cass_uuid_min_from_time(timestamp, &timeuuid.uuid_);
    return timeuuid;
  }

  /**
   * Get the timestamp
   *
   * @return The timestamp in milliseconds since the epoch
   */
  cass_uint64_t timestamp() { return cass_uuid_timestamp(uuid_); }

  CassValueType value_type() const { return CASS_VALUE_TYPE_TIMEUUID; }
};

inline std::ostream& operator<<(std::ostream& output_stream, const Uuid& value) {
  output_stream << value.cql_value();
  return output_stream;
}

inline std::ostream& operator<<(std::ostream& output_stream, const TimeUuid& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_UUID_HPP__
