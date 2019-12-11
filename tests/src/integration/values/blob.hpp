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

#ifndef __TEST_BLOB_HPP__
#define __TEST_BLOB_HPP__

#include "nullable_value.hpp"

#include <cstring>

namespace test { namespace driver { namespace values {

/**
 * Blob wrapped value
 */
class Blob {
public:
  typedef std::string ConvenienceType;
  typedef std::string ValueType;

  Blob() {}

  Blob(const ConvenienceType& blob)
      : blob_(blob) {}

  void append(Collection collection) {
    ASSERT_EQ(CASS_OK, cass_collection_append_bytes(collection.get(), data(), size()));
  }

  std::string cql_type() const { return "blob"; }

  std::string cql_value() const { return "'0x" + str() + "'"; }

  /**
   * Comparison operation for driver bytes
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const std::string& rhs) const {
    // Convert the rhs into a compatible format
    const cass_byte_t* rhs_data = reinterpret_cast<const cass_byte_t*>(rhs.data());
    size_t rhs_size = rhs.size() / sizeof(cass_byte_t);

    // Perform the comparison
    if (size() < rhs_size) return -1;
    if (size() > rhs_size) return 1;
    return std::memcmp(data(), rhs_data, rhs_size);
  }

  /**
   * Comparison operation for blob
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Blob& rhs) const { return compare(rhs.blob_); }

  /**
   * Helper method to return data for the blob type
   *
   * @return Blob data
   */
  const cass_byte_t* data() const { return reinterpret_cast<const cass_byte_t*>(blob_.data()); }

  void initialize(const CassValue* value) {
    const cass_byte_t* bytes = NULL;
    size_t size;
    ASSERT_EQ(CASS_OK, cass_value_get_bytes(value, &bytes, &size))
        << "Unable to Get Blob: Invalid error code returned";
    blob_.assign(reinterpret_cast<const char*>(bytes), size * sizeof(cass_byte_t));
  }

  void set(Tuple tuple, size_t index) {
    ASSERT_EQ(CASS_OK, cass_tuple_set_bytes(tuple.get(), index, data(), size()));
  }

  void set(UserType user_type, const std::string& name) {
    ASSERT_EQ(CASS_OK,
              cass_user_type_set_bytes_by_name(user_type.get(), name.c_str(), data(), size()));
  }

  /**
   * Helper method to determine the size of the blob (amount of data)
   *
   * @return Blob size (e.g.length)
   */
  size_t size() const { return blob_.size() / sizeof(cass_byte_t); }

  void statement_bind(Statement statement, size_t index) {
    ASSERT_EQ(CASS_OK, cass_statement_bind_bytes(statement.get(), index, data(), size()));
  }

  void statement_bind(Statement statement, const std::string& name) {
    ASSERT_EQ(CASS_OK,
              cass_statement_bind_bytes_by_name(statement.get(), name.c_str(), data(), size()));
  }

  std::string str() const {
    // Convert the bytes to their hex value
    std::stringstream value;
    value << std::hex << std::setfill('0');
    for (size_t i = 0; i < blob_.size(); ++i) {
      value << std::setw(2) << static_cast<unsigned>(blob_[i]);
    }

    if (value.str().size() == 0) {
      return "";
    }
    return value.str();
  }

  static std::string supported_server_version() { return "1.2.0"; }

  ValueType value() const { return blob_; }

  CassValueType value_type() const { return CASS_VALUE_TYPE_BLOB; }

protected:
  /**
   * Native driver value
   */
  std::string blob_;
};

inline std::ostream& operator<<(std::ostream& output_stream, const Blob& value) {
  output_stream << value.cql_value();
  return output_stream;
}

}}} // namespace test::driver::values

#endif // __TEST_BLOB_HPP__
