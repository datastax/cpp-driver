/*
  Copyright 2014 DataStax

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

#ifndef __CASS_STATEMENT_HPP_INCLUDED__
#define __CASS_STATEMENT_HPP_INCLUDED__

#include "request.hpp"
#include "buffer_value.hpp"
#include "buffer_collection.hpp"

#include <vector>
#include <string>

#define CASS_VALUE_CHECK_INDEX(i)              \
  if (index >= values_count()) {               \
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS; \
  }

namespace cass {

class Statement : public Request {
public:
  Statement(uint8_t opcode, uint8_t kind)
      : Request(opcode)
      , consistency_(CASS_CONSISTENCY_ONE)
      , serial_consistency_(CASS_CONSISTENCY_ANY)
      , page_size_(-1)
      , kind_(kind) {}

  Statement(uint8_t opcode, uint8_t kind, size_t value_count)
      : Request(opcode)
      , values_(value_count)
      , consistency_(CASS_CONSISTENCY_ONE)
      , serial_consistency_(CASS_CONSISTENCY_ANY)
      , page_size_(-1)
      , kind_(kind) {}

  virtual ~Statement() {}

  int16_t consistency() const { return consistency_; }

  void set_consistency(int16_t consistency) { consistency_ = consistency; }

  int16_t serial_consistency() const { return serial_consistency_; }

  void set_serial_consistency(int16_t serial_consistency) {
    serial_consistency_ = serial_consistency;
  }

  int page_size() const { return page_size_; }

  const std::string paging_state() const { return paging_state_; }

  uint8_t kind() const { return kind_; }

  void set_query(const std::string& query) {
    query_or_prepared_id_ = query;
  }

  void set_query(const char* query, size_t size) {
    query_or_prepared_id_.assign(query, size);
  }

  const std::string& query() const { return query_or_prepared_id_; }

  void set_prepared_id(const std::string& prepared_id) {
    query_or_prepared_id_ = prepared_id;
  }

  const std::string& prepared_id() const { return query_or_prepared_id_; }

  size_t values_count() const { return values_.size(); }

#define BIND_FIXED_TYPE(DeclType, EncodeType, Size)            \
  CassError bind_##EncodeType(size_t index, const DeclType& value) { \
    CASS_VALUE_CHECK_INDEX(index);                             \
    BufferValue buf(sizeof(int32_t) + sizeof(DeclType));       \
    size_t pos = buf.encode_int32(0, sizeof(DeclType));        \
    buf.encode_##EncodeType(pos, value);                       \
    values_[index] = buf;                                      \
    return CASS_OK;                                            \
  }

  BIND_FIXED_TYPE(int32_t, int32, 4)
  BIND_FIXED_TYPE(int64_t, int64, 8)
  BIND_FIXED_TYPE(float, float, 4)
  BIND_FIXED_TYPE(double, double, 8)
  BIND_FIXED_TYPE(bool, byte, 1)
#undef BIND_FIXED_TYPE

  CassError bind_null(size_t index) {
    CASS_VALUE_CHECK_INDEX(index);
    BufferValue buf(sizeof(int32_t));
    buf.encode_int32(0, -1);
    values_[index] = buf;
    return CASS_OK;
  }

  CassError bind(size_t index, const char* value, size_t value_length) {
    CASS_VALUE_CHECK_INDEX(index);
    BufferValue buf(sizeof(int32_t) + value_length);
    size_t pos = buf.encode_int32(0, value_length);
    buf.copy(pos, value, value_length);
    values_[index] = buf;
    return CASS_OK;
  }

  CassError bind(size_t index, const uint8_t* value, size_t value_length) {
    return bind(index, reinterpret_cast<const char*>(value), value_length);
  }

  CassError bind(size_t index, const CassUuid value) {
    CASS_VALUE_CHECK_INDEX(index);
    BufferValue buf(sizeof(int32_t) + sizeof(CassUuid));
    size_t pos = buf.encode_int32(0, sizeof(CassUuid));
    buf.copy(pos, value, sizeof(CassUuid));
    values_[index] = buf;
    return CASS_OK;
  }

  CassError bind(size_t index, int32_t scale, const uint8_t* varint,
                 size_t varint_length) {
    CASS_VALUE_CHECK_INDEX(index);
    BufferValue buf(sizeof(int32_t) + sizeof(int32_t) + varint_length);
    size_t pos = buf.encode_int32(0, sizeof(int32_t) + varint_length);
    pos = buf.encode_int32(pos, scale);
    buf.copy(pos, varint, varint_length);
    values_[index] = buf;
    return CASS_OK;
  }

  CassError bind(size_t index, const BufferCollection* collection) {
    CASS_VALUE_CHECK_INDEX(index);
    if (collection->is_map() && collection->item_count() % 2 != 0) {
      return CASS_ERROR_LIB_INVALID_ITEM_COUNT;
    }
    values_[index] = BufferValue(collection);
    return CASS_OK;
  }

  CassError bind(size_t index, size_t output_size, uint8_t** output) {
    CASS_VALUE_CHECK_INDEX(index);
    BufferValue buf(4 + output_size);
    size_t pos = buf.encode_int32(0, output_size);
    *output = reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data() + pos));
    values_[index] = buf;
    return CASS_OK;
  }

  int32_t encode_values(int version, BufferValueVec*  bufs) const;

private:
  typedef std::vector<BufferValue> ValueVec;

  ValueVec values_;
  int16_t consistency_;
  int16_t serial_consistency_;
  int page_size_;
  std::string paging_state_;
  uint8_t kind_;
  std::string query_or_prepared_id_;
};

} // namespace cass

#endif
