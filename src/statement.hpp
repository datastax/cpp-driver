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

#include "buffer.hpp"
#include "buffer_collection.hpp"
#include "macros.hpp"
#include "request.hpp"

#include <vector>
#include <string>

#define CASS_VALUE_CHECK_INDEX(i)              \
  if (index >= values_count()) {               \
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS; \
  }

struct CassCustom {
  uint8_t** output;
  size_t output_size;
};

struct CassNull {};

namespace cass {

class Statement : public Request {
public:
  Statement(uint8_t opcode, uint8_t kind)
      : Request(opcode)
      , consistency_(CASS_CONSISTENCY_ONE)
      , serial_consistency_(CASS_CONSISTENCY_ANY)
      , skip_metadata_(false)
      , page_size_(-1)
      , kind_(kind) {}

  Statement(uint8_t opcode, uint8_t kind, size_t value_count)
      : Request(opcode)
      , values_(value_count)
      , consistency_(CASS_CONSISTENCY_ONE)
      , serial_consistency_(CASS_CONSISTENCY_ANY)
      , skip_metadata_(false)
      , page_size_(-1)
      , kind_(kind) {}

  virtual ~Statement() {}

  int16_t consistency() const { return consistency_; }

  void set_consistency(int16_t consistency) { consistency_ = consistency; }

  int16_t serial_consistency() const { return serial_consistency_; }

  void set_serial_consistency(int16_t serial_consistency) {
    serial_consistency_ = serial_consistency;
  }

  void set_skip_metadata(bool skip_metadata) {
    skip_metadata_ = skip_metadata;
  }

  bool skip_metadata() const {
    return skip_metadata_;
  }

  int32_t page_size() const { return page_size_; }

  void set_page_size(int32_t page_size) { page_size_ = page_size; }

  const std::string paging_state() const { return paging_state_; }

  void set_paging_state(const std::string& paging_state) {
    paging_state_ = paging_state;
  }

  uint8_t kind() const { return kind_; }

  virtual const std::string& query() const = 0;

  size_t values_count() const { return values_.size(); }

#define BIND_FIXED_TYPE(DeclType, EncodeType, Size)                  \
  CassError bind(size_t index, const DeclType& value) { \
    CASS_VALUE_CHECK_INDEX(index);                                   \
    Buffer buf(sizeof(int32_t) + sizeof(DeclType));                  \
    size_t pos = buf.encode_int32(0, sizeof(DeclType));              \
    buf.encode_##EncodeType(pos, value);                             \
    values_[index] = buf;                                            \
    return CASS_OK;                                                  \
  }

  BIND_FIXED_TYPE(int32_t, int32, sizeof(int32_t))
  BIND_FIXED_TYPE(cass_int64_t, int64, sizeof(cass_int64_t))
  BIND_FIXED_TYPE(float, float, sizeof(float))
  BIND_FIXED_TYPE(double, double, sizeof(double))
  BIND_FIXED_TYPE(bool, bool, sizeof(uint8_t))
#undef BIND_FIXED_TYPE

  CassError bind(size_t index, CassNull) {
    CASS_VALUE_CHECK_INDEX(index);
    Buffer buf(sizeof(int32_t));
    buf.encode_int32(0, -1); // [bytes] "null"
    values_[index] = buf;
    return CASS_OK;
  }

  CassError bind(size_t index, CassString value) {
    return bind(index, value.data, value.length);
  }

  CassError bind(size_t index, CassBytes value) {
    return bind(index, value.data, value.size);
  }

  CassError bind(size_t index, const CassUuid value) {
    CASS_VALUE_CHECK_INDEX(index);
    Buffer buf(sizeof(int32_t) + sizeof(CassUuid));
    size_t pos = buf.encode_int32(0, sizeof(CassUuid));
    buf.copy(pos, value, sizeof(CassUuid));
    values_[index] = buf;
    return CASS_OK;
  }

  CassError bind(size_t index, CassInet value) {
    return bind(index, value.address, value.address_length);
  }

  CassError bind(size_t index, CassDecimal value) {
    CASS_VALUE_CHECK_INDEX(index);
    Buffer buf(sizeof(int32_t) + sizeof(int32_t) + value.varint.size);
    size_t pos = buf.encode_int32(0, sizeof(int32_t) + value.varint.size);
    pos = buf.encode_int32(pos, value.scale);
    buf.copy(pos, value.varint.data, value.varint.size);
    values_[index] = buf;
    return CASS_OK;
  }

  CassError bind(size_t index, const BufferCollection* collection) {
    CASS_VALUE_CHECK_INDEX(index);
    if (collection->is_map() && collection->item_count() % 2 != 0) {
      return CASS_ERROR_LIB_INVALID_ITEM_COUNT;
    }
    values_[index] = Buffer(collection);
    return CASS_OK;
  }

  CassError bind(size_t index, CassCustom custom) {
    CASS_VALUE_CHECK_INDEX(index);
    Buffer buf(4 + custom.output_size);
    size_t pos = buf.encode_int32(0, custom.output_size);
    *(custom.output) = reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data() + pos));
    values_[index] = buf;
    return CASS_OK;
  }

  int32_t encode_values(int version, BufferVec*  bufs) const;

private:
  CassError bind(size_t index, const char* value, size_t value_length) {
    CASS_VALUE_CHECK_INDEX(index);
    Buffer buf(sizeof(int32_t) + value_length);
    size_t pos = buf.encode_int32(0, value_length);
    buf.copy(pos, value, value_length);
    values_[index] = buf;
    return CASS_OK;
  }

  CassError bind(size_t index, const uint8_t* value, size_t value_length) {
    return bind(index, reinterpret_cast<const char*>(value), value_length);
  }

private:
  typedef std::vector<Buffer> ValueVec;

  ValueVec values_;
  int16_t consistency_;
  int16_t serial_consistency_;
  bool skip_metadata_;
  int32_t page_size_;
  std::string paging_state_;
  uint8_t kind_;

private:
  DISALLOW_COPY_AND_ASSIGN(Statement);
};

} // namespace cass

#endif
