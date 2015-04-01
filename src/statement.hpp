/*
  Copyright (c) 2014-2015 DataStax

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

namespace cass {

struct CassNull {};

struct CassBytes {
  const cass_byte_t* data;
  size_t size;
};

struct CassString {
  const char* data;
  size_t length;
};

struct CassCustom {
  uint8_t** output;
  size_t output_size;
};

struct CassDecimal {
  const cass_byte_t* varint;
  size_t varint_size;
  cass_int32_t scale;
};

class Statement : public RoutableRequest {
public:
  Statement(uint8_t opcode, uint8_t kind, size_t value_count = 0)
      : RoutableRequest(opcode)
      , values_(value_count)
      , skip_metadata_(false)
      , page_size_(-1)
      , kind_(kind) {}

  Statement(uint8_t opcode, uint8_t kind, size_t value_count,
            const std::vector<size_t>& key_indices,
            const std::string& keyspace)
      : RoutableRequest(opcode, keyspace)
      , values_(value_count)
      , skip_metadata_(false)
      , page_size_(-1)
      , kind_(kind)
      , key_indices_(key_indices) {}

  virtual ~Statement() {}

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

  void add_key_index(size_t index) { key_indices_.push_back(index); }

  virtual bool get_routing_key(std::string* routing_key)  const;

#define BIND_FIXED_TYPE(DeclType, EncodeType)						\
  CassError bind(size_t index, const DeclType& value) { \
    CASS_VALUE_CHECK_INDEX(index);                      \
    Buffer buf(sizeof(int32_t) + sizeof(DeclType));     \
    size_t pos = buf.encode_int32(0, sizeof(DeclType)); \
    buf.encode_##EncodeType(pos, value);                \
    values_[index] = buf;                               \
    return CASS_OK;                                     \
  }

  BIND_FIXED_TYPE(int32_t, int32)
  BIND_FIXED_TYPE(cass_int64_t, int64)
  BIND_FIXED_TYPE(float, float)
  BIND_FIXED_TYPE(double, double)
  BIND_FIXED_TYPE(bool, bool)
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

  CassError bind(size_t index, CassUuid value) {
    CASS_VALUE_CHECK_INDEX(index);
    Buffer buf(sizeof(int32_t) + sizeof(CassUuid));
    size_t pos = buf.encode_int32(0, sizeof(CassUuid));
    buf.encode_uuid(pos, value);
    values_[index] = buf;
    return CASS_OK;
  }

  CassError bind(size_t index, CassInet value) {
    return bind(index, value.address, value.address_length);
  }

  CassError bind(size_t index, CassDecimal value) {
    return bind(index, value.varint, value.varint_size, value.scale);
  }

  CassError bind(size_t index, const uint8_t* varint, size_t varint_size, int32_t scale) {
    CASS_VALUE_CHECK_INDEX(index);
    Buffer buf(sizeof(int32_t) + sizeof(int32_t) + varint_size);
    size_t pos = buf.encode_int32(0, sizeof(int32_t) + varint_size);
    pos = buf.encode_int32(pos, scale);
    buf.copy(pos, varint, varint_size);
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
    *(custom.output) = reinterpret_cast<uint8_t*>(buf.data() + pos);
    values_[index] = buf;
    return CASS_OK;
  }

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

  int32_t encode_values(int version, BufferVec*  bufs) const;

private:
  typedef BufferVec ValueVec;

  ValueVec values_;
  bool skip_metadata_;
  int32_t page_size_;
  std::string paging_state_;
  uint8_t kind_;
  std::vector<size_t> key_indices_;

private:
  DISALLOW_COPY_AND_ASSIGN(Statement);
};

} // namespace cass

#endif
