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
#include "buffer.hpp"
#include "collection.hpp"

#include <vector>
#include <string>

#define CASS_VALUE_CHECK_INDEX(i)              \
  if (index >= size()) {                       \
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS; \
  }

namespace cass {

class Statement : public Request {
public:
  Statement(uint8_t opcode)
      : Request(opcode)
      , consistency_(CASS_CONSISTENCY_ONE)
      , serial_consistency_(CASS_CONSISTENCY_ANY)
      , page_size_(-1) {}

  Statement(uint8_t opcode, size_t value_count)
      : Request(opcode)
      , values_(value_count)
      , consistency_(CASS_CONSISTENCY_ONE)
      , serial_consistency_(CASS_CONSISTENCY_ANY)
      , page_size_(-1) {}

  virtual ~Statement() {}

  int16_t consistency() const { return consistency_; }

  void set_consistency(int16_t consistency) { consistency_ = consistency; }

  int16_t serial_consistency() const { return serial_consistency_; }

  void set_serial_consistency(int16_t serial_consistency) {
    serial_consistency_ = serial_consistency;
  }

  int page_size() const { return page_size_; }

  const std::string paging_state() const { return paging_state_; }

  virtual uint8_t kind() const = 0;

  virtual void statement(const std::string& statement) = 0;

  virtual void statement(const char* statement, size_t size) = 0;

  virtual const char* statement() const = 0;

  virtual size_t statement_size() const = 0;

  void resize(size_t size) { values_.resize(size); }

  size_t size() const { return values_.size(); }

  bool is_empty() const { return values_.empty(); }

#define BIND_FIXED_TYPE(DeclType, EncodeType, Name)            \
  CassError bind_##Name(size_t index, const DeclType& value) { \
    CASS_VALUE_CHECK_INDEX(index);                             \
    Buffer buffer(sizeof(DeclType));                           \
    encode_##EncodeType(buffer.data(), value);                 \
    values_[index] = buffer;                                   \
    return CASS_OK;                                            \
  }

  BIND_FIXED_TYPE(int32_t, int, int32)
  BIND_FIXED_TYPE(int64_t, int64, int64)
  BIND_FIXED_TYPE(float, float, float)
  BIND_FIXED_TYPE(double, double, double)
  BIND_FIXED_TYPE(bool, byte, bool)
#undef BIND_FIXED_TYPE

  CassError bind_null(size_t index) {
    CASS_VALUE_CHECK_INDEX(index);
    values_[index] = Buffer();
    return CASS_OK;
  }

  CassError bind(size_t index, const char* value, size_t value_length) {
    CASS_VALUE_CHECK_INDEX(index);
    values_[index] = Buffer(value, value_length);
    return CASS_OK;
  }

  CassError bind(size_t index, const uint8_t* value, size_t value_length) {
    CASS_VALUE_CHECK_INDEX(index);
    values_[index] = Buffer(reinterpret_cast<const char*>(value), value_length);
    return CASS_OK;
  }

  CassError bind(size_t index, const uint8_t* value) {
    CASS_VALUE_CHECK_INDEX(index);
    values_[index] = Buffer(reinterpret_cast<const char*>(value), 16);
    return CASS_OK;
  }

  CassError bind(size_t index, const uint8_t* address, uint8_t address_len) {
    CASS_VALUE_CHECK_INDEX(index);
    values_[index] =
        Buffer(reinterpret_cast<const char*>(address), address_len);
    return CASS_OK;
  }

  CassError bind(size_t index, int32_t scale, const uint8_t* varint,
                 size_t varint_length) {
    CASS_VALUE_CHECK_INDEX(index);
    Buffer buffer(sizeof(int32_t) + varint_length);
    encode_decimal(buffer.data(), scale, varint, varint_length);
    values_[index] = buffer;
    return CASS_OK;
  }

  CassError bind(size_t index, const Collection* collection, bool is_map) {
    CASS_VALUE_CHECK_INDEX(index);
    if (is_map && collection->item_count() % 2 != 0) {
      return CASS_ERROR_LIB_INVALID_ITEM_COUNT;
    }
    values_[index] = collection->build(is_map);
    return CASS_OK;
  }

  CassError bind(size_t index, size_t output_size, uint8_t** output) {
    CASS_VALUE_CHECK_INDEX(index);
    values_[index] = Buffer(output_size);
    *output = reinterpret_cast<uint8_t*>(values_[index].data());
    return CASS_OK;
  }

  size_t encoded_size() const;

  char* encode(char* buffer) const;

private:
  typedef std::vector<Buffer> ValueVec;

  ValueVec values_;
  int16_t consistency_;
  int16_t serial_consistency_;
  int page_size_;
  std::string paging_state_;
};

} // namespace cass

#endif
