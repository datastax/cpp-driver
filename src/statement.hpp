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

#include <list>
#include <utility>

#include "message_body.hpp"
#include "buffer.hpp"
#include "collection.hpp"

#define CASS_VALUE_CHECK_INDEX(i)              \
  if (index >= size()) {                       \
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS; \
  }

namespace cass {

struct Statement : public Request {
  typedef std::vector<Buffer> ValueVec;

  ValueVec values;

  Statement(uint8_t opcode)
      : Request(opcode) {}

  Statement(uint8_t opcode, size_t value_count)
      : Request(opcode)
      , values(value_count) {}

  virtual ~Statement() {}

  virtual uint8_t kind() const = 0;

  virtual void statement(const std::string& statement) = 0;

  virtual void statement(const char* statement, size_t size) = 0;

  virtual const char* statement() const = 0;

  virtual size_t statement_size() const = 0;

  virtual void consistency(int16_t consistency) = 0;

  virtual int16_t consistency() const = 0;

  virtual int16_t serial_consistency() const = 0;

  virtual void serial_consistency(int16_t consistency) = 0;

  void resize(size_t size) { values.resize(size); }

  size_t size() const { return values.size(); }

#define BIND_FIXED_TYPE(DeclType, EncodeType, Name)                   \
  CassError bind_##Name(size_t index, const DeclType& value) {        \
    CASS_VALUE_CHECK_INDEX(index);                                    \
    Buffer buffer(sizeof(DeclType));                                  \
    encode_##EncodeType(buffer.data(), value);                        \
    values[index] = buffer;                                           \
    return CASS_OK;                                                   \
  }

  BIND_FIXED_TYPE(int32_t, int, int32)
  BIND_FIXED_TYPE(int64_t, int64, int64)
  BIND_FIXED_TYPE(float, float, float)
  BIND_FIXED_TYPE(double, double, double)
  BIND_FIXED_TYPE(bool, byte, bool)
#undef BIND_FIXED_TYPE

  CassError bind_null(size_t index) {
    CASS_VALUE_CHECK_INDEX(index);
    values[index] = Buffer();
    return CASS_OK;
  }

  CassError bind(size_t index, const char* value, size_t value_length) {
    CASS_VALUE_CHECK_INDEX(index);
    values[index] = Buffer(value, value_length);
    return CASS_OK;
  }

  CassError bind(size_t index, const uint8_t* value,
                        size_t value_length) {
    CASS_VALUE_CHECK_INDEX(index);
    values[index] = Buffer(reinterpret_cast<const char*>(value), value_length);
    return CASS_OK;
  }

  CassError bind(size_t index, const uint8_t* value) {
    CASS_VALUE_CHECK_INDEX(index);
    values[index] = Buffer(reinterpret_cast<const char*>(value), 16);
    return CASS_OK;
  }

  CassError bind(size_t index, const uint8_t* address,
                        uint8_t address_len) {
    CASS_VALUE_CHECK_INDEX(index);
    values[index] = Buffer(reinterpret_cast<const char*>(address), address_len);
    return CASS_OK;
  }

  CassError bind(size_t index, int32_t scale, const uint8_t* varint,
                        size_t varint_length) {
    CASS_VALUE_CHECK_INDEX(index);
    Buffer buffer(sizeof(int32_t) + varint_length);
    encode_decimal(buffer.data(), scale, varint, varint_length);
    values[index] = buffer;
    return CASS_OK;
  }

  CassError bind(size_t index, const Collection* collection,
                        bool is_map) {
    CASS_VALUE_CHECK_INDEX(index);
    if (is_map && collection->item_count() % 2 != 0) {
      return CASS_ERROR_LIB_INVALID_ITEM_COUNT;
    }
    values[index] = collection->build(is_map);
    return CASS_OK;
  }

  CassError bind(size_t index, size_t output_size, uint8_t** output) {
    CASS_VALUE_CHECK_INDEX(index);
    values[index] = Buffer(output_size);
    *output = reinterpret_cast<uint8_t*>(values[index].data());
    return CASS_OK;
  }

  size_t values_size() const {
    size_t total_size = sizeof(uint16_t);
    for(ValueVec::const_iterator it = values.begin(),
        end = values.end(); it != end; ++it) {
      total_size += sizeof(int32_t);
      int32_t value_size = it->size();
      if (value_size > 0) {
        total_size += value_size;
      }
    }
    return total_size;
  }

  char* encode_values(char* buffer) const {
    buffer = encode_short(buffer, values.size());
    for(ValueVec::const_iterator it = values.begin(),
        end = values.end(); it != end; ++it) {
      buffer = encode_bytes(buffer, it->data(), it->size());
    }
    return buffer;
  }
};

} // namespace cass

#endif
