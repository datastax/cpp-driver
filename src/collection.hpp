/*
  Copyright (c) 2014 DataStax

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

#ifndef __CASS_COLLECTION_HPP_INCLUDED__
#define __CASS_COLLECTION_HPP_INCLUDED__

#include "buffer.hpp"
#include "serialization.hpp"
#include "ref_counted.h"

namespace cass {

class Collection : public RefCounted<Collection> {
public:
  explicit
  Collection(bool is_map, size_t item_count)
      : RefCounted<Collection>(1)
      , is_map_(is_map) {
    bufs_.reserve(item_count);
  }

#define APPEND_FIXED_TYPE(DeclType, EncodeType, Name) \
  void append_##Name(const DeclType& value) {         \
    Buffer buf(sizeof(DeclType));                     \
    encode_##EncodeType(buf.data(), value);           \
    bufs_.push_back(buf);                             \
  }

  APPEND_FIXED_TYPE(int32_t, int, int32)
  APPEND_FIXED_TYPE(int64_t, int64, int64)
  APPEND_FIXED_TYPE(float, float, float)
  APPEND_FIXED_TYPE(double, double, double)
  APPEND_FIXED_TYPE(bool, byte, bool)
#undef BIND_FIXED_TYPE

  void append(const char* value, size_t value_length) {
    Buffer buf(value_length);
    memcpy(buf.data(), value, value_length);
    bufs_.push_back(buf);
  }

  void append(const uint8_t* value, size_t value_length) {
    Buffer buf(value_length);
    memcpy(buf.data(), value, value_length);
    bufs_.push_back(buf);
  }

  void append(CassUuid value) {
    Buffer buf(sizeof(CassUuid));
    memcpy(buf.data(), value, sizeof(CassUuid));
    bufs_.push_back(buf);
  }

  void append(const uint8_t* address, uint8_t address_len) {
    Buffer buf(address_len);
    memcpy(buf.data(), address, address_len);
    bufs_.push_back(buf);
  }

  void append(int32_t scale, const uint8_t* varint, size_t varint_len) {
    Buffer buf(sizeof(int32_t) + varint_len);
    encode_decimal(buf.data(), scale, varint, varint_len);
    bufs_.push_back(buf);
  }

  bool is_map() const { return is_map_; }

  size_t item_count() const { return bufs_.size(); }

  int32_t encode(int version, Buffer* buffer) const;

private:
  BufferVec bufs_;
  bool is_map_;
};

} // namespace cass

#endif
