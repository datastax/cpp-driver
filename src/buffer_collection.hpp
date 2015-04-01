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

#ifndef __CASS_BUFFER_COLLECTION_HPP_INCLUDED__
#define __CASS_BUFFER_COLLECTION_HPP_INCLUDED__

#include "buffer.hpp"
#include "ref_counted.hpp"

namespace cass {

class BufferCollection : public RefCounted<BufferCollection> {
public:
  explicit
  BufferCollection(bool is_map, size_t item_count)
      : is_map_(is_map) {
    bufs_.reserve(item_count);
  }

#define APPEND_FIXED_TYPE(DeclType, EncodeType)     \
  void append_##EncodeType(const DeclType& value) { \
    Buffer buf(sizeof(DeclType));                   \
    buf.encode_##EncodeType(0, value);              \
    bufs_.push_back(buf);                           \
  }

  APPEND_FIXED_TYPE(int32_t, int32)
  APPEND_FIXED_TYPE(int64_t, int64)
  APPEND_FIXED_TYPE(float, float)
  APPEND_FIXED_TYPE(double, double)
  APPEND_FIXED_TYPE(uint8_t, byte)
#undef BIND_FIXED_TYPE

  void append(const char* value, size_t value_length) {
    Buffer buf(value_length);
    buf.copy(0, value, value_length);
    bufs_.push_back(buf);
  }

  void append(const uint8_t* value, size_t value_length) {
    append(reinterpret_cast<const char*>(value), value_length);
  }

  void append(CassUuid value) {
    Buffer buf(sizeof(CassUuid));
    buf.encode_uuid(0, value);
    bufs_.push_back(buf);
  }

  void append(const uint8_t* varint, size_t varint_len, int32_t scale) {
    Buffer buf(sizeof(int32_t) + varint_len);
    size_t pos = buf.encode_int32(0, scale);
    buf.copy(pos, varint, varint_len);
    bufs_.push_back(buf);
  }

  bool is_map() const { return is_map_; }

  size_t item_count() const { return bufs_.size(); }

  int encode(int version, BufferVec* bufs) const;
  int calculate_size(int version) const;
  void encode(int version, char* buf) const;

private:

  BufferVec bufs_;
  bool is_map_;
};

} // namespace cass

#endif
