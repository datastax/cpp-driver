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

#ifndef DATASTAX_INTERNAL_BUFFER_HPP
#define DATASTAX_INTERNAL_BUFFER_HPP

#include "ref_counted.hpp"
#include "serialization.hpp"
#include "vector.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class Buffer {
public:
  Buffer()
      : size_(0) {}

  Buffer(const char* data, size_t size)
      : size_(size) {
    if (size > FIXED_BUFFER_SIZE) {
      RefBuffer* buffer = RefBuffer::create(size);
      buffer->inc_ref();
      memcpy(buffer->data(), data, size);
      data_.buffer = buffer;
    } else if (size > 0) {
      memcpy(data_.fixed, data, size);
    }
  }

  explicit Buffer(size_t size)
      : size_(size) {
    if (size > FIXED_BUFFER_SIZE) {
      RefBuffer* buffer = RefBuffer::create(size);
      buffer->inc_ref();
      data_.buffer = buffer;
    }
  }

  Buffer(const Buffer& buf)
      : size_(0) {
    copy(buf);
  }

  Buffer& operator=(const Buffer& buf) {
    copy(buf);
    return *this;
  }

  ~Buffer() {
    if (size_ > FIXED_BUFFER_SIZE) {
      data_.buffer->dec_ref();
    }
  }

  size_t encode_byte(size_t offset, uint8_t value) {
    assert(offset + sizeof(uint8_t) <= static_cast<size_t>(size_));
    internal::encode_byte(data() + offset, value);
    return offset + sizeof(uint8_t);
  }

  size_t encode_int8(size_t offset, int8_t value) {
    assert(offset + sizeof(int8_t) <= static_cast<size_t>(size_));
    internal::encode_int8(data() + offset, value);
    return offset + sizeof(int8_t);
  }

  size_t encode_int16(size_t offset, int16_t value) {
    assert(offset + sizeof(int16_t) <= static_cast<size_t>(size_));
    internal::encode_int16(data() + offset, value);
    return offset + sizeof(int16_t);
  }

  size_t encode_uint16(size_t offset, uint16_t value) {
    assert(offset + sizeof(uint16_t) <= static_cast<size_t>(size_));
    internal::encode_uint16(data() + offset, value);
    return offset + sizeof(uint16_t);
  }

  size_t encode_int32(size_t offset, int32_t value) {
    assert(offset + sizeof(int32_t) <= static_cast<size_t>(size_));
    internal::encode_int32(data() + offset, value);
    return offset + sizeof(int32_t);
  }

  size_t encode_uint32(size_t offset, uint32_t value) {
    assert(offset + sizeof(uint32_t) <= static_cast<size_t>(size_));
    internal::encode_uint32(data() + offset, value);
    return offset + sizeof(uint32_t);
  }

  size_t encode_int64(size_t offset, int64_t value) {
    assert(offset + sizeof(int64_t) <= static_cast<size_t>(size_));
    internal::encode_int64(data() + offset, value);
    return offset + sizeof(int64_t);
  }

  size_t encode_float(size_t offset, float value) {
    assert(offset + sizeof(float) <= static_cast<size_t>(size_));
    internal::encode_float(data() + offset, value);
    return offset + sizeof(float);
  }

  size_t encode_double(size_t offset, double value) {
    assert(offset + sizeof(double) <= static_cast<size_t>(size_));
    internal::encode_double(data() + offset, value);
    return offset + sizeof(double);
  }

  size_t encode_long_string(size_t offset, const char* value, int32_t size) {
    size_t pos = encode_int32(offset, size);
    return copy(pos, value, size);
  }

  size_t encode_bytes(size_t offset, const char* value, int32_t size) {
    size_t pos = encode_int32(offset, size);
    if (size > 0) {
      return copy(pos, value, size);
    }
    return pos;
  }

  size_t encode_string(size_t offset, const char* value, uint16_t size) {
    size_t pos = encode_uint16(offset, size);
    return copy(pos, value, size);
  }

  size_t encode_string_list(size_t offset, const Vector<String>& value) {
    size_t pos = encode_uint16(offset, static_cast<uint16_t>(value.size()));
    for (Vector<String>::const_iterator it = value.begin(), end = value.end(); it != end; ++it) {
      pos = encode_string(pos, it->data(), static_cast<uint16_t>(it->size()));
    }
    return pos;
  }

  size_t encode_string_map(size_t offset, const Map<String, String>& value) {
    size_t pos = encode_uint16(offset, static_cast<uint16_t>(value.size()));
    for (Map<String, String>::const_iterator it = value.begin(); it != value.end(); ++it) {
      pos = encode_string(pos, it->first.c_str(), static_cast<uint16_t>(it->first.size()));
      pos = encode_string(pos, it->second.c_str(), static_cast<uint16_t>(it->second.size()));
    }
    return pos;
  }

  size_t encode_uuid(size_t offset, CassUuid value) {
    assert(offset + sizeof(CassUuid) <= static_cast<size_t>(size_));
    internal::encode_uuid(data() + offset, value);
    return offset + sizeof(CassUuid);
  }

  size_t copy(size_t offset, const char* value, size_t size) {
    assert(offset + size <= static_cast<size_t>(size_));
    memcpy(data() + offset, value, size);
    return offset + size;
  }

  size_t copy(size_t offset, const uint8_t* source, size_t size) {
    return copy(offset, reinterpret_cast<const char*>(source), size);
  }

  char* data() {
    return size_ > FIXED_BUFFER_SIZE ? static_cast<RefBuffer*>(data_.buffer)->data() : data_.fixed;
  }

  const char* data() const {
    return size_ > FIXED_BUFFER_SIZE ? static_cast<RefBuffer*>(data_.buffer)->data() : data_.fixed;
  }

  size_t size() const { return size_; }

private:
  // Enough space to avoid extra allocations for most of the basic types
  static const size_t FIXED_BUFFER_SIZE = 16;

private:
  void copy(const Buffer& buf) {
    RefBuffer* temp = data_.buffer;

    if (buf.size_ > FIXED_BUFFER_SIZE) {
      buf.data_.buffer->inc_ref();
      data_.buffer = buf.data_.buffer;
    } else if (buf.size_ > 0) {
      memcpy(data_.fixed, buf.data_.fixed, buf.size_);
    }

    if (size_ > FIXED_BUFFER_SIZE) {
      temp->dec_ref();
    }

    size_ = buf.size_;
  }

  union Data {
    char fixed[FIXED_BUFFER_SIZE];
    RefBuffer* buffer;

    Data()
        : buffer(NULL) {}
  } data_;

  size_t size_;
};

typedef Vector<Buffer> BufferVec;

}}} // namespace datastax::internal::core

#endif
