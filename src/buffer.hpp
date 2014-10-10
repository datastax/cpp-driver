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

#ifndef __CASS_BUFFER_HPP_INCLUDED__
#define __CASS_BUFFER_HPP_INCLUDED__

#include "ref_counted.hpp"
#include "serialization.hpp"

#include "third_party/boost/boost/cstdint.hpp"

#include <vector>

namespace cass {

class BufferArray : public RefCounted<BufferArray> {
public:
  BufferArray(size_t size)
    : data_(new char[size]) {}

  virtual ~BufferArray() {
    delete[] data_;
  }

  const char* data() const { return data_; }
  char* data() { return data_; }

private:
  char* data_;
};

class BufferCollection;

class Buffer {
public:
  Buffer()
      : size_(IS_EMPTY) {}

  Buffer(const char* data, size_t size)
    : size_(size) {
    if (size > static_cast<size_t>(FIXED_BUFFER_SIZE)) {
      BufferArray* array = new BufferArray(size);
      array->inc_ref();
      memcpy(array->data(), data, size);
      data_.ref.array = array;
    } else {
      memcpy(data_.fixed, data, size);
    }
  }

  explicit
  Buffer(size_t size)
    : size_(size) {
    if (size > static_cast<size_t>(FIXED_BUFFER_SIZE)) {
      BufferArray* array = new BufferArray(size);
      array->inc_ref();
      data_.ref.array = array;
    }
  }

  Buffer(const BufferCollection* collection);

  Buffer(const Buffer& buf)
    : size_(IS_EMPTY) {
    copy(buf);
  }

  Buffer& operator=(const Buffer& buf) {
    copy(buf);
    return *this;
  }

  ~Buffer();

  size_t encode_byte(size_t offset, uint8_t value) {
    assert(is_buffer() && offset + sizeof(uint8_t) <= static_cast<size_t>(size_));
    cass::encode_byte(buffer() + offset, value);
    return offset + sizeof(uint8_t);
  }

  size_t encode_uint16(size_t offset, uint16_t value) {
    assert(is_buffer() && offset + sizeof(int16_t) <= static_cast<size_t>(size_));
    cass::encode_uint16(buffer() + offset, value);
    return offset + sizeof(int16_t);
  }

  size_t encode_int32(size_t offset, int32_t value) {
    assert(is_buffer() && offset + sizeof(int32_t) <= static_cast<size_t>(size_));
    cass::encode_int32(buffer() + offset, value);
    return offset + sizeof(int32_t);
  }

  size_t encode_int64(size_t offset, int64_t value) {
    assert(is_buffer() && offset + sizeof(int64_t) <= static_cast<size_t>(size_));
    cass::encode_int64(buffer() + offset, value);
    return offset + sizeof(int64_t);
  }

  size_t encode_float(size_t offset, float value) {
    assert(is_buffer() && offset + sizeof(float) <= static_cast<size_t>(size_));
    cass::encode_float(buffer() + offset, value);
    return offset + sizeof(float);
  }

  size_t encode_double(size_t offset, double value) {
    assert(is_buffer() && offset + sizeof(double) <= static_cast<size_t>(size_));
    cass::encode_double(buffer() + offset, value);
    return offset + sizeof(double);
  }

  size_t encode_bool(size_t offset, bool value) {
    return encode_byte(offset, value);
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

  size_t encode_string_list(size_t offset, const std::vector<std::string>& value) {
    size_t pos = encode_uint16(offset, value.size());
    for (std::vector<std::string>::const_iterator it = value.begin(),
         end = value.end(); it != end; ++it) {
      pos = encode_string(pos, it->data(), it->size());
    }
    return pos;
  }

  size_t encode_string_map(size_t offset, const std::map<std::string, std::string>& value) {
    size_t pos = encode_uint16(offset, value.size());
    for (std::map<std::string, std::string>::const_iterator it = value.begin();
         it != value.end(); ++it) {
      pos = encode_string(pos, it->first.c_str(), it->first.size());
      pos = encode_string(pos, it->second.c_str(), it->second.size());
    }
    return pos;
  }

  size_t copy(size_t offset, const char* value, size_t size) {
    assert(is_buffer() && offset + size <= static_cast<size_t>(size_));
    memcpy(buffer() + offset, value, size);
    return offset + size;
  }

  size_t copy(size_t offset, const uint8_t* source, size_t size) {
    return copy(offset, reinterpret_cast<const char*>(source), size);
  }

  const char* data() const {
    assert(is_buffer());
    return size_ > FIXED_BUFFER_SIZE ? static_cast<BufferArray*>(data_.ref.array)->data() : data_.fixed;
  }

  int size() const { return size_; }

  bool is_buffer() const { return size_ >= 0; }

  bool is_empty() const { return size_ == IS_EMPTY; }

  bool is_collection() const { return size_ == IS_COLLECTION; }

  const BufferCollection* collection() const;

private:
  enum {
    IS_EMPTY = -1,
    IS_COLLECTION = -2
  };

  char* buffer() {
    assert(is_buffer());
    return size_ > FIXED_BUFFER_SIZE
        ? static_cast<BufferArray*>(data_.ref.array)->data()
        : data_.fixed;
  }

  // Enough space to avoid extra allocations for most of the basic types
  static const int FIXED_BUFFER_SIZE = 16;

private:
  void copy(const Buffer& buffer);

  union BufferRef {
    BufferArray* array;
    const BufferCollection* collection;
  };

  union {
    char fixed[FIXED_BUFFER_SIZE];
    BufferRef ref;
  } data_;
  int size_;
};

typedef std::vector<Buffer> BufferVec;

} // namespace cass

#endif
