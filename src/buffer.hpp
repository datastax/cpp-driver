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

#include "ref_counted.h"

#include "third_party/boost/boost/cstdint.hpp"

#include <vector>

namespace cass {

class Collection;
class RefCollectionBuffer;

class RefBuffer : public RefCounted<RefBuffer> {
public:
  RefBuffer()
    : RefCounted<RefBuffer>(1)
    , data_(NULL) {}

  RefBuffer(size_t size)
    : RefCounted<RefBuffer>(1)
    , data_(new char[size]) {}

  virtual ~RefBuffer() {
    delete[] data_;
  }

  const char* data() const { return data_; }
  char* data() { return data_; }

protected:
  char* data_;

private:
  DISALLOW_COPY_AND_ASSIGN(RefBuffer);
};

class Buffer {
public:
  static const int32_t FIXED_BUFFER_SIZE = 32;

  Buffer()
      : size_(-1) {}

  Buffer(const char* data, int32_t size);

  explicit
  Buffer(int32_t size);

  explicit
  Buffer(const Collection* collection);

  ~Buffer() {
    if (size_ > FIXED_BUFFER_SIZE || size_ == -2) {
      data_.ref->release();
    }
  }

  Buffer(const Buffer& Buffer) { copy(Buffer); }

  Buffer& operator=(const Buffer& Buffer) {
    copy(Buffer);
    return *this;
  }

  void copy(const char* source, int32_t size) { memcpy(data(), source, size); }

  char* data() {
    assert(is_value());
    return size_ > FIXED_BUFFER_SIZE ? data_.ref->data() : data_.fixed;
  }

  const char* data() const {
    assert(is_value());
    return size_ > FIXED_BUFFER_SIZE ? data_.ref->data() : data_.fixed;
  }

  int32_t encode_collection(int version, Buffer* buffer) const;

  int32_t size() const { return size_; }

  bool is_value() const { return size_ >= 0; }
  bool is_null() const { return size_ == -1; }
  bool is_collection() const { return size_ == -2; }

private:
  void copy(const Buffer& buffer);

  union {
    char fixed[FIXED_BUFFER_SIZE];
    RefBuffer* ref;
  } data_;
  int32_t size_;
};

typedef std::vector<Buffer> BufferVec;

} // namespace cass

#endif
