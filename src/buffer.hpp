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

class Buffer {
public:
  static const int32_t FIXED_BUFFER_SIZE = 32;

  Buffer()
      : size_(-1) {}

  Buffer(const char* data, int32_t size)
      : size_(size) {
    if (size > FIXED_BUFFER_SIZE) {
      data_.alloced = new RefBuffer(size);
      memcpy(data_.alloced->data(), data, size);
    } else if (size > 0) {
      memcpy(data_.fixed, data, size);
    }
  }

  Buffer(int32_t size)
      : size_(size) {
    if (size > FIXED_BUFFER_SIZE) {
      data_.alloced = new RefBuffer(size);
    }
  }

  ~Buffer() {
    if (size_ > FIXED_BUFFER_SIZE) {
      data_.alloced->release();
    }
  }

  Buffer(const Buffer& Buffer) { copy(Buffer); }

  Buffer& operator=(const Buffer& Buffer) {
    copy(Buffer);
    return *this;
  }

  void copy(const char* source, int32_t size) { memcpy(data(), source, size); }

  char* data() {
    return size_ > FIXED_BUFFER_SIZE ? data_.alloced->data() : data_.fixed;
  }

  const char* data() const {
    return size_ > FIXED_BUFFER_SIZE ? data_.alloced->data() : data_.fixed;
  }

  int32_t size() const { return size_; }

private:
  void copy(const Buffer& buffer) {
    size_ = buffer.size_;
    if (size_ > 0) {
      if (size_ > FIXED_BUFFER_SIZE) {
        buffer.data_.alloced->retain();
        RefBuffer* temp = data_.alloced;
        data_.alloced = buffer.data_.alloced;
        temp->release();
      } else {
        memcpy(data_.fixed, buffer.data_.fixed, size_);
      }
    }
  }

  class RefBuffer : public RefCounted<RefBuffer> {
  public:
    RefBuffer(size_t size)
      : RefCounted<RefBuffer>(1)
      , data_(new char[size]) {}

    ~RefBuffer() {
      delete[] data_;
    }

    const char* data() const { return data_; }
    char* data() { return data_; }

  private:
    char* data_;
  };

  union {
    char fixed[FIXED_BUFFER_SIZE];
    RefBuffer* alloced;
  } data_;
  int32_t size_;
};

typedef std::vector<Buffer> BufferVec;

} // namespace cass

#endif
