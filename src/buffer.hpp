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

#include <stdint.h>

namespace cass {

class Buffer {
public:
  static const int32_t FIXED_BUFFER_SIZE = 32;

  Buffer()
      : size_(-1) {}

  Buffer(const char* data, int32_t size)
      : size_(size) {
    if (size > FIXED_BUFFER_SIZE) {
      data_.alloced = new char[size];
      memcpy(data_.alloced, data, size);
    } else if (size > 0) {
      memcpy(data_.fixed, data, size);
    }
  }

  Buffer(int32_t size)
      : size_(size) {
    if (size > FIXED_BUFFER_SIZE) {
      data_.alloced = new char[size];
    }
  }

  ~Buffer() {
    if (size_ > FIXED_BUFFER_SIZE && data_.alloced != nullptr) {
      delete data_.alloced;
    }
  }

  Buffer(const Buffer& Buffer) { copy(Buffer); }

  Buffer& operator=(const Buffer& Buffer) {
    copy(Buffer);
    return *this;
  }

  Buffer(Buffer&& Buffer) { move(std::move(Buffer)); }

  Buffer& operator=(Buffer&& Buffer) {
    move(std::move(Buffer));
    return *this;
  }

  void copy(const char* source, int32_t size) { memcpy(data(), source, size); }

  char* data() {
    return size_ > FIXED_BUFFER_SIZE ? data_.alloced : data_.fixed;
  }

  const char* data() const {
    return size_ > FIXED_BUFFER_SIZE ? data_.alloced : data_.fixed;
  }

  int32_t size() const { return size_; }

private:
  void move(Buffer&& Buffer) {
    size_ = Buffer.size_;
    if (size_ > 0) {
      if (size_ > FIXED_BUFFER_SIZE) {
        data_.alloced = Buffer.data_.alloced;
        Buffer.data_.alloced = nullptr;
      } else {
        memcpy(data_.fixed, Buffer.data_.fixed, size_);
      }
    }
  }

  void copy(const Buffer& Buffer) {
    size_ = Buffer.size_;
    if (size_ > 0) {
      if (size_ > FIXED_BUFFER_SIZE) {
        data_.alloced = new char[size_];
        memcpy(data_.alloced, Buffer.data_.alloced, size_);
      } else {
        memcpy(data_.fixed, Buffer.data_.fixed, size_);
      }
    }
  }

  union {
    char fixed[FIXED_BUFFER_SIZE];
    char* alloced;
  } data_;
  int32_t size_;
};

} // namespace cass

#endif
