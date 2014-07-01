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

class Ref : public RefCounted<Ref> {
public:
  Ref() : RefCounted<Ref>(1) {}
  virtual ~Ref() {}
private:
  DISALLOW_COPY_AND_ASSIGN(Ref);
};

class RefArray : public Ref {
public:
  RefArray(size_t size)
    : data_(new char[size]) {}

  virtual ~RefArray() {
    delete[] data_;
  }

  const char* data() const { return data_; }
  char* data() { return data_; }

private:
  char* data_;
};

class Buffer {
public:
  Buffer()
      : size_(IS_NULL) {}

  Buffer(const char* data, int32_t size);

  explicit
  Buffer(int32_t size);

  Buffer(const Collection* collection);

  Buffer(const Buffer& Buffer);

  ~Buffer();

  Buffer& operator=(const Buffer& Buffer) {
    copy(Buffer);
    return *this;
  }

  void copy(const char* source, int32_t size) { memcpy(data(), source, size); }

  char* data() {
    assert(is_value());
    return size_ > FIXED_BUFFER_SIZE ? static_cast<RefArray*>(data_.ref)->data() : data_.fixed;
  }

  const char* data() const {
    assert(is_value());
    return size_ > FIXED_BUFFER_SIZE ? static_cast<RefArray*>(data_.ref)->data() : data_.fixed;
  }

  int32_t size() const { return size_; }

  bool is_value() const { return size_ >= 0; }

  bool is_null() const { return size_ == IS_NULL; }

  bool is_collection() const { return size_ == IS_COLLECTION; }

  const Collection* collection() const;

private:
  enum {
    IS_NULL = -1,
    IS_COLLECTION = -2
  };

  static const int32_t FIXED_BUFFER_SIZE = 32;

private:
  void copy(const Buffer& buffer);

  union {
    char fixed[FIXED_BUFFER_SIZE];
    Ref* ref;
  } data_;
  int32_t size_;
};

typedef std::vector<Buffer> BufferVec;

} // namespace cass

#endif
