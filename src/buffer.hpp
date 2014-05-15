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

namespace cass {

class Buffer {
  public:
    static const size_t FIXED_BUFFER_SIZE = 32;

    Buffer()
      : size_(0) { }

    Buffer(const char* data, size_t size)
      : size_(size) {
      if(size > FIXED_BUFFER_SIZE) {
        data_.alloced = new char[size];
        memcpy(data_.alloced, data, size);
      } else {
        memcpy(data_.fixed, data, size);
      }
    }

    Buffer(size_t size)
      : size_(size) {
      if(size > FIXED_BUFFER_SIZE) {
        data_.alloced = new char[size];
      }
    }

    ~Buffer() {
      if(size_ > FIXED_BUFFER_SIZE && data_.alloced != nullptr) {
        delete data_.alloced;
      }
    }

    Buffer(const Buffer& buffer) {
      copy(buffer);
    }

    Buffer& operator=(const Buffer& buffer) {
      copy(buffer);
      return *this;
    }

    Buffer(Buffer&& buffer) {
      move(std::move(buffer));
    }

    Buffer& operator=(Buffer&& buffer) {
      move(std::move(buffer));
      return *this;
    }

    void copy(const char* source, size_t size) {
      memcpy(data(), source, size);
    }

    char* data() {
      return size_ > FIXED_BUFFER_SIZE ? data_.alloced : data_.fixed;
    }

    const char* data() const {
      return size_ > FIXED_BUFFER_SIZE ? data_.alloced : data_.fixed;
    }

    size_t size() const {
      return size_;
    }

  private:
    void move(Buffer&& buffer) {
      size_ = buffer.size_;
      if(size_ > FIXED_BUFFER_SIZE) {
        data_.alloced = buffer.data_.alloced;
        buffer.data_.alloced = nullptr;
      } else {
        memcpy(data_.fixed, buffer.data_.fixed, size_);
      }
    }

    void copy(const Buffer& buffer) {
      size_ = buffer.size_;
      if(size_ > FIXED_BUFFER_SIZE) {
        data_.alloced = new char[size_];
        memcpy(data_.alloced, buffer.data_.alloced, size_);
      } else {
        memcpy(data_.fixed, buffer.data_.fixed, size_);
      }
    }

    union {
        char fixed[FIXED_BUFFER_SIZE];
        char* alloced;
    } data_;
    size_t size_;
};

} // namespace cass

#endif
