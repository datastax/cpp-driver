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

#ifndef __CASS_BUFFER_LIST_HPP_INCLUDED__
#define __CASS_BUFFER_LIST_HPP_INCLUDED__

#include <vector>

#include "buffer.hpp"

namespace cass {

class BufferList {
public:
  BufferList(size_t count)
      : size_(0) {
    buffers_.reserve(count);
  }

  Buffer* append(int32_t size) {
    buffers_.push_back(Buffer(size));
    size_ += size;
    return &buffers_.back();
  }

  void append(const char* data, int32_t size) {
    buffers_.push_back(Buffer(data, size));
    size_ += size;
  }

  void combine(char* output) const {
    int32_t offset = 0;
    for (const auto& buffer : buffers_) {
      memcpy(output + offset, buffer.data(), buffer.size());
      offset += buffer.size();
    }
  }

  size_t count() const { return buffers_.size(); }
  int32_t size() const { return size_; }

private:
  std::vector<Buffer> buffers_;
  int32_t size_;
};
}

#endif
