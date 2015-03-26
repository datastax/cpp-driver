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

#ifndef __CASS_BUFFER_PIECE_HPP_INCLUDED__
#define __CASS_BUFFER_PIECE_HPP_INCLUDED__

#include "string_ref.hpp"

#include <uv.h>

#include <string>

namespace cass {

class BufferPiece {
public:
  BufferPiece()
      : data_(NULL)
      , size_(-1) {}

  BufferPiece(char* data, size_t size)
      : data_(data)
      , size_(size) {}

  char* data() const { return data_; }
  int32_t size() const { return size_; }

  StringRef to_string_ref() const {
    if (size_ < 0) {
      StringRef();
    }
    return StringRef(data_, size_);
  }

  std::string to_string() const {
    return to_string_ref().to_string();
  }

private:
  char* data_;
  int32_t size_;
};

} // namespace cass

#endif
