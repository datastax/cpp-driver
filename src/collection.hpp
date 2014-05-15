/*
  Copyright (c) 2014 DataStax

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

#ifndef __CASS_COLLECTION_HPP_INCLUDED__
#define __CASS_COLLECTION_HPP_INCLUDED__

#include "buffer_list.hpp"
#include "serialization.hpp"

namespace cass {

class Collection {
  public:
    Collection(size_t element_count)
      : buffer_list_(element_count) { }

#define APPEND_FIXED_TYPE(DeclType, EncodeType, Name)                            \
  inline void append_##Name(const DeclType& value) {                             \
    Buffer* buffer = buffer_list_.append(sizeof(uint16_t) + sizeof(DeclType));   \
    encode_##EncodeType(encode_short(buffer->data(), sizeof(DeclType)) , value); \
  }

  APPEND_FIXED_TYPE(int32_t, int, int32)
  APPEND_FIXED_TYPE(int64_t, int64,int64)
  APPEND_FIXED_TYPE(float, float, float)
  APPEND_FIXED_TYPE(double, double, double)
  APPEND_FIXED_TYPE(bool,  double, bool)
#undef BIND_FIXED_TYPE


  inline void append(const char* input, size_t input_length) {
    Buffer* buffer = buffer_list_.append(sizeof(uint16_t) + input_length);
    memcpy(encode_short(buffer->data(), input_length), input, input_length);
  }

  inline void append(const uint8_t* uuid) {
    Buffer* buffer = buffer_list_.append(sizeof(uint16_t) + 16);
    memcpy(encode_short(buffer->data(), 16), uuid, 16);
  }

  inline void append(const uint8_t* address, uint8_t address_len) {
    Buffer* buffer = buffer_list_.append(sizeof(uint16_t) + address_len);
    memcpy(encode_short(buffer->data(), address_len), address, address_len);
  }

  Buffer build(bool is_map) {
    Buffer combined(buffer_list_.size());
    size_t count = buffer_list_.count();
    if(is_map) {
      count /= 2;
    }
    buffer_list_.combine(encode_short(combined.data(), count));
    return combined;
  }

  private:
    BufferList buffer_list_;
};

} // namespace cass

#endif
