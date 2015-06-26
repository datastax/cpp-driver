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

#ifndef __CASS_TYPES_HPP_INCLUDED__
#define __CASS_TYPES_HPP_INCLUDED__

#include "cassandra.h"

namespace cass {

struct CassNull { };

struct CassBytes {
  CassBytes(const cass_byte_t* data, size_t size)
    : data(data), size(size) { }
  const cass_byte_t* data;
  size_t size;
};

struct CassString {
  CassString(const char* data, size_t length)
    : data(data), length(length) { }
  const char* data;
  size_t length;
};

struct CassDecimal {
  CassDecimal(const cass_byte_t* varint,
              size_t varint_size,
              int scale)
    : varint(varint)
    , varint_size(varint_size)
    , scale(scale) { }
  const cass_byte_t* varint;
  size_t varint_size;
  cass_int32_t scale;
};

struct CassCustom {
  CassCustom(cass_byte_t** output,
             size_t output_size)
    : output(output)
    , output_size(output_size) { }
  cass_byte_t** output;
  size_t output_size;
};

} // namespace cass

#endif
