/*
  Copyright (c) DataStax, Inc.

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

#ifndef DATASTAX_INTERNAL_TYPES_HPP
#define DATASTAX_INTERNAL_TYPES_HPP

#include "cassandra.h"
#include "string_ref.hpp"

namespace datastax { namespace internal { namespace core {

struct CassNull {};

struct CassUnset {};

struct CassBytes {
  CassBytes(const cass_byte_t* data, size_t size)
      : data(data)
      , size(size) {}
  const cass_byte_t* data;
  size_t size;
};

struct CassCustom {
  CassCustom(StringRef class_name, const cass_byte_t* data, size_t size)
      : class_name(class_name)
      , data(data)
      , size(size) {}
  StringRef class_name;
  const cass_byte_t* data;
  size_t size;
};

struct CassString {
  CassString(const char* data, size_t length)
      : data(data)
      , length(length) {}
  const char* data;
  size_t length;
};

struct CassDecimal {
  CassDecimal(const cass_byte_t* varint, size_t varint_size, int scale)
      : varint(varint)
      , varint_size(varint_size)
      , scale(scale) {}
  const cass_byte_t* varint;
  size_t varint_size;
  cass_int32_t scale;
};

struct CassDuration {
  CassDuration(cass_int32_t months, cass_int32_t days, cass_int64_t nanos)
      : months(months)
      , days(days)
      , nanos(nanos) {}
  cass_int32_t months;
  cass_int32_t days;
  cass_int64_t nanos;
};

}}} // namespace datastax::internal::core

#endif
