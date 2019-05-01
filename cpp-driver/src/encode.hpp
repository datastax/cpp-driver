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

#ifndef DATASTAX_INTERNAL_ENCODING_HPP
#define DATASTAX_INTERNAL_ENCODING_HPP

#include "buffer.hpp"
#include "types.hpp"

namespace datastax { namespace internal { namespace core {

inline Buffer encode_with_length(CassNull) {
  Buffer buf(sizeof(int32_t));
  buf.encode_int32(0, -1); // [bytes] "null"
  return buf;
}

inline Buffer encode_with_length(CassUnset) {
  Buffer buf(sizeof(int32_t));
  buf.encode_int32(0, -2); // [bytes] "unset"
  return buf;
}

inline Buffer encode_with_length(cass_int8_t value) {
  Buffer buf(sizeof(int32_t) + sizeof(int8_t));
  size_t pos = buf.encode_int32(0, sizeof(int8_t));
  buf.encode_int8(pos, value);
  return buf;
}

inline Buffer encode_with_length(cass_int16_t value) {
  Buffer buf(sizeof(int32_t) + sizeof(int16_t));
  size_t pos = buf.encode_int32(0, sizeof(int16_t));
  buf.encode_int16(pos, value);
  return buf;
}

inline Buffer encode_with_length(cass_int32_t value) {
  Buffer buf(sizeof(int32_t) + sizeof(int32_t));
  size_t pos = buf.encode_int32(0, sizeof(int32_t));
  buf.encode_int32(pos, value);
  return buf;
}

inline Buffer encode_with_length(cass_uint32_t value) {
  Buffer buf(sizeof(int32_t) + sizeof(uint32_t));
  size_t pos = buf.encode_int32(0, sizeof(uint32_t));
  buf.encode_uint32(pos, value);
  return buf;
}

inline Buffer encode_with_length(cass_int64_t value) {
  Buffer buf(sizeof(int32_t) + sizeof(int64_t));
  size_t pos = buf.encode_int32(0, sizeof(int64_t));
  buf.encode_int64(pos, value);
  return buf;
}

inline Buffer encode_with_length(cass_float_t value) {
  Buffer buf(sizeof(int32_t) + sizeof(float));
  size_t pos = buf.encode_int32(0, sizeof(float));
  buf.encode_float(pos, value);
  return buf;
}

inline Buffer encode_with_length(cass_double_t value) {
  Buffer buf(sizeof(int32_t) + sizeof(double));
  size_t pos = buf.encode_int32(0, sizeof(double));
  buf.encode_double(pos, value);
  return buf;
}

inline Buffer encode_with_length(cass_bool_t value) {
  Buffer buf(sizeof(int32_t) + 1);
  size_t pos = buf.encode_int32(0, 1);
  buf.encode_byte(pos, static_cast<int8_t>(value));
  return buf;
}

inline Buffer encode_with_length(CassString value) {
  Buffer buf(sizeof(int32_t) + value.length);
  size_t pos = buf.encode_int32(0, value.length);
  buf.copy(pos, value.data, value.length);
  return buf;
}

inline Buffer encode_with_length(CassBytes value) {
  Buffer buf(sizeof(int32_t) + value.size);
  size_t pos = buf.encode_int32(0, value.size);
  buf.copy(pos, reinterpret_cast<const char*>(value.data), value.size);
  return buf;
}

inline Buffer encode_with_length(CassCustom value) {
  Buffer buf(sizeof(int32_t) + value.size);
  size_t pos = buf.encode_int32(0, value.size);
  buf.copy(pos, reinterpret_cast<const char*>(value.data), value.size);
  return buf;
}

inline Buffer encode_with_length(CassUuid value) {
  Buffer buf(sizeof(int32_t) + sizeof(CassUuid));
  size_t pos = buf.encode_int32(0, sizeof(CassUuid));
  buf.encode_uuid(pos, value);
  return buf;
}

inline Buffer encode_with_length(CassInet value) {
  Buffer buf(sizeof(int32_t) + value.address_length);
  size_t pos = buf.encode_int32(0, value.address_length);
  buf.copy(pos, value.address, value.address_length);
  return buf;
}

inline Buffer encode_with_length(CassDecimal value) {
  Buffer buf(sizeof(int32_t) + sizeof(int32_t) + value.varint_size);
  size_t pos = buf.encode_int32(0, sizeof(int32_t) + value.varint_size);
  pos = buf.encode_int32(pos, value.scale);
  buf.copy(pos, value.varint, value.varint_size);
  return buf;
}

inline Buffer encode(cass_int8_t value) {
  Buffer buf(sizeof(cass_int8_t));
  buf.encode_int8(0, value);
  return buf;
}

inline Buffer encode(cass_int16_t value) {
  Buffer buf(sizeof(cass_int16_t));
  buf.encode_int16(0, value);
  return buf;
}

inline Buffer encode(cass_int32_t value) {
  Buffer buf(sizeof(int32_t));
  buf.encode_int32(0, value);
  return buf;
}

inline Buffer encode(cass_uint32_t value) {
  Buffer buf(sizeof(uint32_t));
  buf.encode_uint32(0, value);
  return buf;
}

inline Buffer encode(cass_int64_t value) {
  Buffer buf(sizeof(int64_t));
  buf.encode_int64(0, value);
  return buf;
}

inline Buffer encode(cass_float_t value) {
  Buffer buf(sizeof(float));
  buf.encode_float(0, value);
  return buf;
}

inline Buffer encode(cass_double_t value) {
  Buffer buf(sizeof(double));
  buf.encode_double(0, value);
  return buf;
}

inline Buffer encode(cass_bool_t value) {
  Buffer buf(1);
  buf.encode_byte(0, static_cast<int8_t>(value));
  return buf;
}

inline Buffer encode(CassString value) {
  Buffer buf(value.length);
  buf.copy(0, value.data, value.length);
  return buf;
}

inline Buffer encode(CassBytes value) {
  Buffer buf(value.size);
  buf.copy(0, reinterpret_cast<const char*>(value.data), value.size);
  return buf;
}

inline Buffer encode(CassCustom value) {
  Buffer buf(value.size);
  buf.copy(0, reinterpret_cast<const char*>(value.data), value.size);
  return buf;
}

inline Buffer encode(CassUuid value) {
  Buffer buf(sizeof(CassUuid));
  buf.encode_uuid(0, value);
  return buf;
}

inline Buffer encode(CassInet value) {
  Buffer buf(value.address_length);
  buf.copy(0, value.address, value.address_length);
  return buf;
}

inline Buffer encode(CassDecimal value) {
  Buffer buf(sizeof(int32_t) + value.varint_size);
  size_t pos = buf.encode_int32(0, value.scale);
  buf.copy(pos, value.varint, value.varint_size);
  return buf;
}

Buffer encode(CassDuration value);

Buffer encode_with_length(CassDuration value);

}}} // namespace datastax::internal::core

#endif
