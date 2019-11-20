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

#ifndef DATASTAX_ENTERPRISE_INTERNAL_SERIALIZATION_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_SERIALIZATION_HPP

#include "dse.h"

#include "macros.hpp"
#include "serialization.hpp"
#include "vector.hpp"

#include <assert.h>
#include <stdint.h>
#include <string.h>

#define DSE_POINT_TYPE "org.apache.cassandra.db.marshal.PointType"
#define DSE_LINE_STRING_TYPE "org.apache.cassandra.db.marshal.LineStringType"
#define DSE_POLYGON_TYPE "org.apache.cassandra.db.marshal.PolygonType"
#define DSE_DATE_RANGE_TYPE "org.apache.cassandra.db.marshal.DateRangeType"

#define WKB_HEADER_SIZE (sizeof(cass_uint8_t) + sizeof(cass_uint32_t))        // Endian + Type
#define WKB_POLYGON_HEADER_SIZE (WKB_HEADER_SIZE + sizeof(cass_uint32_t))     // Header + Num rings
#define WKB_LINE_STRING_HEADER_SIZE (WKB_HEADER_SIZE + sizeof(cass_uint32_t)) // Header + Num points

namespace datastax { namespace internal { namespace enterprise {

enum DateRangeBoundType {
  DATE_RANGE_BOUND_TYPE_SINGLE_DATE = 0,
  DATE_RANGE_BOUND_TYPE_CLOSED_RANGE = 1,
  DATE_RANGE_BOUND_TYPE_OPEN_RANGE_HIGH = 2,
  DATE_RANGE_BOUND_TYPE_OPEN_RANGE_LOW = 3,
  DATE_RANGE_BOUND_TYPE_BOTH_OPEN_RANGE = 4,
  DATE_RANGE_BOUND_TYPE_SINGLE_DATE_OPEN = 5
};

enum WkbGeometryType {
  WKB_GEOMETRY_TYPE_POINT = 1,
  WKB_GEOMETRY_TYPE_LINESTRING = 2,
  WKB_GEOMETRY_TYPE_POLYGON = 3,
  WKB_GEOMETRY_TYPE_MULTIPOINT = 4,
  WKB_GEOMETRY_TYPE_MULTILINESTRING = 5,
  WKB_GEOMETRY_TYPE_MULTIPOLYGON = 6,
  WKB_GEOMETRY_TYPE_GEOMETRYCOLLECTION = 7
};

enum WkbByteOrder { WKB_BYTE_ORDER_BIG_ENDIAN = 0, WKB_BYTE_ORDER_LITTLE_ENDIAN = 1 };

#if defined(_M_IX86) || defined(_M_X64) || defined(_M_AMD64) || defined(__i386__) || \
    defined(__x86_64__) || defined(__amd64__)
inline WkbByteOrder native_byte_order() { return WKB_BYTE_ORDER_LITTLE_ENDIAN; }
#elif defined(__BYTE_ORDER__)
inline WkbByteOrder native_byte_order() {
  return __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__ ? WKB_BYTE_ORDER_BIG_ENDIAN
                                                : WKB_BYTE_ORDER_LITTLE_ENDIAN;
}
#else
inline WkbByteOrder native_byte_order() {
  const uint32_t native_byte_order = 0x01020304;
  return reinterpret_cast<const cass_uint8_t*>(&NATIVE_ORDER)[0] == 0x01
             ? WKB_BYTE_ORDER_BIG_ENDIAN
             : WKB_BYTE_ORDER_LITTLE_ENDIAN;
}
#endif

typedef Vector<cass_byte_t> Bytes;

#if defined(HAVE_BUILTIN_BSWAP32) && defined(HAVE_BUILTIN_BSWAP64)
inline cass_uint32_t swap_uint32(cass_uint32_t value) { return __builtin_bswap32(value); }

inline cass_uint64_t swap_uint64(cass_uint64_t value) { return __builtin_bswap64(value); }
#elif defined(_MSC_VER)
inline cass_uint32_t swap_uint32(cass_uint32_t value) {
  STATIC_ASSERT(sizeof(cass_uint32_t) == sizeof(unsigned long));
  return _byteswap_ulong(value);
}

inline cass_uint64_t swap_uint64(cass_uint64_t value) { return _byteswap_uint64(value); }
#else
inline cass_uint32_t swap_uint32(cass_uint32_t value) {
  value = ((value << 8) & 0xFF00FF00) | ((value >> 8) & 0xFF00FF);
  return (value << 16) | (value >> 16);
}

inline cass_uint64_t swap_uint64(cass_uint64_t value) {
  value = ((value << 8) & 0xFF00FF00FF00FF00ULL) | ((value >> 8) & 0x00FF00FF00FF00FFULL);
  value = ((value << 16) & 0xFFFF0000FFFF0000ULL) | ((value >> 16) & 0x0000FFFF0000FFFFULL);
  return (value << 32) | (value >> 32);
}
#endif

template <class T>
inline void encode(T value, size_t index, Bytes& bytes) {
  assert(bytes.size() >= index + sizeof(T));
  memcpy(&bytes[index], &value, sizeof(T));
}

template <class T>
inline void encode_append(T value, Bytes& bytes) {
  cass_uint8_t* data = reinterpret_cast<cass_uint8_t*>(&value);
  for (size_t i = 0; i < sizeof(T); ++i) {
    bytes.push_back(data[i]);
  }
}

inline void encode_header_append(WkbGeometryType type, Bytes& bytes) {
  bytes.push_back(native_byte_order());
  encode_append(static_cast<cass_uint32_t>(type), bytes);
}

inline cass_double_t decode_double(const cass_byte_t* bytes, WkbByteOrder byte_order) {
  STATIC_ASSERT(sizeof(cass_double_t) == sizeof(cass_uint64_t));
  cass_double_t value;
  if (byte_order != native_byte_order()) {
    cass_uint64_t temp;
    memcpy(&temp, bytes, sizeof(cass_uint64_t));
    swap_uint64(temp);
    memcpy(&value, &temp, sizeof(cass_uint64_t));
  } else {
    memcpy(&value, bytes, sizeof(cass_uint64_t));
  }
  return value;
}

inline cass_uint32_t decode_uint32(const cass_byte_t* bytes, WkbByteOrder byte_order) {
  cass_uint32_t value;
  memcpy(&value, bytes, sizeof(cass_uint32_t));
  if (byte_order != native_byte_order()) {
    swap_uint32(value);
  }
  return value;
}

inline WkbGeometryType decode_header(const cass_byte_t* bytes, WkbByteOrder* byte_order) {
  *byte_order = static_cast<WkbByteOrder>(bytes[0]);
  return static_cast<WkbGeometryType>(decode_uint32(bytes + 1, *byte_order));
}

}}} // namespace datastax::internal::enterprise

#endif
