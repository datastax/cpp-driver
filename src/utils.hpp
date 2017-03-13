/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_COMMON_HPP_INCLUDED__
#define __CASS_COMMON_HPP_INCLUDED__

#include "cassandra.h"
#include "macros.hpp"

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <string.h>
#include <vector>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace cass {

class BufferPiece;
class Value;

typedef std::vector<std::string> ContactPointList;
typedef std::vector<std::string> DcList;

template<class From, class To>
#if _MSC_VER && !__INTEL_COMPILER
class IsConvertible : public std::is_convertible<From, To> {
#else
class IsConvertible {
  private:
    typedef char Yes;
    typedef struct { char not_used[2]; } No;

    struct Helper {
      static Yes test(To);
      static No test(...);
      static From& check();
    };

  public:
    static const bool value = sizeof(Helper::test(Helper::check())) == sizeof(Yes);
#endif
};

// copy_cast<> prevents incorrect code from being generated when two unrelated
// types reference the same memory location and strict aliasing is enabled.
// The type "char*" is an exception and is allowed to alias any other
// pointer type. This allows memcpy() to copy bytes from one type to the other
// without violating strict aliasing and usually optimizes away on a modern
// compiler (GCC, Clang, and MSVC).

template<typename From, typename To>
inline To copy_cast(const From& from)
{
  STATIC_ASSERT(sizeof(From) == sizeof(To));

  To to;
  memcpy(&to, &from, sizeof(from));
  return to;
}

inline size_t next_pow_2(size_t num) {
  size_t next = 2;
  size_t i = 0;
  while (next < num) {
    next = static_cast<size_t>(1) << i++;
  }
  return next;
}

std::string opcode_to_string(int opcode);

void explode(const std::string& str, std::vector<std::string>& vec, const char delimiter = ',');

std::string& trim(std::string& str);

bool is_valid_cql_id(const std::string& str);

std::string& to_cql_id(std::string& str);

std::string& escape_id(std::string& str);

inline size_t num_leading_zeros(int64_t value) {
  if (value == 0)
    return 64;

#if defined(_MSC_VER)
  unsigned long index;
#  if defined(_M_AMD64)
  _BitScanReverse64(&index, value);
#  else
  // On 32-bit this needs to be split into two operations
  char isNonzero = _BitScanReverse(&index, (unsigned long)(value >> 32));

  if (isNonzero)
    // The most significant 4 bytes has a bit set, and our index is relative to that.
    // Add 32 to account for the lower 4 bytes that make up our 64-bit number.
    index += 32;
  else {
    // Scan the last 32 bits by truncating the 64-bit value
    _BitScanReverse(&index, (unsigned long) value);
  }
#  endif
  // index is the (zero based) index, counting from lsb, of the most-significant 1 bit.
  // For example, a value of 12 (b1100) would return 3. The 4th bit is set, so there are
  // 60 leading zeros.
  return 64 - index - 1;
#else
  return __builtin_clzll(value);
#endif
}

inline size_t vint_size(int64_t value) {
  // | with 1 to ensure magnitude <= 63, so (63 - 1) / 7 <= 8
  size_t magnitude = num_leading_zeros(value | 1);
  return magnitude ? (9 - ((magnitude - 1) / 7)) : 9;
}

int32_t get_pid();

} // namespace cass

#endif
