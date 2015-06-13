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

#ifndef __CASS_COMMON_HPP_INCLUDED__
#define __CASS_COMMON_HPP_INCLUDED__

#include "cassandra.h"
#include "macros.hpp"

#include <stddef.h>
#include <string>
#include <string.h>

namespace cass {

class BufferPiece;
class Value;

template<class From, class To>
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

std::string& trim(std::string& str);

bool is_valid_cql_id(const std::string& str);

std::string& to_cql_id(std::string& str);

} // namespace cass

#endif
