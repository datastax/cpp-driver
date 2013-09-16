/*
  Copyright (c) 2013 Matthew Stump

  This file is part of cassandra.

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

#ifndef CQL_UTIL_H_
#define CQL_UTIL_H_

#include <ostream>
#include <streambuf>
#include <vector>

#include "cql/cql.hpp"
#include "cql/internal/cql_system_dependent.hpp"

namespace cql {

struct cql_hex_char_struct_t {
    unsigned char c;
    cql_hex_char_struct_t(unsigned char _c) : c(_c) { }
};

inline std::ostream&
operator<<(std::ostream& o, const cql_hex_char_struct_t& hs) {
    return (o << std::hex << (int)hs.c);
}

inline
cql_hex_char_struct_t hex(unsigned char _c) {
    return cql_hex_char_struct_t(_c);
}

inline std::string
get_consistency_string(const cql::cql_short_t consistency) {
    switch (consistency) {
    case CQL_CONSISTENCY_ANY:
        return "CQL_CONSISTENCY_ANY";
        break;
    case CQL_CONSISTENCY_ONE:
        return "CQL_CONSISTENCY_ONE";
        break;
    case CQL_CONSISTENCY_TWO:
        return "CQL_CONSISTENCY_TWO";
        break;
    case CQL_CONSISTENCY_THREE:
        return "CQL_CONSISTENCY_THREE";
        break;
    case CQL_CONSISTENCY_QUORUM:
        return "CQL_CONSISTENCY_QUORUM";
        break;
    case CQL_CONSISTENCY_ALL:
        return "CQL_CONSISTENCY_ALL";
        break;
    case CQL_CONSISTENCY_LOCAL_QUORUM:
        return "CQL_CONSISTENCY_LOCAL_QUORUM";
        break;
    case CQL_CONSISTENCY_EACH_QUORUM:
        return "CQL_CONSISTENCY_EACH_QUORUM";
        break;
    default:
        return "UNKNOWN";
    }
}

// Safe version of strncpy (this method always null terminates
// dest buffer).
char* safe_strncpy(char* dest, const char* src, const size_t count);

} // namespace cql

#endif // CQL_UTIL_H_
