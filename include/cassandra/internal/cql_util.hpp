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
#include "cassandra/cql.hpp"

namespace cql {

    struct HexCharStruct
    {
        unsigned char c;
        HexCharStruct(unsigned char _c) : c(_c) { }
    };

    inline std::ostream&
    operator<<(std::ostream& o, const HexCharStruct& hs)
    {
        return (o << std::hex << (int)hs.c);
    }

    inline
    HexCharStruct hex(unsigned char _c)
    {
        return HexCharStruct(_c);
    }

    inline std::string
    get_consistency_string(const cql::cql_short_t consistency)
    {
        switch (consistency)
        {
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

} // namespace cql

#endif // CQL_UTIL_H_
