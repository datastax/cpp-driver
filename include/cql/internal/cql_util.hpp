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
#include <boost/asio/ip/address.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "cql/cql.hpp"
#include "cql/internal/cql_system_dependent.hpp"
#include "cql/cql_endpoint.hpp"

namespace cql {

struct cql_hex_char_struct_t {
    unsigned char c;
    cql_hex_char_struct_t(unsigned char _c) : c(_c) { }
};

inline std::ostream&
operator<<(std::ostream& o, const cql_hex_char_struct_t& hs) {
    return (o << std::hex << (int)hs.c);
}

inline cql_hex_char_struct_t
hex(unsigned char _c) {
    return cql_hex_char_struct_t(_c);
}

// Safe version of strncpy (this method always null terminates
// dest buffer).
char*
safe_strncpy(char* dest, const char* src, const size_t count);

// When str is NULL returns empty string(""), in any other
// case returns str.
inline const char*
empty_when_null(const char* str) {
		if(!str)
			return "";
		return str;
}

// tries to convert given string to boost asio ip address.
// return false when conversion fails, return true otherwise.
// if this returns true then result will contain valid ip address.
bool
to_ipaddr(const std::string& str, boost::asio::ip::address& result);

std::vector<cql_byte_t>
compress(const std::vector<cql_byte_t>& buffer, cql_compression_enum e);

void
compress_inplace(std::vector<cql_byte_t>& buffer, cql_compression_enum e);

std::vector<cql_byte_t>
uncompress(const std::vector<cql_byte_t>& buffer, cql_compression_enum e);

void
uncompress_inplace(std::vector<cql_byte_t>& buffer, cql_compression_enum e);

// returns current UTC time.
// result of this functions depends on local
// system settings which may be modified by user.
boost::posix_time::ptime
utc_now();

} // namespace cql

#endif // CQL_UTIL_H_
