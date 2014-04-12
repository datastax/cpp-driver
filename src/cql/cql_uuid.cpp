/*
 *      Copyright (C) 2013 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "cql/cql_uuid.hpp"
#include "cql/internal/cql_serialization.hpp"

#include <boost/thread.hpp>
#include <boost/random/mersenne_twister.hpp>

#include <stdint.h>
#include <algorithm>
#include <exception>

//----------------------------------------------------------------------------------------
namespace { // Anonymous namespace with helper tools. Adapted from boost.
    
template <typename CharIterator>
typename std::iterator_traits<CharIterator>::value_type
get_next_char(CharIterator& begin, CharIterator end) {
    if (begin == end) {
        throw std::invalid_argument("String to make UUID from cannot be empty");
    }
    return *begin++;
}

/*
// WCHAR overloads are commented out. Uncomment them if needed.
unsigned char
get_value(wchar_t c) {
    static wchar_t const*const digits_begin = L"0123456789abcdefABCDEF";
    static wchar_t const*const digits_end   = digits_begin + 22;
    
    static unsigned char const values[] =
        { 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,10,11,12,13,14,15
            , static_cast<unsigned char>(-1) };
    
    wchar_t const* d = std::find(digits_begin, digits_end, c);
    return values[d - digits_begin];
}
bool is_dash(wchar_t c) {
    return c == L'-';
}
bool is_open_brace(wchar_t c) {
    return (c == L'{');
}
void check_close_brace(wchar_t c, wchar_t open_brace) {
    if (open_brace == L'{' && c == L'}') {
        // great
    } else {
        throw std::invalid_argument("String to make UUID from has an unclosed brace");
    }
}
inline wchar_t to_wchar(size_t i) {
    if (i <= 9) {
        return static_cast<wchar_t>(L'0' + i);
    } else {
        return static_cast<wchar_t>(L'a' + (i-10));
    }
}
*/

unsigned char
get_value(char c) {
    static char const * const digits_begin = "0123456789abcdefABCDEF";
    static char const * const digits_end   = digits_begin + 22;
    
    static unsigned char const values[] =
        { 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,10,11,12,13,14,15
            , static_cast<unsigned char>(-1) };
    
    char const* d = std::find(digits_begin, digits_end, c);
    return values[d - digits_begin];
}
bool is_dash(char c) {
    return c == '-';
}
bool is_open_brace(char c) {
    return (c == '{');
}
void check_close_brace(char c, char open_brace) {
    if (open_brace == '{' && c == '}') {
        //great
    } else {
        throw std::invalid_argument("String to make UUID from has an unclosed brace");
    }
}
inline char to_char(size_t i) {
    if (i <= 9) {
        return static_cast<char>('0' + i);
    } else {
        return static_cast<char>('a' + (i-10));
    }
}

} // End of anonymous namespace.
//----------------------------------------------------------------------------------------

cql::cql_uuid_t
cql::cql_uuid_t::create() {
    static boost::mutex mutex;
    boost::mutex::scoped_lock lock(mutex);
    
    static ::uint64_t id = 0xAAAAAAAAAAAAAAAAUL;
    static boost::mt19937_64 mt;

    cql_uuid_t ret;
    *reinterpret_cast< ::uint64_t*>(&ret._uuid[0]) = id++;
    *reinterpret_cast< ::uint64_t*>(&ret._uuid[8]) = mt();
    
    // set some variant and version (adapted from boost::uuids)
    *(ret._uuid+8) &= 0xBF;
    *(ret._uuid+8) |= 0x80;
    *(ret._uuid+6) &= 0x4F;
    *(ret._uuid+6) |= 0x40;

    return ret;
}

cql::cql_uuid_t::cql_uuid_t() {
    for (size_t i = 0u; i < _size; ++i) {
        _uuid[i] = 0;
    }
}

// Adapted from boost::uuids
cql::cql_uuid_t::cql_uuid_t(const std::string& uuid_string) {
    typedef std::string::value_type char_type;
    std::string::const_iterator begin = uuid_string.begin(),
                                end   = uuid_string.end();
    
    // check open brace
    char_type c = get_next_char(begin, end);
    bool has_open_brace = is_open_brace(c);
    char_type open_brace_char = c;
    if (has_open_brace) {
        c = get_next_char(begin, end);
    }
    
    bool has_dashes = false;
    
    size_t i = 0u;
    for (cql_byte_t* it_byte = _uuid; it_byte != _uuid+_size; ++it_byte, ++i) {
        if (it_byte != _uuid) {
            c = get_next_char(begin, end);
        }
        
        if (i == 4) {
            has_dashes = is_dash(c);
            if (has_dashes) {
                c = get_next_char(begin, end);
            }
        }
        
        if (has_dashes) {
            if (i == 6 || i == 8 || i == 10) {
                if (is_dash(c)) {
                    c = get_next_char(begin, end);
                } else {
                    throw std::invalid_argument("String to make UUID from has a misplaced dash");
                }
            }
        }
        
        *it_byte = get_value(c);
        
        c = get_next_char(begin, end);
        *it_byte <<= 4;
        *it_byte |= get_value(c);
    }
    
    // check close brace
    if (has_open_brace) {
        c = get_next_char(begin, end);
        check_close_brace(c, open_brace_char);
    }
}

cql::cql_uuid_t::cql_uuid_t(cql_byte_t* bytes) {
    for (size_t i = 0u; i < _size; ++i) {
        _uuid[i] = bytes[i];
    }
}

cql::cql_uuid_t::cql_uuid_t(const std::vector<cql_byte_t>& bytes) {
    for (size_t i = 0u; i < _size && i < bytes.size(); ++i) {
        _uuid[i] = bytes[i];
    }
}

bool
cql::cql_uuid_t::empty() const {
    for (size_t i = 0u; i < _size; ++i) {
        if (_uuid[i] != 0) {
            return false;
         }
    }
    return true;
}

cql::cql_bigint_t
cql::cql_uuid_t::get_timestamp() const {
    cql_bigint_t ret_val;
    cql_byte_t bytes[sizeof(cql_bigint_t)];
    std::copy(_uuid, _uuid+sizeof(cql_bigint_t), bytes);
    
    // This was rewritten straight from C#:
    bytes[7] &= (cql_byte_t)0x0f;
    // decode_bigint assumes network byte order. UUIDs also do, so can be used here.
    decode_bigint(bytes, ret_val);
    return ret_val;
}

std::string
cql::cql_uuid_t::to_string() const
{
    std::string result;
    result.reserve(36);
    
    size_t i = 0u;
    for (const cql_byte_t* it_data = _uuid; it_data != _uuid+_size; ++it_data, ++i) {
        const size_t hi = ((*it_data) >> 4) & 0x0F;
        result += to_char(hi);
        
        const size_t lo = (*it_data) & 0x0F;
        result += to_char(lo);
        
        if (i == 3 || i == 5 || i == 7 || i == 9) {
            result += '-';
        }
    }
    return result;
}

std::vector<cql::cql_byte_t>
cql::cql_uuid_t::get_data() const {
    return std::vector<cql_byte_t>(_uuid, _uuid + _size);
}

bool
cql::cql_uuid_t::operator==(const cql_uuid_t& other) const {
    for (size_t i = 0u; i < _size; ++i) {
        if (_uuid[i] != other._uuid[i]) {
            return false;
        }
    }
    return true;
}

namespace cql {
    bool
    operator <(const cql::cql_uuid_t& left, const cql::cql_uuid_t& right) {
        return (left.get_timestamp() < right.get_timestamp());
    }
}

