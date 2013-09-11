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

#include <vector>

#ifdef _WIN32
#include <Winsock2.h>
#else
#include <arpa/inet.h>
#endif

#include <boost/foreach.hpp>
#include <boost/detail/endian.hpp>
#include "cassandra/internal/cql_defines.hpp"

#include "cassandra/cql_serialization.hpp"

inline double
swap_double(double source) {
#ifdef _WIN32
    uint64_t bytes = *reinterpret_cast<uint64_t*>(&source);
    uint64_t swapped = _byteswap_uint64(bytes);
    return *reinterpret_cast<double*>(&swapped);
#else
    union {
        uint64_t bytes;
        double src;
    } q;
    q.src = source;
    q.bytes = __builtin_bswap64(q.bytes);
    return q.src;
#endif
}

inline float
swap_float(float source) {
#ifdef _WIN32
    uint32_t bytes = *reinterpret_cast<uint32_t*>(&source);
    uint32_t swapped = _byteswap_ulong(bytes);
    return *reinterpret_cast<float*>(&swapped);
#else
    union {
        uint32_t bytes;
        float src;
    } q;
    q.src = source;
    q.bytes = __builtin_bswap32(q.bytes);
    return q.src;
#endif
}

// stolen from http://stackoverflow.com/a/4311985/67707
template <class builtin>
builtin ntoh(const builtin input) {
    if ((int)ntohs(1) != 1) {
        union {
            char buffer[sizeof(builtin)];
            builtin data;
        } in, out;

        in.data = input;
        for (size_t i = 0 ; i < sizeof(builtin); i++) {
            out.buffer[i] = in.buffer[sizeof(builtin) - i - 1];
        }

        return out.data;
    }
    return input;
}

std::ostream&
cql::encode_bool(std::ostream& output,
                 bool value) {
    if (value) {
        output.put(0x01);
    } else {
        output.put(0x00);
    }
    return output;
}

void
cql::encode_bool(std::vector<cql::cql_byte_t>& output,
                 const bool value) {
    output.resize(1, 0);
    if (value) {
        output[0] = 0x01;
    } else {
        output[0] = 0x00;
    }
}

std::istream&
cql::decode_bool(std::istream& input,
                 bool& value) {
    std::vector<char> v(1, 0);
    input.read(reinterpret_cast<char*>(&v), 1);
    value = v[0] == 0x01;
    return input;
}

bool
cql::decode_bool(const std::vector<cql::cql_byte_t>& input) {
    return input[0] ? true : false;
}

std::ostream&
cql::encode_short(std::ostream& output,
                  const cql::cql_short_t value) {
    cql::cql_short_t s = htons(value);
    output.write(reinterpret_cast<char*>(&s), sizeof(s));
    return output;
}

void
cql::encode_short(std::vector<cql::cql_byte_t>& output,
                  const cql::cql_short_t value) {
    cql::cql_short_t s = htons(value);
    output.resize(sizeof(s));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&s);
    output.assign(it, it + sizeof(s));
}

std::istream&
cql::decode_short(std::istream& input,
                  cql::cql_short_t& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = ntohs(value);
    return input;
}

cql::cql_short_t
cql::decode_short(const std::vector<cql::cql_byte_t>& input) {
    return ntohs(*(reinterpret_cast<const cql_short_t*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_short(cql::cql_byte_t* input,
                  cql::cql_short_t& value) {
    value = ntohs(*(reinterpret_cast<const cql_short_t*>(input)));
    return input + sizeof(cql::cql_short_t);
}

std::ostream&
cql::encode_int(std::ostream& output,
                const cql::cql_int_t value) {
    cql::cql_int_t l = htonl(value);
    output.write(reinterpret_cast<char*>(&l), sizeof(l));
    return output;
}

void
cql::encode_int(std::vector<cql::cql_byte_t>& output,
                const cql::cql_int_t value) {
    cql::cql_int_t l = htonl(value);
    output.resize(sizeof(l));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&l);
    output.assign(it, it + sizeof(l));
}

std::istream&
cql::decode_int(std::istream& input,
                cql::cql_int_t& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = ntohl(value);
    return input;
}

cql::cql_int_t
cql::decode_int(const std::vector<cql::cql_byte_t>& input) {
    return ntohl(*(reinterpret_cast<const cql::cql_int_t*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_int(cql::cql_byte_t* input,
                cql::cql_int_t& output) {
    output = ntohl(*(reinterpret_cast<const cql::cql_int_t*>(input)));
    return input + sizeof(cql::cql_int_t);
}

std::ostream&
cql::encode_float(std::ostream& output,
                  const float value) {
    float swapped_float = swap_float(value);
    cql::cql_int_t l = *reinterpret_cast<cql::cql_int_t *>(&swapped_float);
    output.write(reinterpret_cast<char*>(&l), sizeof(l));
    return output;
}

void
cql::encode_float(std::vector<cql::cql_byte_t>& output,
                  const float value) {
    cql::cql_int_t l = swap_float(value);
    output.resize(sizeof(l));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&l);
    output.assign(it, it + sizeof(l));
}

std::istream&
cql::decode_float(std::istream& input,
                  float& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = swap_float(value);
    return input;
}

float
cql::decode_float(const std::vector<cql::cql_byte_t>& input) {
    return swap_float(*(reinterpret_cast<const float*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_float(cql::cql_byte_t* input,
                  float& output) {
    output = swap_float(*(reinterpret_cast<const float*>(input)));
    return input + sizeof(float);
}

std::ostream&
cql::encode_double(std::ostream& output,
                   const double value) {
    double d = swap_double(value);
    output.write(reinterpret_cast<char*>(&d), sizeof(d));
    return output;
}

void
cql::encode_double(std::vector<cql::cql_byte_t>& output,
                   const double value) {
    double d = swap_double(value);
    output.resize(sizeof(d));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&d);
    output.assign(it, it + sizeof(d));
}

std::istream&
cql::decode_double(std::istream& input,
                   double& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = swap_double(value);
    return input;
}

double
cql::decode_double(const std::vector<cql::cql_byte_t>& input) {
    return swap_double(*(reinterpret_cast<const double*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_double(cql::cql_byte_t* input,
                   double& output) {
    output = swap_double(*(reinterpret_cast<const double*>(input)));
    return input + sizeof(double);
}

std::ostream&
cql::encode_bigint(std::ostream& output,
                   const cql::cql_bigint_t value) {
    cql::cql_bigint_t d = ntoh<cql::cql_bigint_t>(value);
    output.write(reinterpret_cast<char*>(&d), sizeof(d));
    return output;
}

void
cql::encode_bigint(std::vector<cql::cql_byte_t>& output,
                   const cql::cql_bigint_t value) {
    cql::cql_bigint_t d = ntoh<cql::cql_bigint_t>(value);
    output.resize(sizeof(d));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&d);
    output.assign(it, it + sizeof(d));
}

std::istream&
cql::decode_bigint(std::istream& input,
                   cql::cql_bigint_t& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = ntoh<cql::cql_bigint_t>(value);
    return input;
}

cql::cql_bigint_t
cql::decode_bigint(const std::vector<cql::cql_byte_t>& input) {
    return ntoh<cql::cql_bigint_t>(*(reinterpret_cast<const cql::cql_bigint_t*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_bigint(cql::cql_byte_t* input,
                   cql::cql_bigint_t& output) {
    output = ntoh<cql::cql_bigint_t>(*(reinterpret_cast<const cql::cql_bigint_t*>(input)));
    return input + sizeof(cql::cql_bigint_t);
}

std::ostream&
cql::encode_string(std::ostream& output,
                   const std::string& value) {
    cql::encode_short(output, value.size());
    output.write(reinterpret_cast<const char*>(value.c_str()), value.size());
    return output;
}

std::istream&
cql::decode_string(std::istream& input,
                   std::string& value) {
    cql::cql_short_t len;
    cql::decode_short(input, len);

    std::vector<char> buffer(len, 0);
    input.read(&buffer[0], len);
    value.assign(buffer.begin(), buffer.end());
    return input;
}

std::string
cql::decode_string(const std::vector<cql::cql_byte_t>& input) {
    return std::string(input.begin(), input.end());
}

cql::cql_byte_t*
cql::decode_string(cql::cql_byte_t* input,
                   std::string& value) {
    cql::cql_short_t len;
    input = cql::decode_short(input, len);
    value.assign(input, input + len);
    return input + len;
}

std::ostream&
cql::encode_bytes(std::ostream& output,
                  const std::vector<cql::cql_byte_t>& value) {
    cql::cql_int_t len = htonl(value.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));
    output.write(reinterpret_cast<const char*>(&value[0]), value.size());
    return output;
}

std::istream&
cql::decode_bytes(std::istream& input,
                  std::vector<cql::cql_byte_t>& value) {
    cql::cql_int_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohl(len);

    value.resize(len, 0);
    input.read(reinterpret_cast<char*>(&value[0]), len);
    return input;
}

std::ostream&
cql::encode_short_bytes(std::ostream& output,
                        const std::vector<cql::cql_byte_t>& value) {
    cql::cql_short_t len = htons(value.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));
    output.write(reinterpret_cast<const char*>(&value[0]), value.size());
    return output;
}

std::istream&
cql::decode_short_bytes(std::istream& input,
                        std::vector<cql::cql_byte_t>& value) {
    cql::cql_short_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohs(len);

    value.resize(len, 0);
    input.read(reinterpret_cast<char*>(&value[0]), len);
    return input;
}

cql::cql_byte_t*
cql::decode_short_bytes(cql::cql_byte_t* input,
                        std::vector<cql::cql_byte_t>& value) {
    cql::cql_short_t len;
    input = cql::decode_short(input, len);

    value.resize(len, 0);
    value.assign(input, input + len);
    return input;
}

std::ostream&
cql::encode_long_string(std::ostream& output,
                        const std::string& value) {
    cql::cql_int_t len = htonl(value.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));
    output.write(reinterpret_cast<const char*>(value.c_str()), value.size());
    return output;
}

std::istream&
cql::decode_long_string(std::istream& input,
                        std::string& value) {
    cql::cql_int_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohl(len);

    std::vector<char> buffer(len);
    input.read(&buffer[0], len);
    value.assign(buffer.begin(), buffer.end());
    return input;
}

std::ostream&
cql::encode_string_list(std::ostream& output,
                        const std::list<std::string>& list) {
    cql::cql_short_t len = htons(list.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));
    BOOST_FOREACH(const std::string& s, list) {
        cql::encode_string(output, s);
    }
    return output;
}

std::istream&
cql::decode_string_list(std::istream& input,
                        std::list<std::string>& list) {
    cql::cql_short_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohs(len);

    list.clear();
    for (int i = 0; i < len; i++) {
        std::string s;
        cql::decode_string(input, s);
        list.push_back(s);
    }

    return input;
}

std::ostream&
cql::encode_string_map(std::ostream& output,
                       const std::map<std::string, std::string>& map) {
    cql::cql_short_t len = htons(map.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));

    std::map<std::string, std::string>::const_iterator it = map.begin();
    for (; it != map.end(); it++) {
        cql::encode_string(output, (*it).first);
        cql::encode_string(output, (*it).second);
    }

    return output;
}

std::istream&
cql::decode_string_map(std::istream& input,
                       std::map<std::string, std::string>& map) {
    cql::cql_short_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohs(len);

    map.clear();
    for (int i = 0; i < len; i++) {
        std::string key;
        std::string value;
        cql::decode_string(input, key);
        cql::decode_string(input, value);
        map.insert(std::pair<std::string, std::string>(key, value));
    }

    return input;
}

std::ostream&
cql::encode_string_multimap(std::ostream& output,
                            const std::map<std::string, std::list<std::string> >& map) {
    cql::cql_short_t len = htons(map.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));

    std::map<std::string, std::list<std::string> >::const_iterator it = map.begin();
    for (; it != map.end(); it++) {
        cql::encode_string(output, (*it).first);
        cql::encode_string_list(output, (*it).second);
    }

    return output;
}

std::istream&
cql::decode_string_multimap(std::istream& input,
                            std::map<std::string, std::list<std::string> >& map) {
    cql::cql_short_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohs(len);

    map.clear();
    for (int i = 0; i < len; i++) {
        std::string key;
        cql::decode_string(input, key);

        std::list<std::string> values;
        cql::decode_string_list(input, values);
        map.insert(std::pair<std::string, std::list<std::string> >(key, values));
    }

    return input;
}

inline cql::cql_column_type_enum
short_to_column_type(cql::cql_short_t input) {
    switch (input) {
    case 0x0000:
        return cql::CQL_COLUMN_TYPE_CUSTOM;
    case 0x0001:
        return cql::CQL_COLUMN_TYPE_ASCII;
    case 0x0002:
        return cql::CQL_COLUMN_TYPE_BIGINT;
    case 0x0003:
        return cql::CQL_COLUMN_TYPE_BLOB;
    case 0x0004:
        return cql::CQL_COLUMN_TYPE_BOOLEAN;
    case 0x0005:
        return cql::CQL_COLUMN_TYPE_COUNTER;
    case 0x0006:
        return cql::CQL_COLUMN_TYPE_DECIMAL;
    case 0x0007:
        return cql::CQL_COLUMN_TYPE_DOUBLE;
    case 0x0008:
        return cql::CQL_COLUMN_TYPE_FLOAT;
    case 0x0009:
        return cql::CQL_COLUMN_TYPE_INT;
    case 0x000A:
        return cql::CQL_COLUMN_TYPE_TEXT;
    case 0x000B:
        return cql::CQL_COLUMN_TYPE_TIMESTAMP;
    case 0x000C:
        return cql::CQL_COLUMN_TYPE_UUID;
    case 0x000D:
        return cql::CQL_COLUMN_TYPE_VARCHAR;
    case 0x000E:
        return cql::CQL_COLUMN_TYPE_VARINT;
    case 0x000F:
        return cql::CQL_COLUMN_TYPE_TIMEUUID;
    case 0x0010:
        return cql::CQL_COLUMN_TYPE_INET;
    case 0x0020:
        return cql::CQL_COLUMN_TYPE_LIST;
    case 0x0021:
        return cql::CQL_COLUMN_TYPE_MAP;
    case 0x0022:
        return cql::CQL_COLUMN_TYPE_SET;
    default:
        return cql::CQL_COLUMN_TYPE_UNKNOWN;
    }
}

std::ostream&
cql::encode_option(std::ostream& output,
                   cql::cql_column_type_enum& id,
                   const std::string& value) {
    cql::encode_short(output, reinterpret_cast<cql::cql_short_t&>(id));
    if (id == CQL_COLUMN_TYPE_CUSTOM) {
        cql::encode_string(output, value);
    }
    return output;
}

std::istream&
cql::decode_option(std::istream& input,
                   cql::cql_column_type_enum& id,
                   std::string& value) {
    cql::cql_short_t t = 0xFFFF;
    cql::decode_short(input, t);
    id = short_to_column_type(t);

    if (id == CQL_COLUMN_TYPE_CUSTOM) {
        cql::decode_string(input, value);
    }
    return input;
}

cql::cql_byte_t*
cql::decode_option(cql::cql_byte_t* input,
                   cql::cql_column_type_enum& id,
                   std::string& value) {
    cql::cql_short_t t = 0xFFFF;
    input = cql::decode_short(input, t);
    id = short_to_column_type(t);

    if (id == cql::CQL_COLUMN_TYPE_CUSTOM) {
        input = cql::decode_string(input, value);
    }
    return input;
}

std::ostream&
cql::encode_inet(std::ostream& output,
                 const std::string& ip,
                 const cql::cql_int_t port) {
    cql::encode_string(output, ip);
    cql::encode_int(output, port);
    return output;
}

std::istream&
cql::decode_inet(std::istream& input,
                 std::string& ip,
                 cql::cql_int_t& port) {
    cql::decode_string(input, ip);
    cql::decode_int(input, port);
    return input;
}
