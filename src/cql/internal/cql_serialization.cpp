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
#include <vector>
#include <map>
#include <string>

#ifdef _WIN32
#include <Winsock2.h>
#else
#include <arpa/inet.h>
#endif

#include <boost/foreach.hpp>
#include <boost/detail/endian.hpp>
#include <boost/lexical_cast.hpp>

#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_serialization.hpp"

using namespace std;

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

ostream&
cql::encode_bool(ostream& output,
                 bool value) {
    if (value) {
        output.put(0x01);
    } else {
        output.put(0x00);
    }
    return output;
}

void
cql::encode_bool(vector<cql::cql_byte_t>& output,
                 const bool value) {
    output.resize(1, 0);
    if (value) {
        output[0] = 0x01;
    } else {
        output[0] = 0x00;
    }
}

istream&
cql::decode_bool(istream& input,
                 bool& value) {
    vector<char> v(1, 0);
    input.read(reinterpret_cast<char*>(v.data()), 1);
    
    value = (v[0] != 0x00);
    return input;
}

bool
cql::decode_bool(const vector<cql::cql_byte_t>& input) {
    return input[0] ? true : false;
}

ostream&
cql::encode_short(ostream& output,
                  const cql::cql_short_t value) {
    cql::cql_short_t s = htons(value);
    output.write(reinterpret_cast<char*>(&s), sizeof(s));
    return output;
}

void
cql::encode_short(vector<cql::cql_byte_t>& output,
                  const cql::cql_short_t value) {
    cql::cql_short_t s = htons(value);
    output.resize(sizeof(s));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&s);
    output.assign(it, it + sizeof(s));
}

istream&
cql::decode_short(istream& input,
                  cql::cql_short_t& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = ntohs(value);
    return input;
}

cql::cql_short_t
cql::decode_short(const vector<cql::cql_byte_t>& input) {
    return ntohs(*(reinterpret_cast<const cql_short_t*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_short(cql::cql_byte_t* input,
                   cql::cql_short_t& value) {
    value = ntohs(*(reinterpret_cast<const cql_short_t*>(input)));
    return input + sizeof(cql::cql_short_t);
}

ostream&
cql::encode_int(ostream& output,
                const cql::cql_int_t value) {
    cql::cql_int_t l = htonl(value);
    output.write(reinterpret_cast<char*>(&l), sizeof(l));
    return output;
}

void
cql::encode_int(vector<cql::cql_byte_t>& output,
                const cql::cql_int_t value) {
    cql::cql_int_t l = htonl(value);
    output.resize(sizeof(l));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&l);
    output.assign(it, it + sizeof(l));
}

istream&
cql::decode_int(istream& input,
                cql::cql_int_t& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = ntohl(value);
    return input;
}

cql::cql_int_t
cql::decode_int(const vector<cql::cql_byte_t>& input) {
    return ntohl(*(reinterpret_cast<const cql::cql_int_t*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_int(cql::cql_byte_t* input,
                cql::cql_int_t& output) {
    output = ntohl(*(reinterpret_cast<const cql::cql_int_t*>(input)));
    return input + sizeof(cql::cql_int_t);
}

ostream&
cql::encode_float(ostream& output,
                  const float value) {
    float network_order_float = swap_float(value);
    cql::cql_int_t l = *reinterpret_cast<cql::cql_int_t *>(&network_order_float);
    output.write(reinterpret_cast<char*>(&l), sizeof(l));
    return output;
}

void
cql::encode_float(vector<cql::cql_byte_t>& output,
                  const float value) {
	float swapped = swap_float(value);
    cql::cql_int_t l = *reinterpret_cast<cql::cql_int_t*>(&swapped);
    output.resize(sizeof(l));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&l);
    output.assign(it, it + sizeof(l));
}

istream&
cql::decode_float(istream& input,
                  float& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = swap_float(value);
    return input;
}

float
cql::decode_float(const vector<cql::cql_byte_t>& input) {
    return swap_float(*(reinterpret_cast<const float*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_float(cql::cql_byte_t* input,
                  float& output) {
    output = swap_float(*(reinterpret_cast<const float*>(input)));
    return input + sizeof(float);
}

ostream&
cql::encode_double(ostream& output,
                   const double value) {
    double d = swap_double(value);
    output.write(reinterpret_cast<char*>(&d), sizeof(d));
    return output;
}

void
cql::encode_double(vector<cql::cql_byte_t>& output,
                   const double value) {
    double d = swap_double(value);
    output.resize(sizeof(d));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&d);
    output.assign(it, it + sizeof(d));
}

istream&
cql::decode_double(istream& input,
                   double& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = swap_double(value);
    return input;
}

double
cql::decode_double(const vector<cql::cql_byte_t>& input) {
    return swap_double(*(reinterpret_cast<const double*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_double(cql::cql_byte_t* input,
                   double& output) {
    output = swap_double(*(reinterpret_cast<const double*>(input)));
    return input + sizeof(double);
}

ostream&
cql::encode_bigint(ostream& output,
                   const cql::cql_bigint_t value) {
    cql::cql_bigint_t d = ntoh<cql::cql_bigint_t>(value);
    output.write(reinterpret_cast<char*>(&d), sizeof(d));
    return output;
}

void
cql::encode_bigint(vector<cql::cql_byte_t>& output,
                   const cql::cql_bigint_t value) {
    cql::cql_bigint_t d = ntoh<cql::cql_bigint_t>(value);
    output.resize(sizeof(d));
    const cql::cql_byte_t* it = reinterpret_cast<cql::cql_byte_t*>(&d);
    output.assign(it, it + sizeof(d));
}

istream&
cql::decode_bigint(istream& input,
                   cql::cql_bigint_t& value) {
    input.read(reinterpret_cast<char*>(&value), sizeof(value));
    value = ntoh<cql::cql_bigint_t>(value);
    return input;
}

cql::cql_bigint_t
cql::decode_bigint(const vector<cql::cql_byte_t>& input) {
    return ntoh<cql::cql_bigint_t>(*(reinterpret_cast<const cql::cql_bigint_t*>(&input[0])));
}

cql::cql_byte_t*
cql::decode_bigint(cql::cql_byte_t* input,
                   cql::cql_bigint_t& output) {
    output = ntoh<cql::cql_bigint_t>(*(reinterpret_cast<const cql::cql_bigint_t*>(input)));
    return input + sizeof(cql::cql_bigint_t);
}

ostream&
cql::encode_string(ostream& output,
                   const string& value) {
    cql::encode_short(output, value.size());
    output.write(reinterpret_cast<const char*>(value.c_str()), value.size());
    return output;
}

istream&
cql::decode_string(istream& input,
                   string& value) {
    cql::cql_short_t len;
    cql::decode_short(input, len);

    vector<char> buffer(len, 0);
    input.read(&buffer[0], len);
    value.assign(buffer.begin(), buffer.end());
    return input;
}

string
cql::decode_string(const vector<cql::cql_byte_t>& input) {
    return string(input.begin(), input.end());
}

cql::cql_byte_t*
cql::decode_string(cql::cql_byte_t* input,
                   string& value) {
    cql::cql_short_t len;
    input = cql::decode_short(input, len);
    value.assign(input, input + len);
    return input + len;
}

ostream&
cql::encode_bytes(ostream& output,
                  const vector<cql::cql_byte_t>& value) {
    cql::cql_int_t len = htonl(value.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));
    output.write(reinterpret_cast<const char*>(&value[0]), value.size());
    return output;
}

istream&
cql::decode_bytes(istream& input,
                  vector<cql::cql_byte_t>& value) {
    cql::cql_int_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohl(len);

    value.resize(len, 0);
    input.read(reinterpret_cast<char*>(&value[0]), len);
    return input;
}

ostream&
cql::encode_short_bytes(ostream& output,
                        const vector<cql::cql_byte_t>& value) {
    cql::cql_short_t len = htons(value.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));
    output.write(reinterpret_cast<const char*>(&value[0]), value.size());
    return output;
}

istream&
cql::decode_short_bytes(istream& input,
                        vector<cql::cql_byte_t>& value) {
    cql::cql_short_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohs(len);

    value.resize(len, 0);
    input.read(reinterpret_cast<char*>(&value[0]), len);
    return input;
}

cql::cql_byte_t*
cql::decode_short_bytes(cql::cql_byte_t* input,
                        vector<cql::cql_byte_t>& value) {
    cql::cql_short_t len;
    input = cql::decode_short(input, len);

    value.resize(len, 0);
    value.assign(input, input + len);
    return input;
}

ostream&
cql::encode_long_string(ostream& output,
                        const string& value) {
    cql::cql_int_t len = htonl(value.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));
    output.write(reinterpret_cast<const char*>(value.c_str()), value.size());
    return output;
}

istream&
cql::decode_long_string(istream& input,
                        string& value) {
    cql::cql_int_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohl(len);

    vector<char> buffer(len);
    input.read(&buffer[0], len);
    value.assign(buffer.begin(), buffer.end());
    return input;
}

ostream&
cql::encode_string_list(ostream& output,
                        const list<string>& list) {
    cql::cql_short_t len = htons(list.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));
    BOOST_FOREACH(const string& s, list) {
        cql::encode_string(output, s);
    }
    return output;
}

istream&
cql::decode_string_list(istream& input,
                        list<string>& list) {
    cql::cql_short_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohs(len);

    list.clear();
    for (int i = 0; i < len; i++) {
        string s;
        cql::decode_string(input, s);
        list.push_back(s);
    }

    return input;
}

ostream&
cql::encode_string_map(ostream& output,
                       const map<string, string>& map) {
    cql::cql_short_t len = htons(map.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));

    std::map<string,string>::const_iterator it = map.begin();
    for (; it != map.end(); ++it) {
        cql::encode_string(output, (*it).first);
        cql::encode_string(output, (*it).second);
    }

    return output;
}

istream&
cql::decode_string_map(istream& input,
                       map<string, string>& map) {
    cql::cql_short_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohs(len);

    map.clear();
    for (int i = 0; i < len; i++) {
        string key;
        string value;
        cql::decode_string(input, key);
        cql::decode_string(input, value); 
        map.insert(make_pair(key, value));
    }

    return input;
}

ostream&
cql::encode_string_multimap(ostream& output,
                            const map<string, list<string> >& map) {
    cql::cql_short_t len = htons(map.size());
    output.write(reinterpret_cast<char*>(&len), sizeof(len));

    std::map<string, list<string> >::const_iterator it = map.begin();
    for (; it != map.end(); ++it) {
        cql::encode_string(output, (*it).first);
        cql::encode_string_list(output, (*it).second);
    }

    return output;
}

istream&
cql::decode_string_multimap(istream& input,
                            map<string, list<string> >& map) {
    cql::cql_short_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    len = ntohs(len);

    map.clear();
    for (int i = 0; i < len; i++) {
        string key;
        cql::decode_string(input, key);

        list<string> values;
        cql::decode_string_list(input, values);
        map.insert(make_pair(key, values));
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

ostream&
cql::encode_option(ostream& output,
                   cql::cql_column_type_enum& id,
                   const string& value) {
    cql::encode_short(output, reinterpret_cast<cql::cql_short_t&>(id));
    if (id == CQL_COLUMN_TYPE_CUSTOM) {
        cql::encode_string(output, value);
    }
    return output;
}

istream&
cql::decode_option(istream& input,
                   cql::cql_column_type_enum& id,
                   string& value) {
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
                   string& value) {
    cql::cql_short_t t = 0xFFFF;
    input = cql::decode_short(input, t);
    id = short_to_column_type(t);

    if (id == cql::CQL_COLUMN_TYPE_CUSTOM) {
        input = cql::decode_string(input, value);
    }
    return input;
}

std::string
cql::decode_ipv4_from_bytes(const cql::cql_byte_t* data)
{
    int digit0 = int(data[0]);
    int digit1 = int(data[1]);
    int digit2 = int(data[2]);
    int digit3 = int(data[3]);
    
    return boost::lexical_cast<std::string>(digit0) + "."
            + boost::lexical_cast<std::string>(digit1) + "."
            + boost::lexical_cast<std::string>(digit2) + "."
            + boost::lexical_cast<std::string>(digit3);
}

ostream&
cql::encode_inet(ostream& output,
                 const string& ip,
                 const cql::cql_int_t port) {
    
    // FIXME: encode address length with just one byte.
    cql::encode_string(output, ip);
    cql::encode_int(output, port);
    return output;
}

istream&
cql::decode_inet(istream& input,
                 string& ip,
                 cql::cql_int_t& port) {
    cql::cql_byte_t len;
    input.read(reinterpret_cast<char*>(&len), sizeof(len));
    
    vector<cql::cql_byte_t> buffer(len, 0);
    input.read(reinterpret_cast<char*>(&buffer[0]), len);
    ip = decode_ipv4_from_bytes(&buffer[0]);
    //TODO: handle ipv6

    cql::decode_int(input, port);
    return input;
}
