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
#include <boost/function.hpp>

#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_map_impl.hpp"

using namespace std;

cql::cql_map_impl_t::cql_map_impl_t(cql::cql_byte_t*          start,
                                    cql::cql_column_type_enum key_type,
                                    cql::cql_column_type_enum value_type,
                                    string&              key_custom_class,
                                    string&              value_custom_class) :
    _start(start),
    _key_type(key_type),
    _value_type(value_type),
    _key_custom_class(key_custom_class),
    _value_custom_class(value_custom_class) 
{
    cql::cql_short_t count = 0;
    cql::cql_byte_t* _pos = cql::decode_short(_start, count);
    
    _keys.reserve(count);
    _values.reserve(count);

    // get keys and values positions
    // [length][key1_size][key1][value1_size][value1]...[value_length]
    for (int i = 0; i < count; ++i) {
        _keys.push_back(_pos);
        cql::cql_short_t len = 0;
        _pos = cql::decode_short(_pos, len);
        _pos += len;

        _values.push_back(_pos);
        _pos = cql::decode_short(_pos, len);
        _pos += len;
    }
}

bool
cql::cql_map_impl_t::get_key_bool(size_t i,
                                  bool& output) const {
    if (i >= _keys.size()) {
        return false;
    }

    output = *(_keys[i] + sizeof(cql_short_t)) != 0x00;
    return true;
}

bool
cql::cql_map_impl_t::get_key_int(size_t i,
                                 cql_int_t& output) const {
    if (i >= _keys.size()) {
        return false;
    }

    cql::decode_int(_keys[i] + sizeof(cql_short_t), output);
    return true;
}

bool
cql::cql_map_impl_t::get_key_float(size_t i,
                                   float& output) const {
    if (i >= _keys.size()) {
        return false;
    }

    cql::decode_float(_keys[i] + sizeof(cql_short_t), output);
    return true;
}

bool
cql::cql_map_impl_t::get_key_double(size_t i,
                                    double& output) const {
    if (i >= _keys.size()) {
        return false;
    }

    cql::decode_double(_keys[i] + sizeof(cql_short_t), output);
    return true;
}

bool
cql::cql_map_impl_t::get_key_bigint(size_t i,
                                    cql::cql_bigint_t& output) const {
    if (i >= _keys.size()) {
        return false;
    }

    cql::decode_bigint(_keys[i] + sizeof(cql_short_t), output);
    return true;
}

bool
cql::cql_map_impl_t::get_key_string(size_t i,
                                    string& output) const {
    cql_byte_t* data = 0;
    cql_short_t size = 0;
    
    if (get_key_data(i, &data, size)) {
        output.assign(data, data + size);
        return true;
    }
    return false;
}

bool
cql::cql_map_impl_t::get_key_data(size_t i,
                                  cql::cql_byte_t** output,
                                  cql::cql_short_t& size) const {
    if (i >= _keys.size()) {
        return false;
    }

    *output = cql::decode_short(_keys[i], size);
    return true;
}

bool
cql::cql_map_impl_t::get_value_bool(size_t i,
                                    bool& output) const {
    if (i >= _values.size()) {
        return false;
    }

    output = *(_values[i] + sizeof(cql_short_t)) != 0x00;
    return true;
}

bool
cql::cql_map_impl_t::get_value_int(size_t i,
                                   cql_int_t& output) const {
    if (i >= _values.size()) {
        return false;
    }

    cql::decode_int(_values[i] + sizeof(cql_short_t), output);
    return true;
}

bool
cql::cql_map_impl_t::get_value_float(size_t i,
                                     float& output) const {
    if (i >= _values.size()) {
        return false;
    }

    cql::decode_float(_values[i] + sizeof(cql_short_t), output);
    return true;
}

bool
cql::cql_map_impl_t::get_value_double(size_t i,
                                      double& output) const {
    if (i >= _values.size()) {
        return false;
    }

    cql::decode_double(_values[i] + sizeof(cql_short_t), output);
    return true;
}

bool
cql::cql_map_impl_t::get_value_bigint(size_t i,
                                      cql::cql_bigint_t& output) const {
    if (i >= _values.size()) {
        return false;
    }

    cql::decode_bigint(_values[i] + sizeof(cql_short_t), output);
    return true;
}

bool
cql::cql_map_impl_t::get_value_string(size_t i,
                                      string& output) const {
    cql_byte_t* data = 0;
    cql_short_t size = 0;
    
    if (get_value_data(i, &data, size)) {
        output.assign(data, data + size);
        return true;
    }
    return false;
}

bool
cql::cql_map_impl_t::get_value_data(size_t i,
                                    cql::cql_byte_t** output,
                                    cql::cql_short_t& size) const {
    if (i >= _values.size()) {
        return false;
    }

    *output = cql::decode_short(_values[i], size);
    return true;
}

string
cql::cql_map_impl_t::str() const {
    return "map";
}

cql::cql_column_type_enum
cql::cql_map_impl_t::key_type() const {
    return _key_type;
}

const string&
cql::cql_map_impl_t::key_custom_class() const {
    return _key_custom_class;
}

cql::cql_column_type_enum
cql::cql_map_impl_t::value_type() const {
    return _value_type;
}

const string&
cql::cql_map_impl_t::value_custom_class() const {
    return _value_custom_class;
}

size_t
cql::cql_map_impl_t::size() const {
    return _keys.size();
}
