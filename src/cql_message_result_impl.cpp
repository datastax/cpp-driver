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
#include <iomanip>
#include <iostream>
#include <sstream>
#include <boost/foreach.hpp>
#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_vector_stream.hpp"
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_list_impl.hpp"
#include "cql/internal/cql_map_impl.hpp"
#include "cql/internal/cql_set_impl.hpp"
#include "cql/internal/cql_util.hpp"

#include "cql/internal/cql_message_result_impl.hpp"

std::string
result_type_string(cql::cql_short_t t) {
    switch(t) {

    case cql::CQL_RESULT_ROWS:
        return "CQL_RESULT_ROWS";
        break;

    case cql::CQL_RESULT_SET_KEYSPACE:
        return "CQL_RESULT_SET_KEYSPACE";
        break;

    case cql::CQL_RESULT_PREPARED:
        return "CQL_RESULT_PREPARED";
        break;

    case cql::CQL_RESULT_VOID:
        return "CQL_RESULT_VOID";
        break;

    default:
        return "UNKNOWN";
        break;
    }
}

cql::cql_message_result_impl_t::cql_message_result_impl_t() :
    _buffer(new std::vector<cql_byte_t>()),
    _pos(0),
    _row_pos(0),
    _row_count(0),
    _column_count(0),
    _query_id(0),
    _result_type(cql::CQL_RESULT_VOID)
{}

cql::cql_message_result_impl_t::cql_message_result_impl_t(size_t size) :
    _buffer(new std::vector<cql_byte_t>(size)),
    _pos(0),
    _row_pos(0),
    _row_count(0),
    _column_count(0),
    _query_id(0),
    _result_type(cql::CQL_RESULT_VOID)
{}

cql::cql_message_buffer_t
cql::cql_message_result_impl_t::buffer() {
    return _buffer;
}

const cql::cql_result_metadata_t&
cql::cql_message_result_impl_t::get_metadata() {
    return _metadata;
}

cql::cql_result_type_enum
cql::cql_message_result_impl_t::result_type() const {
    return _result_type;
}

cql::cql_opcode_enum
cql::cql_message_result_impl_t::opcode() const {
    return CQL_OPCODE_RESULT;
}

cql::cql_int_t
cql::cql_message_result_impl_t::size() const {
    return _buffer->size();
}

std::string
cql::cql_message_result_impl_t::str() const {
    std::stringstream output;
    output << std::string("RESULT ") << result_type_string(_result_type);

    if (! _query_id.empty()) {
        output << " QUERY_ID 0x";
        output << std::setfill('0');
        BOOST_FOREACH(cql::cql_byte_t c, _query_id) {
            output << std::setw(2) << cql::hex(c);
        }
    }
    output << " ROW_COUNT " << _row_count;
    output << " METADATA " << _metadata.str();
    return output.str();
}

bool
cql::cql_message_result_impl_t::consume(cql::cql_error_t*) {
    _keyspace_name.clear();
    _row_count = 0;
    _row_pos = 0;
    _pos = &((*_buffer)[0]);

    cql::cql_int_t result_type = 0;
    _pos = cql::decode_int(_pos, result_type);

    switch (result_type) {

    case 0x0001:
        _result_type = cql::CQL_RESULT_VOID;
        break;

    case 0x0002:
        _result_type = cql::CQL_RESULT_ROWS;
        break;

    case 0x0003:
        _result_type = cql::CQL_RESULT_SET_KEYSPACE;
        break;

    case 0x0004:
        _result_type = cql::CQL_RESULT_PREPARED;
        break;

    case 0x0005:
        _result_type = cql::CQL_RESULT_SCHEMA_CHANGE;
        break;
    }

    switch(_result_type) {

    case CQL_RESULT_ROWS:
        _pos = _metadata.read(_pos);
        _pos = cql::decode_int(_pos, _row_count);
        _column_count = _metadata.column_count();
        _row.resize(_column_count);
        break;

    case CQL_RESULT_SET_KEYSPACE:
        cql::decode_string(_pos, _keyspace_name);
        _row_pos = _row_count;
        break;

    case CQL_RESULT_PREPARED:
        cql::decode_short_bytes(_pos, _query_id);
        _row_pos = _row_count;
        break;

    case CQL_RESULT_VOID:
        _row_pos = _row_count;
        break;

    case CQL_RESULT_SCHEMA_CHANGE:
        break;
    }

    return true;
}

bool
cql::cql_message_result_impl_t::prepare(cql::cql_error_t*) {
    return true;
}

size_t
cql::cql_message_result_impl_t::row_count() const {
    return _row_count;
}

size_t
cql::cql_message_result_impl_t::column_count() const {
    return _column_count;
}

const std::vector<cql::cql_byte_t>&
cql::cql_message_result_impl_t::query_id() const {
    return _query_id;
}

bool
cql::cql_message_result_impl_t::exists(const std::string& column) const {
    return _metadata.exists(column);
}

bool
cql::cql_message_result_impl_t::column_name(int i,
                                            std::string& output_keyspace,
                                            std::string& output_table,
                                            std::string& output_column) const
{
    return _metadata.column_name(i,
                                 output_keyspace,
                                 output_table,
                                 output_column);
}

bool
cql::cql_message_result_impl_t::column_class(int i,
        std::string& output) const {
    return _metadata.column_class(i, output);
}

bool
cql::cql_message_result_impl_t::column_class(const std::string& column,
        std::string& output) const {
    return _metadata.column_class(column, output);
}

bool
cql::cql_message_result_impl_t::column_type(int i,
        cql_column_type_enum& output) const {
    return _metadata.column_type(i, output);
}

bool
cql::cql_message_result_impl_t::column_type(const std::string& column,
        cql_column_type_enum& output) const {
    return _metadata.column_type(column, output);
}

bool
cql::cql_message_result_impl_t::get_index(const std::string& column,
        int& output) const {
    return _metadata.get_index(column, output);
}

bool
cql::cql_message_result_impl_t::next() {
    if (_row_pos >= (unsigned) _row_count) {
        return false;
    }
    _row.clear();

    for (int i = 0; i < _column_count; ++i) {
        _row.push_back(_pos);
        cql::cql_int_t len = 0;
        _pos = cql::decode_int(_pos, len);
        if (len > 0) {
            _pos += len;
        }
    }

    _row_pos++;
    return true;
}

bool
cql::cql_message_result_impl_t::is_null(
    int   i,
    bool& output) const {
    if (i >= _column_count || i < 0) {
        return true;
    }
    cql::cql_int_t row_size = 0;
    cql::decode_int(_row[i], row_size);
    output = (row_size <= 0);
    return false;
}

bool
cql::cql_message_result_impl_t::is_null(
    const std::string& column,
    bool&              output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        cql::cql_int_t row_size = 0;
        cql::decode_int(_row[i], row_size);
        output = (row_size <= 0);
        return false;
    }
    return true;
}

bool
cql::cql_message_result_impl_t::get_bool(int i,
        bool& output) const {
    if (is_valid(i, cql::CQL_COLUMN_TYPE_BOOLEAN)) {
        output = *(_row[i] + sizeof(cql_int_t)) != 0x00;
        return true;
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_bool(const std::string& column,
        bool& output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_bool(i, output);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_int(int i,
                                        cql::cql_int_t& output) const {
    if (is_valid(i, cql::CQL_COLUMN_TYPE_INT)) {
        cql::decode_int(_row[i] + sizeof(cql_int_t), output);
        return true;
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_int(const std::string& column,
                                        cql::cql_int_t& output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_int(i, output);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_float(int i,
        float& output) const {
    if (is_valid(i, cql::CQL_COLUMN_TYPE_FLOAT)) {
        cql::decode_float(_row[i] + sizeof(cql_int_t), output);
        return true;
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_float(const std::string& column,
        float& output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_float(i, output);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_double(int i,
        double& output) const {
    if (is_valid(i, cql::CQL_COLUMN_TYPE_DOUBLE)) {
        cql_byte_t* p = _row[i] + sizeof(cql_int_t);
        cql::decode_double(p, output);
        return true;
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_double(const std::string& column,
        double& output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_double(i, output);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_bigint(int i,
        cql::cql_bigint_t& output) const {
    if (is_valid(i, cql::CQL_COLUMN_TYPE_BIGINT)
          || is_valid(i, cql::CQL_COLUMN_TYPE_TIMESTAMP)
          || is_valid(i, cql::CQL_COLUMN_TYPE_COUNTER)) {
        cql::decode_bigint(_row[i] + sizeof(cql_int_t), output);
        return true;
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_bigint(const std::string& column,
        cql::cql_bigint_t& output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_bigint(i, output);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_string(int i,
        std::string& output) const {
    cql_byte_t* data = 0;
    cql_int_t size = 0;
    if (get_data(i, &data, size)) {
        output.assign(data, data + size);
        return true;
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_string(const std::string& column,
        std::string& output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_string(i, output);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_data(int i,
        cql::cql_byte_t** output,
        cql::cql_int_t& size) const {
    bool empty = false;
    if (!is_null(i, empty)) {
        if (!empty) {
            cql_byte_t* pos = _row[i];
            *output = cql::decode_int(pos, size);
            return true;
        }
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_data(const std::string& column,
        cql::cql_byte_t** output,
        cql::cql_int_t& size) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_data(i, output, size);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_list(int i,
        cql::cql_list_t** output) const {
    if (is_valid(i, cql::CQL_COLUMN_TYPE_LIST)) {
        cql::cql_column_type_enum member_type;
        std::string member_class;
        _metadata.collection_primary_type(i, member_type);
        _metadata.collection_primary_class(i, member_class);
        *output = new cql::cql_list_impl_t(_row[i] + sizeof(cql_int_t), member_type, member_class);
        return true;
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_list(const std::string& column,
        cql::cql_list_t** output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_list(i, output);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_set(int i,
                                        cql::cql_set_t** output) const {
    if (is_valid(i, cql::CQL_COLUMN_TYPE_SET)) {
        cql::cql_column_type_enum member_type;
        std::string member_class;
        _metadata.collection_primary_type(i, member_type);
        _metadata.collection_primary_class(i, member_class);
        *output = new cql::cql_set_impl_t(_row[i] + sizeof(cql_int_t), member_type, member_class);
        return true;
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_set(const std::string& column,
                                        cql::cql_set_t** output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_set(i, output);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_map(int i,
                                        cql::cql_map_t** output) const {
    if (is_valid(i, cql::CQL_COLUMN_TYPE_MAP)) {
        cql::cql_column_type_enum key_type;
        cql::cql_column_type_enum value_type;
        std::string key_class;
        std::string value_class;
        _metadata.collection_primary_type(i, key_type);
        _metadata.collection_primary_class(i, key_class);
        _metadata.collection_secondary_type(i, value_type);
        _metadata.collection_secondary_class(i, value_class);
        *output = new cql::cql_map_impl_t(_row[i] + sizeof(cql_int_t), key_type, value_type, key_class, value_class);
        return true;
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_map(const std::string& column,
                                        cql::cql_map_t** output) const {
    int i = 0;
    if (_metadata.get_index(column, i)) {
        return get_map(i, output);
    }
    return false;
}

bool
cql::cql_message_result_impl_t::get_keyspace_name(std::string& output) const {
    output = _keyspace_name;
    return true;
}
