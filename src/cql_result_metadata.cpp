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
#include <sstream>
#include <boost/version.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/unordered/unordered_map.hpp>
#if BOOST_VERSION >= 104300
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#else
#include <algorithm>
#include <ext/functional>
#endif
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_serialization.hpp"

#include "cql/internal/cql_result_metadata.hpp"

struct column_name_to_str {
    typedef std::string result_type;

    std::string
    operator()(const cql::cql_result_metadata_t::column_name_t& n) const {
        return std::string("[") + n.get<0>() + ", " + n.get<1>() + ", " + n.get<2>() + "]";
    }
};


cql::cql_result_metadata_t::cql_result_metadata_t() :
    _flags(0),
    _column_count(0)
{}

std::string
cql::cql_result_metadata_t::str() const {
    std::list<std::string> columns;
#if BOOST_VERSION >= 104300
    boost::copy(
        _column_name_idx | boost::adaptors::map_keys | boost::adaptors::transformed(column_name_to_str()),
        std::back_inserter(columns));
#else
    std::transform(_column_name_idx.begin(), _column_name_idx.end(), std::back_inserter(columns),
                   __gnu_cxx::compose1(column_name_to_str(), __gnu_cxx::select1st<column_name_idx_t::value_type>()));
#endif
    std::stringstream output;
    output << "[" << boost::algorithm::join(columns, ", ") << "]";
    return output.str();
}

cql::cql_byte_t*
cql::cql_result_metadata_t::read(cql::cql_byte_t* input) {
    input = cql::decode_int(input, _flags);
    input = cql::decode_int(input, _column_count);

    if (_flags & CQL_RESULT_ROWS_FLAGS_GLOBAL_TABLES_SPEC) {
        input = cql::decode_string(input, _global_keyspace_name);
        input = cql::decode_string(input, _global_table_name);
    }

    for (int i = 0; i < _column_count; ++i) {
        std::string keyspace_name;
        std::string table_name;

        if (!(_flags & CQL_RESULT_ROWS_FLAGS_GLOBAL_TABLES_SPEC)) {
            input = cql::decode_string(input, keyspace_name);
            input = cql::decode_string(input, table_name);
        } else {
            keyspace_name = _global_keyspace_name;
            table_name = _global_table_name;
        }
        std::string column_name;
        input = cql::decode_string(input, column_name);

        option_t option;
        input = cql::decode_option(input, option.primary_type, option.primary_class);

        if (option.primary_type == cql::CQL_COLUMN_TYPE_SET || option.primary_type == cql::CQL_COLUMN_TYPE_LIST) {
            // it's a native set or list. Read the option which tells us the collecion member type
            input = cql::decode_option(input, option.collection_primary_type, option.collection_primary_class);
        }

        if (option.primary_type == cql::CQL_COLUMN_TYPE_MAP) {
            // it's a native map. Read the options which tells us the collecion key and values types
            input = cql::decode_option(input, option.collection_primary_type, option.collection_primary_class);
            input = cql::decode_option(input, option.collection_secondary_type, option.collection_secondary_class);
        }

        option.name = column_name_t(keyspace_name, table_name, column_name);
        _column_name_idx.insert(column_name_idx_t::value_type(option.name, i));
        _columns.push_back(option);
    }
    return input;
}

cql::cql_int_t
cql::cql_result_metadata_t::flags() const {
    return _flags;
}

void
cql::cql_result_metadata_t::flags(cql::cql_int_t v) {
    _flags = v;
}

cql::cql_int_t
cql::cql_result_metadata_t::column_count() const {
    return _column_count;
}

void
cql::cql_result_metadata_t::column_count(cql::cql_int_t v) {
    _column_count = v;
}

bool
cql::cql_result_metadata_t::has_global_keyspace() const {
    return _flags & CQL_RESULT_ROWS_FLAGS_GLOBAL_TABLES_SPEC;
}

bool
cql::cql_result_metadata_t::has_global_table() const {
    return _flags & CQL_RESULT_ROWS_FLAGS_GLOBAL_TABLES_SPEC;
}

const std::string&
cql::cql_result_metadata_t::global_keyspace() const {
    return _global_keyspace_name;
}

void
cql::cql_result_metadata_t::global_keyspace(const std::string& keyspace) {
    _global_keyspace_name = keyspace;
}

const std::string&
cql::cql_result_metadata_t::global_table() const {
    return _global_table_name;
}

void
cql::cql_result_metadata_t::global_table(const std::string& table) {
    _global_table_name = table;
}

bool
cql::cql_result_metadata_t::column_name(int i,
                                        std::string& output_keyspace,
                                        std::string& output_table,
                                        std::string& output_column) const
{
    if (i >= _column_count || i < 0) {
        return false;
    }

    output_keyspace = _columns[i].name.get<0>();
    output_table = _columns[i].name.get<1>();
    output_column = _columns[i].name.get<2>();
    return true;
}

bool
cql::cql_result_metadata_t::column_class(int i,
        std::string& output) const {
    if (i >= _column_count || i < 0) {
        return false;
    }

    output = _columns[i].primary_class;
    return true;
}

bool
cql::cql_result_metadata_t::column_class(const std::string& column,
        std::string& output) const {
    if (_global_keyspace_name.empty() || _global_table_name.empty()) {
        return false;
    }

    return column_class(_global_keyspace_name, _global_table_name, column, output);
}

bool
cql::cql_result_metadata_t::column_class(const std::string& keyspace,
        const std::string& table,
        const std::string& column,
        std::string& output) const {
    column_name_idx_t::const_iterator it = _column_name_idx.find(column_name_t(keyspace, table, column));
    if(it != _column_name_idx.end()) {
        output = _columns[it->second].primary_class;
        return true;
    }

    return false;
}

bool
cql::cql_result_metadata_t::column_type(int i,
                                        cql::cql_column_type_enum& output) const {
    if (i >= _column_count || i < 0) {
        return false;
    }

    int val = _columns[i].primary_type;
    if (val >= 0 && val <= 0x0022) {
        output = static_cast<cql_column_type_enum>(val);
    } else {
        output = CQL_COLUMN_TYPE_UNKNOWN;
    }
    return true;
}

bool
cql::cql_result_metadata_t::column_type(const std::string& column,
                                        cql::cql_column_type_enum& output) const {
    if (_global_keyspace_name.empty() || _global_table_name.empty()) {
        return false;
    }

    return column_type(_global_keyspace_name, _global_table_name, column, output);
}

bool
cql::cql_result_metadata_t::column_type(const std::string& keyspace,
                                        const std::string& table,
                                        const std::string& column,
                                        cql::cql_column_type_enum& output) const {
    column_name_idx_t::const_iterator it = _column_name_idx.find(column_name_t(keyspace, table, column));
    if(it != _column_name_idx.end()) {
        column_type(it->second, output);
        return true;
    }
    return false;
}

bool
cql::cql_result_metadata_t::exists(const std::string& column) const {
    if (_global_keyspace_name.empty() || _global_table_name.empty()) {
        return false;
    }

    return _column_name_idx.find(
               column_name_t(_global_keyspace_name, _global_table_name, column))
           != _column_name_idx.end();
}

bool
cql::cql_result_metadata_t::exists(const std::string& keyspace,
                                   const std::string& table,
                                   const std::string& column) const {
    return _column_name_idx.find(
               column_name_t(keyspace, table, column))
           != _column_name_idx.end();
}

bool
cql::cql_result_metadata_t::get_index(const std::string& column,
                                      int& output) const {
    if (_global_keyspace_name.empty() || _global_table_name.empty()) {
        return false;
    }

    return get_index(_global_keyspace_name, _global_table_name, column, output);
}

bool
cql::cql_result_metadata_t::get_index(const std::string& keyspace,
                                      const std::string& table,
                                      const std::string& column,
                                      int& output) const {
    column_name_idx_t::const_iterator it = _column_name_idx.find(column_name_t(keyspace, table, column));
    if(it != _column_name_idx.end()) {
        output = it->second;
        return true;
    }
    return false;
}

bool
cql::cql_result_metadata_t::collection_primary_class(int i,
        std::string& output) const {
    if (i >= _column_count || i < 0) {
        return false;
    }

    output = _columns[i].collection_primary_class;
    return true;
}

bool
cql::cql_result_metadata_t::collection_primary_class(const std::string& column,
        std::string& output) const {
    if (_global_keyspace_name.empty() || _global_table_name.empty()) {
        return false;
    }

    return collection_primary_class(_global_keyspace_name, _global_table_name, column, output);
}

bool
cql::cql_result_metadata_t::collection_primary_class(const std::string& keyspace,
        const std::string& table,
        const std::string& column,
        std::string& output) const {
    column_name_idx_t::const_iterator it = _column_name_idx.find(column_name_t(keyspace, table, column));
    if(it != _column_name_idx.end()) {
        output = _columns[it->second].collection_primary_class;
        return true;
    }

    return false;
}

bool
cql::cql_result_metadata_t::collection_primary_type(int i,
        cql::cql_column_type_enum& output) const {
    if (i >= _column_count || i < 0) {
        return false;
    }

    output = _columns[i].collection_primary_type;
    return true;
}

bool
cql::cql_result_metadata_t::collection_primary_type(const std::string& column,
        cql::cql_column_type_enum& output) const {
    if (_global_keyspace_name.empty() || _global_table_name.empty()) {
        return false;
    }

    return collection_primary_type(_global_keyspace_name, _global_table_name, column, output);
}

bool
cql::cql_result_metadata_t::collection_primary_type(const std::string& keyspace,
        const std::string& table,
        const std::string& column,
        cql::cql_column_type_enum& output) const {
    column_name_idx_t::const_iterator it = _column_name_idx.find(column_name_t(keyspace, table, column));
    if(it != _column_name_idx.end()) {
        output = _columns[it->second].collection_primary_type;
        return true;
    }

    return false;
}

bool
cql::cql_result_metadata_t::collection_secondary_class(int i,
        std::string& output) const {
    if (i >= _column_count || i < 0) {
        return false;
    }

    output = _columns[i].collection_secondary_class;
    return true;
}

bool
cql::cql_result_metadata_t::collection_secondary_class(const std::string& column,
        std::string& output) const {
    if (_global_keyspace_name.empty() || _global_table_name.empty()) {
        return false;
    }

    return collection_secondary_class(_global_keyspace_name, _global_table_name, column, output);
}

bool
cql::cql_result_metadata_t::collection_secondary_class(const std::string& keyspace,
        const std::string& table,
        const std::string& column,
        std::string& output) const {
    column_name_idx_t::const_iterator it = _column_name_idx.find(column_name_t(keyspace, table, column));
    if(it != _column_name_idx.end()) {
        output = _columns[it->second].collection_secondary_class;
        return true;
    }

    return false;
}

bool
cql::cql_result_metadata_t::collection_secondary_type(int i,
        cql::cql_column_type_enum& output) const {
    if (i >= _column_count || i < 0) {
        return false;
    }

    output = _columns[i].collection_secondary_type;
    return true;
}

bool
cql::cql_result_metadata_t::collection_secondary_type(const std::string& column,
        cql::cql_column_type_enum& output) const {
    if (_global_keyspace_name.empty() || _global_table_name.empty()) {
        return false;
    }

    return collection_secondary_type(_global_keyspace_name, _global_table_name, column, output);
}

bool
cql::cql_result_metadata_t::collection_secondary_type(const std::string& keyspace,
        const std::string& table,
        const std::string& column,
        cql::cql_column_type_enum& output) const {
    column_name_idx_t::const_iterator it = _column_name_idx.find(column_name_t(keyspace, table, column));
    if(it != _column_name_idx.end()) {
        output = _columns[it->second].collection_secondary_type;
        return true;
    }

    return false;
}
