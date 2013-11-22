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
#include <map>
#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include "cql/internal/cql_vector_stream.hpp"
#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_defines.hpp"

#include "cql/internal/cql_message_startup_impl.hpp"

cql::cql_message_startup_impl_t::cql_message_startup_impl_t() :
    _buffer(new std::vector<cql_byte_t>()),
    _version(),
    _compression()
{}

cql::cql_message_startup_impl_t::cql_message_startup_impl_t(size_t size) :
    _buffer(new std::vector<cql_byte_t>(size)),
    _version(),
    _compression()
{}

cql::cql_message_buffer_t
cql::cql_message_startup_impl_t::buffer() {
    return _buffer;
}

void
cql::cql_message_startup_impl_t::compression(const std::string& c) {
    _compression = c;
}

const std::string&
cql::cql_message_startup_impl_t::compression() const {
    return _compression;
}

void
cql::cql_message_startup_impl_t::version(const std::string& v) {
    _version = v;
}

const std::string&
cql::cql_message_startup_impl_t::version() const {
    return _version;
}

cql::cql_opcode_enum
cql::cql_message_startup_impl_t::opcode() const {
    return CQL_OPCODE_STARTUP;
}

cql::cql_int_t
cql::cql_message_startup_impl_t::size() const {
    return _buffer->size();
}

std::string
cql::cql_message_startup_impl_t::str() const {
    std::stringstream output;
    output << "{version: " << _version << ", compression: " << _compression << "}";
    return output.str();
}

bool
cql::cql_message_startup_impl_t::consume(cql::cql_error_t*) {
    cql::vector_stream_t buffer(*_buffer);
    std::istream stream(&buffer);

    std::map<std::string, std::string> startup;
    cql::decode_string_map(stream, startup);

    if (startup.find(CQL_VERSION) != startup.end()) {
        _version = startup[CQL_VERSION];
    }

    if (startup.find(CQL_COMPRESSION) != startup.end()) {
        _compression = startup[CQL_COMPRESSION];
    }

    return true;
}

bool
cql::cql_message_startup_impl_t::prepare(cql::cql_error_t*) {
    std::map<std::string, std::string> startup;
    size_t size = sizeof(cql::cql_short_t);

    if (!_version.empty()) {
        startup.insert(std::pair<std::string, std::string>(CQL_VERSION, _version));
        size += strlen(CQL_VERSION) + _version.size() + (2* sizeof(cql::cql_short_t));
    }

    if (!_compression.empty()) {
        startup.insert(std::pair<std::string, std::string>(CQL_COMPRESSION, _compression));
        size += strlen(CQL_COMPRESSION) + _compression.size() + (2* sizeof(cql::cql_short_t));
    }

    _buffer->resize(size);
    cql::vector_stream_t buffer(*_buffer);
    std::ostream stream(&buffer);
    cql::encode_string_map(stream, startup);
    return true;
}
