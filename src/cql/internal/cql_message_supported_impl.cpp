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
#include <boost/algorithm/string/join.hpp>
#include "cql/internal/cql_vector_stream.hpp"
#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_defines.hpp"

#include "cql/internal/cql_message_supported_impl.hpp"

cql::cql_message_supported_impl_t::cql_message_supported_impl_t() :
    _buffer(new std::vector<cql_byte_t>())
{}

cql::cql_message_supported_impl_t::cql_message_supported_impl_t(size_t size) :
    _buffer(new std::vector<cql_byte_t>(size))
{}

cql::cql_message_buffer_t
cql::cql_message_supported_impl_t::buffer() {
    return _buffer;
}

void
cql::cql_message_supported_impl_t::compressions(const std::list<std::string>& c) {
    _compressions = c;
}

const std::list<std::string>&
cql::cql_message_supported_impl_t::compressions() const {
    return _compressions;
}

void
cql::cql_message_supported_impl_t::versions(const std::list<std::string>& v) {
    _versions = v;
}

const std::list<std::string>&
cql::cql_message_supported_impl_t::version() const {
    return _versions;
}

cql::cql_opcode_enum
cql::cql_message_supported_impl_t::opcode() const {
    return CQL_OPCODE_SUPPORTED;
}

cql::cql_int_t
cql::cql_message_supported_impl_t::size() const {
    return _buffer->size();
}

std::string
cql::cql_message_supported_impl_t::str() const {
    std::stringstream output;
    output << "{versions: [" << boost::algorithm::join(_versions, ", ");
    output << "], compressions: [" << boost::algorithm::join(_compressions, ", ") << "]}";
    return output.str();
}

bool
cql::cql_message_supported_impl_t::consume(cql::cql_error_t*) {
    cql::vector_stream_t buffer(*_buffer);
    std::istream stream(&buffer);

    std::map<std::string, std::list<std::string> > supported;
    cql::decode_string_multimap(stream, supported);

    if (supported.find(CQL_VERSION) != supported.end())
        _versions = supported[CQL_VERSION];

    if (supported.find(CQL_COMPRESSION) != supported.end())
        _compressions = supported[CQL_COMPRESSION];

    return true;
}

bool
cql::cql_message_supported_impl_t::prepare(cql::cql_error_t*) {
    cql::vector_stream_t buffer(*_buffer);
    std::ostream stream(&buffer);

    std::map<std::string, std::list<std::string> > supported;

    if (!_versions.empty())
        supported.insert(std::pair<std::string, std::list<std::string> >(CQL_VERSION, _versions));

    if (!_compressions.empty())
        supported.insert(std::pair<std::string, std::list<std::string> >(CQL_COMPRESSION, _compressions));

    cql::encode_string_multimap(stream, supported);
    return true;
}
