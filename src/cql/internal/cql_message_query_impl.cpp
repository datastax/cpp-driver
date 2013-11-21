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
#include <sstream>
#ifdef _WIN32
#include <Winsock2.h>
#else
#include <arpa/inet.h>
#endif
#include "cql/internal/cql_vector_stream.hpp"
#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_util.hpp"
#include "cql/internal/cql_defines.hpp"

#include "cql/internal/cql_message_query_impl.hpp"

cql::cql_message_query_impl_t::cql_message_query_impl_t() :
    _buffer(new std::vector<cql_byte_t>()),
    _consistency(CQL_CONSISTENCY_ANY),
    _query()
{}

cql::cql_message_query_impl_t::cql_message_query_impl_t(size_t size) :
    _buffer(new std::vector<cql_byte_t>(size)),
    _consistency(CQL_CONSISTENCY_ANY),
    _query()
{}

cql::cql_message_query_impl_t::cql_message_query_impl_t(const boost::shared_ptr<cql_query_t>& query) 
	: _buffer(new std::vector<cql_byte_t>())
	, _consistency(query->consistency())
	, _query(query->query())
{}

cql::cql_message_buffer_t
cql::cql_message_query_impl_t::buffer() {
    return _buffer;
}

const std::string&
cql::cql_message_query_impl_t::query() const {
    return _query;
}

cql::cql_consistency_enum
cql::cql_message_query_impl_t::consistency() const {
    return _consistency;
}

void
cql::cql_message_query_impl_t::set_query(const std::string& q) {
    _query = q;
}

void
cql::cql_message_query_impl_t::set_consistency(cql::cql_consistency_enum consistency) {
    _consistency = consistency;
}

cql::cql_opcode_enum
cql::cql_message_query_impl_t::opcode() const {
    return CQL_OPCODE_QUERY;
}

cql::cql_int_t
cql::cql_message_query_impl_t::size() const {
    return _buffer->size();
}

std::string
cql::cql_message_query_impl_t::str() const {
    return _query + " " + cql::to_string(_consistency);
}

bool
cql::cql_message_query_impl_t::consume(cql::cql_error_t*) {
    cql::vector_stream_t buffer(*_buffer);
    std::istream stream(&buffer);
    cql::decode_long_string(stream, _query);

	cql::cql_short_t consistency_bytes;
    stream.read(reinterpret_cast<char*>(&consistency_bytes), sizeof(consistency_bytes));
    _consistency = (cql::cql_consistency_enum) ntohs(consistency_bytes);

    return true;
}

bool
cql::cql_message_query_impl_t::prepare(cql::cql_error_t*) {
    _buffer->resize(_query.size() + sizeof(cql::cql_int_t) + sizeof(cql::cql_short_t));

    cql::vector_stream_t buffer(*_buffer);
    std::ostream stream(&buffer);
    cql::encode_long_string(stream, _query);
    cql::encode_short(stream, static_cast<cql::cql_short_t>(_consistency));
    return true;
}
