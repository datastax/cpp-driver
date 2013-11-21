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
#include "cql/internal/cql_vector_stream.hpp"
#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_defines.hpp"

#include "cql/internal/cql_message_prepare_impl.hpp"

cql::cql_message_prepare_impl_t::cql_message_prepare_impl_t() :
    _buffer(new std::vector<cql_byte_t>())
{}

cql::cql_message_prepare_impl_t::cql_message_prepare_impl_t(size_t size) :
    _buffer(new std::vector<cql_byte_t>(size))
{}

cql::cql_message_prepare_impl_t::cql_message_prepare_impl_t(
        const boost::shared_ptr<cql_query_t>& query)
    : _buffer(new std::vector<cql_byte_t>())
    , _query(query->query())  { }

cql::cql_message_buffer_t
cql::cql_message_prepare_impl_t::buffer() {
    return _buffer;
}

const std::string&
cql::cql_message_prepare_impl_t::query() const {
    return _query;
}

void
cql::cql_message_prepare_impl_t::set_query(const std::string& q) {
    _query = q;
}

cql::cql_opcode_enum
cql::cql_message_prepare_impl_t::opcode() const {
    return CQL_OPCODE_PREPARE;
}

cql::cql_int_t
cql::cql_message_prepare_impl_t::size() const {
    return _buffer->size();
}

std::string
cql::cql_message_prepare_impl_t::str() const {
    return _query;
}

bool
cql::cql_message_prepare_impl_t::consume(cql::cql_error_t*) {
    cql::vector_stream_t buffer(*_buffer);
    std::istream stream(&buffer);
    cql::decode_long_string(stream, _query);
    return true;
}

bool
cql::cql_message_prepare_impl_t::prepare(cql::cql_error_t*) {
    _buffer->resize(_query.size() + sizeof(cql::cql_int_t));
    cql::vector_stream_t buffer(*_buffer);
    std::ostream stream(&buffer);
    cql::encode_long_string(stream, _query);
    return true;
}
