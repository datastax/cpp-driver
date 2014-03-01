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
#include <boost/foreach.hpp>

#include "cql/internal/cql_vector_stream.hpp"
#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_defines.hpp"

#include "cql/internal/cql_message_error_impl.hpp"

cql::cql_message_error_impl_t::cql_message_error_impl_t() :
    _buffer(new std::vector<cql::cql_byte_t>()),
    _code(0),
    _message()
{}

cql::cql_message_error_impl_t::cql_message_error_impl_t(size_t size) :
    _buffer(new std::vector<cql::cql_byte_t>(size)),
    _code(0),
    _message()
{}

cql::cql_message_error_impl_t::cql_message_error_impl_t(cql::cql_int_t code,
        const std::string& message) :
    _buffer(new std::vector<cql::cql_byte_t>(0)),
    _code(code),
    _message(message)
{}

const std::string&
cql::cql_message_error_impl_t::message() const {
    return _message;
}

void
cql::cql_message_error_impl_t::message(const std::string& m) {
    _message = m;
}

cql::cql_int_t
cql::cql_message_error_impl_t::code() const {
    return _code;
}

void
cql::cql_message_error_impl_t::code(cql::cql_int_t c) {
    _code = c;
}

cql::cql_opcode_enum
cql::cql_message_error_impl_t::opcode() const {
    return CQL_OPCODE_ERROR;
}

cql::cql_int_t
cql::cql_message_error_impl_t::size() const {
    return _buffer->size();
}

std::string
cql::cql_message_error_impl_t::str() const {
    return _message;
}

bool
cql::cql_message_error_impl_t::consume(cql::cql_error_t*) {
    cql::vector_stream_t buffer(*_buffer);
    std::istream input(&buffer);

    cql::decode_int(input, _code);
    cql::decode_string(input, _message);
    
    switch (_code) {
        case CQL_ERROR_SERVER:
        case CQL_ERROR_PROTOCOL:
        case CQL_ERROR_BAD_CREDENTIALS:
        case CQL_ERROR_UNAVAILABLE:
        case CQL_ERROR_OVERLOADED:
        case CQL_ERROR_IS_BOOTSTRAPPING:
        case CQL_ERROR_TRUNCATE:
        case CQL_ERROR_WRITE_TIMEOUT:
        case CQL_ERROR_READ_TIMEOUT:
        case CQL_ERROR_SYNTAX:
        case CQL_ERROR_UNAUTHORIZED:
        case CQL_ERROR_INVALID:
        case CQL_ERROR_CONFIG:
        case CQL_ERROR_ALREADY_EXISTS:
        case CQL_ERROR_UNPREPARED:
        default: {
            assert(0 && "Cassandra responded with unknown error code.");
        }
    }
    
    return true;
}

bool
cql::cql_message_error_impl_t::prepare(cql::cql_error_t*) {
    _buffer->resize(sizeof(_code) + sizeof(cql::cql_short_t) + _message.size());

    cql::vector_stream_t buffer(*_buffer);
    std::ostream output(&buffer);

    cql::encode_int(output, _code);
    cql::encode_string(output, _message);
    return true;
}

cql::cql_message_buffer_t
cql::cql_message_error_impl_t::buffer() {
    return _buffer;
}

bool
cql::cql_message_error_impl_t::_read_server_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_protocol_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_bad_credentials_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_unavailable_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_overloaded_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_is_bootstrapping_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_truncate_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_write_timeout_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_read_timeout_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_syntax_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_unauthorized_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_invalid_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_config_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_already_exists_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}

bool
cql::cql_message_error_impl_t::_read_unprepared_error(cql_error_t* error, std::istream& input) const
{
    assert(0 && "Not implemented");
}
