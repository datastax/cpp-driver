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
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_message_options_impl.hpp"

cql::cql_message_options_impl_t::cql_message_options_impl_t() :
    _buffer(new std::vector<cql::cql_byte_t>(0))
{}

cql::cql_message_options_impl_t::cql_message_options_impl_t(size_t size) :
    _buffer(new std::vector<cql::cql_byte_t>(size, 0))
{}

cql::cql_message_buffer_t
cql::cql_message_options_impl_t::buffer() {
    return _buffer;
}

cql::cql_int_t
cql::cql_message_options_impl_t::size() const {
    return _buffer->size();
}

cql::cql_opcode_enum
cql::cql_message_options_impl_t::opcode() const {
    return CQL_OPCODE_OPTIONS;
}

std::string
cql::cql_message_options_impl_t::str() const {
    return "OPTIONS";
}

bool
cql::cql_message_options_impl_t::consume(cql::cql_error_t*) {
    return true;
}

bool
cql::cql_message_options_impl_t::prepare(cql::cql_error_t*) {
    return true;
}
