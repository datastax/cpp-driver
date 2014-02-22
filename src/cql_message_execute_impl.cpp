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
#include <boost/foreach.hpp>
#include <iomanip>
#include <memory>
#include <sstream>
#include <vector>

#include "cql/internal/cql_vector_stream.hpp"
#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_util.hpp"

#include "cql/internal/cql_message_execute_impl.hpp"

cql::cql_message_execute_impl_t::cql_message_execute_impl_t() :
    _buffer(new std::vector<cql_byte_t>()),
    _consistency(cql::CQL_CONSISTENCY_ONE)
{}

cql::cql_message_execute_impl_t::cql_message_execute_impl_t(size_t size) :
    _buffer(new std::vector<cql_byte_t>(size)),
    _consistency(cql::CQL_CONSISTENCY_ONE)
{}

cql::cql_message_execute_impl_t::cql_message_execute_impl_t(const std::vector<cql::cql_byte_t>& id,
        cql::cql_consistency_enum consistency) :
    _buffer(new std::vector<cql_byte_t>()),
    _query_id(id),
    _consistency(consistency)
{}

cql::cql_message_buffer_t
cql::cql_message_execute_impl_t::buffer() {
    return _buffer;
}

const std::vector<cql::cql_byte_t>&
cql::cql_message_execute_impl_t::query_id() const {
    return _query_id;
}

void
cql::cql_message_execute_impl_t::query_id(const std::vector<cql::cql_byte_t>& id) {
    _query_id = id;
}

cql::cql_consistency_enum
cql::cql_message_execute_impl_t::consistency() const {
    return _consistency;
}

void
cql::cql_message_execute_impl_t::consistency(const cql::cql_consistency_enum consistency) {
    _consistency = consistency;
}

void
cql::cql_message_execute_impl_t::push_back(const param_t& val) {
    _params.push_back(param_t(val.begin(), val.end()));
}

void
cql::cql_message_execute_impl_t::push_back(const std::string& val) {
    _params.push_back(param_t(val.begin(), val.end()));
}

void
cql::cql_message_execute_impl_t::push_back(const cql::cql_short_t val) {
    cql::cql_message_execute_impl_t::param_t p;
    cql::encode_short(p, val);
    _params.push_back(p);
}

void
cql::cql_message_execute_impl_t::push_back(const cql::cql_int_t val) {
    cql::cql_message_execute_impl_t::param_t p;
    cql::encode_int(p, val);
    _params.push_back(p);
}

void
cql::cql_message_execute_impl_t::push_back(const cql::cql_bigint_t val) {
    cql::cql_message_execute_impl_t::param_t p;
    cql::encode_bigint(p, val);
    _params.push_back(p);
}

void
cql::cql_message_execute_impl_t::push_back(const float val) {
    cql::cql_message_execute_impl_t::param_t p;
    cql::encode_float(p, val);
    _params.push_back(p);
}

void
cql::cql_message_execute_impl_t::push_back(const double val) {
    cql::cql_message_execute_impl_t::param_t p;
    cql::encode_double(p, val);
    _params.push_back(p);
}

void
cql::cql_message_execute_impl_t::push_back(const bool val) {
    cql::cql_message_execute_impl_t::param_t p;
    cql::encode_bool(p, val);
    _params.push_back(p);
}

void
cql::cql_message_execute_impl_t::pop_back() {
    _params.pop_back();
}

cql::cql_opcode_enum
cql::cql_message_execute_impl_t::opcode() const {
    return CQL_OPCODE_EXECUTE;
}

cql::cql_int_t
cql::cql_message_execute_impl_t::size() const {
    return _buffer->size();
}

std::string
cql::cql_message_execute_impl_t::str() const {
    std::stringstream output;
    output << "EXECUTE";

    if (! _query_id.empty()) {
        output << " 0x";
        output << std::setfill('0');
        BOOST_FOREACH(cql::cql_byte_t c, _query_id) {
            output << std::setw(2) << cql::hex(c);
        }
    }
    output << " " << cql::to_string(_consistency);
    return output.str();
}

bool
cql::cql_message_execute_impl_t::consume(cql::cql_error_t*) {
    cql::vector_stream_t buffer(*_buffer);
    std::istream stream(&buffer);

    _params.clear();

    cql::cql_short_t count = 0;
    cql::decode_short_bytes(stream, _query_id);
    cql::decode_short(stream, count);

    for (int i = 0; i < count; ++i) {
        cql::cql_message_execute_impl_t::param_t p;
        cql::decode_bytes(stream, p);
        _params.push_back(p);
    }

    cql::cql_short_t consistency = 0;
    cql::decode_short(stream, consistency);

    switch (consistency) {

    case 0x0000:
        _consistency = cql::CQL_CONSISTENCY_ANY;
        break;

    case 0x0001:
        _consistency = cql::CQL_CONSISTENCY_ONE;
        break;

    case 0x0002:
        _consistency = cql::CQL_CONSISTENCY_TWO;
        break;

    case 0x0003:
        _consistency = cql::CQL_CONSISTENCY_THREE;
        break;

    case 0x0004:
        _consistency = cql::CQL_CONSISTENCY_QUORUM;
        break;

    case 0x0005:
        _consistency = cql::CQL_CONSISTENCY_ALL;
        break;

    case 0x0006:
        _consistency = cql::CQL_CONSISTENCY_LOCAL_QUORUM;
        break;

    case 0x0007:
        _consistency = cql::CQL_CONSISTENCY_EACH_QUORUM;
        break;
    }

    return true;
}

bool
cql::cql_message_execute_impl_t::prepare(cql::cql_error_t*) {
    size_t size = (3 * sizeof(cql_short_t)) + _query_id.size();
    BOOST_FOREACH(const param_t& p, _params) {
        size += p.size() + sizeof(cql_int_t);
    }
    _buffer->resize(size);

    cql::vector_stream_t buffer(*_buffer);
    std::ostream stream(&buffer);

    cql::encode_short_bytes(stream, _query_id);
    cql::encode_short(stream, _params.size());

    BOOST_FOREACH(const param_t& p, _params) {
        cql::encode_bytes(stream, p);
    }

    cql::cql_short_t consistency = 0;

    switch (_consistency) {

    case cql::CQL_CONSISTENCY_ANY:
        consistency = 0x0000;
        break;

    case cql::CQL_CONSISTENCY_ONE:
        consistency = 0x0001;
        break;

    case cql::CQL_CONSISTENCY_TWO:
        consistency = 0x0002;
        break;

    case cql::CQL_CONSISTENCY_THREE:
        consistency = 0x0003;
        break;

    case cql::CQL_CONSISTENCY_QUORUM:
        consistency = 0x0004;
        break;

    case cql::CQL_CONSISTENCY_ALL:
        consistency = 0x0005;
        break;

    case cql::CQL_CONSISTENCY_LOCAL_QUORUM:
        consistency = 0x0006;
        break;

    case cql::CQL_CONSISTENCY_EACH_QUORUM:
        consistency = 0x0007;
        break;
    }

    cql::encode_short(stream, consistency);
    return true;
}

cql::cql_stream_t
cql::cql_message_execute_impl_t::stream()
{
    return _stream;
}

void
cql::cql_message_execute_impl_t::set_stream(const cql_stream_t& stream)
{
    _stream = stream;
}

