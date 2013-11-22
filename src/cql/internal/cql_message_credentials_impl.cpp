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

#include <boost/version.hpp>
#include <boost/foreach.hpp>
#if BOOST_VERSION >= 104300
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#else
#include <algorithm>
#include <ext/functional>
#endif
#include <boost/algorithm/string/join.hpp>

#include "cql/internal/cql_vector_stream.hpp"
#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_defines.hpp"

#include "cql/internal/cql_message_credentials_impl.hpp"

cql::cql_message_credentials_impl_t::cql_message_credentials_impl_t() :
    _buffer(new std::vector<cql_byte_t>(0))
{}

cql::cql_message_credentials_impl_t::cql_message_credentials_impl_t(size_t size) :
    _buffer(new std::vector<cql_byte_t>(size))
{}

cql::cql_message_buffer_t
cql::cql_message_credentials_impl_t::buffer() {
    return _buffer;
}

void
cql::cql_message_credentials_impl_t::credentials(const std::map<std::string, std::string>& c) {
    _credentials = c;
}

const std::map<std::string, std::string>&
cql::cql_message_credentials_impl_t::credentials() const {
    return _credentials;
}

cql::cql_opcode_enum
cql::cql_message_credentials_impl_t::opcode() const {
    return CQL_OPCODE_CREDENTIALS;
}

cql::cql_int_t
cql::cql_message_credentials_impl_t::size() const {
    return _buffer->size();
}

std::string
cql::cql_message_credentials_impl_t::str() const {
    std::list<std::string> keys;
#if BOOST_VERSION >= 104300
    boost::copy(_credentials | boost::adaptors::map_keys, std::back_inserter(keys));
#else
    std::transform(_credentials.begin(), _credentials.end(), std::back_inserter(keys), __gnu_cxx::select1st<credentials_map_t::value_type>());
#endif

    std::stringstream output;
    output << "[" << boost::algorithm::join(keys, ", ") << "]";
    return output.str();
}

bool
cql::cql_message_credentials_impl_t::consume(cql::cql_error_t*) {
    cql::vector_stream_t buffer(*_buffer);
    std::istream stream(&buffer);

    cql::decode_string_map(stream, _credentials);
    return true;
}

bool
cql::cql_message_credentials_impl_t::prepare(cql::cql_error_t*) {
    size_t size = sizeof(cql_short_t);
    
    BOOST_FOREACH(const credentials_map_t::value_type& pair, _credentials) {
        size += sizeof(cql_short_t);
        size += pair.first.size();
        
        size += sizeof(cql_short_t);
        size += pair.second.size();
    }
    _buffer->resize(size);

    cql::vector_stream_t buffer(*_buffer);
    std::ostream stream(&buffer);

    cql::encode_string_map(stream, _credentials);
    return true;
}
