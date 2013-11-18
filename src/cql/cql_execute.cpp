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
#include "cql/internal/cql_message_execute_impl.hpp"

#include "cql/cql_execute.hpp"

cql::cql_execute_t::cql_execute_t() :
    _impl(new cql_message_execute_impl_t())
{}

cql::cql_execute_t::cql_execute_t(const std::vector<cql::cql_byte_t>& id,
                                  cql::cql_consistency_enum consistency) :
    _impl(new cql_message_execute_impl_t(id, consistency))
{}

cql::cql_execute_t::~cql_execute_t() {
    delete _impl;
}

const std::vector<cql::cql_byte_t>&
cql::cql_execute_t::query_id() const {
    return _impl->query_id();
}

void
cql::cql_execute_t::query_id(const std::vector<cql::cql_byte_t>& id) {
    _impl->query_id(id);
}

cql::cql_consistency_enum
cql::cql_execute_t::consistency() const {
    return _impl->consistency();
}

void
cql::cql_execute_t::consistency(const cql::cql_consistency_enum consistency) {
    _impl->consistency(consistency);
}

void
cql::cql_execute_t::push_back(const std::vector<cql::cql_byte_t>& val) {
    _impl->push_back(val);
}

void
cql::cql_execute_t::push_back(const std::string& val) {
    _impl->push_back(val);
}

void
cql::cql_execute_t::push_back(const char* val) {
    _impl->push_back(std::string(val));
}

void
cql::cql_execute_t::push_back(const cql::cql_short_t val) {
    _impl->push_back(val);
}

void
cql::cql_execute_t::push_back(const cql_int_t val) {
    _impl->push_back(val);
}

void
cql::cql_execute_t::push_back(const cql::cql_bigint_t val) {
    _impl->push_back(val);
}

void
cql::cql_execute_t::push_back(const float val) {
    _impl->push_back(val);
}

void
cql::cql_execute_t::push_back(const double val) {
    _impl->push_back(val);
}

void
cql::cql_execute_t::push_back(const bool val) {
    _impl->push_back(val);
}

void
cql::cql_execute_t::pop_back() {
    _impl->pop_back();
}

cql::cql_message_execute_impl_t*
cql::cql_execute_t::impl() const {
    return _impl;
}

cql::cql_stream_t
cql::cql_execute_t::stream()
{
    return impl()->stream();
}

void
cql::cql_execute_t::set_stream(const cql_stream_t& stream)
{
    impl()->set_stream(stream);
}
