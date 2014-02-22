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
#include "cql_stream.hpp"
#include "cql.hpp"

const cql::cql_stream_t&
cql::cql_stream_t::invalid_stream() {
    static cql_stream_t invalid =
                    cql_stream_t::from_stream_id(INVALID_STREAM_ID);
    return invalid;
}

cql::cql_stream_t
cql::cql_stream_t::from_stream_id(const cql_stream_id_t& stream_id) {
    return cql_stream_t(stream_id);
}
