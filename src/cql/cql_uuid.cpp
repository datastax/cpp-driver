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
#include <boost/atomic.hpp>
#include "cql/cql_uuid.hpp"

cql::cql_uuid_t
cql::cql_uuid_t::create() {
    static boost::atomic_ulong id; 
    return cql::cql_uuid_t(id++);
}    

namespace cql {
bool
operator <(const cql::cql_uuid_t& left, const cql::cql_uuid_t& right) {
    return (left._uuid < right._uuid);
}
}
