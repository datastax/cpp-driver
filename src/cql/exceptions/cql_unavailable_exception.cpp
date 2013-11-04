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
#include <string>
#include <boost/format.hpp>

#include "cql/exceptions/cql_unavailable_exception.hpp"
#include "cql/internal/cql_util.hpp"

std::string
cql::cql_unavailable_exception::create_message(
    cql_consistency_enum consistency_level, 
    cql_int_t required, 
    cql_int_t alive) 
{
    return boost::str(boost::format(
        "Not enough replica available for query at consistency %1% (%2% required but only %3% alive)")
        % to_string(consistency_level)
        % required
        % alive);
}
