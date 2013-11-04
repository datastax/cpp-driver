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

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_write_timeout_exception.hpp"

std::string
cql::cql_write_timeout_exception::create_message(
    cql::cql_consistency_enum consistency_level, 
    cql::cql_int_t received, 
    cql::cql_int_t required)
{
    return boost::str(boost::format(
        "Cassandra timeout during write query at consistency %1% "
        "(%2% replica acknowledged the write over %3% required)")
            % to_string(consistency_level)
            % received
            % required);
}
