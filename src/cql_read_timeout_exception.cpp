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
#include "cql/exceptions/cql_read_timeout_exception.hpp"

std::string
cql::cql_read_timeout_exception::create_message(
        cql::cql_consistency_enum consistency_level, 
        cql::cql_int_t received, 
        cql::cql_int_t required,
        bool data_present)
{
    return boost::str(
        boost::format("Cassandra timeout during read query at consistency %1% (%2%)")
            % to_string(consistency_level)
            % get_message_details(received, required, data_present)
            );
}

std::string
cql::cql_read_timeout_exception::get_message_details(
    cql::cql_int_t received, 
    cql::cql_int_t required,
    bool data_present)
{
    if (received < required)
        return boost::str(boost::format("%1% replica responded over %2% required") % received % required);
    else if (!data_present)
        return "the replica queried for data didn't responded";
    else
        return "timeout while waiting for repair of inconsistent replica";

}
    
