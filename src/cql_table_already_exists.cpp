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
#include <boost/format.hpp>

#include "cql_util.hpp"
#include "cql_table_already_exists.hpp"

cql::cql_table_already_exists_exception::
	cql_table_already_exists_exception(const std::string& table_name) 
    : cql::cql_query_validation_exception(create_message("", table_name))
{ }

cql::cql_table_already_exists_exception::
	cql_table_already_exists_exception(const std::string& keyspace, const std::string& table_name) 
    : cql::cql_query_validation_exception(create_message(keyspace, table_name))
{ }

std::string
cql::cql_table_already_exists_exception::create_message(
	const std::string& table_name, 
	const std::string& keyspace) 
{   
    const char* format_string = keyspace.empty() 
        ? "Table '%1%%2%' already exists."
        : "Table '%1%.%2%' already exists.";
    
    return boost::str(boost::format(format_string) % keyspace % table_name);
}
