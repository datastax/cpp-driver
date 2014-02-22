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
#include "cql_keyspace_already_exists_exception.hpp"
#include "cql_exception.hpp"

using namespace boost;

cql::cql_keyspace_already_exists_exception::cql_keyspace_already_exists_exception(const std::string& keyspace) 
    : cql_query_validation_exception(create_message(keyspace))
{ }

std::string
cql::cql_keyspace_already_exists_exception::create_message(const std::string& keyspace) {
    return str(format("Keyspace '%1%' already exists.") % keyspace);
}
