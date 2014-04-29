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
#include <boost/preprocessor.hpp>
#include <boost/shared_ptr.hpp>

#include "cql/cql.hpp"
#include "cql/exceptions/cql_exception.hpp"

#define CASE_STRING_FOR_ENUM(enum_const) \
	case enum_const: return #enum_const

const char*
cql::to_string(const cql_consistency_enum consistency) {
	switch(consistency) {
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_ANY);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_ONE);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_TWO);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_THREE);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_QUORUM);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_ALL);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_LOCAL_QUORUM);
		CASE_STRING_FOR_ENUM(CQL_CONSISTENCY_EACH_QUORUM);
	default:
		return "Unknown " BOOST_PP_STRINGIZE(cql_consistency_enum) " value";
	}
}

const char*
cql::to_string(const cql_host_distance_enum host_distance) {
	switch(host_distance) {
		CASE_STRING_FOR_ENUM(CQL_HOST_DISTANCE_LOCAL);
		CASE_STRING_FOR_ENUM(CQL_HOST_DISTANCE_IGNORE);
		CASE_STRING_FOR_ENUM(CQL_HOST_DISTANCE_REMOTE);
	default:
		return "Unknown " BOOST_PP_STRINGIZE(cql_host_distance_enum) " value";
	}
}

const char*
cql::to_string(const cql_compression_enum compression) {
    switch(compression) {
        case CQL_COMPRESSION_SNAPPY:
            return "snappy";
        default:
            throw cql_exception("Requested string representation of an unknown compression type");
    }
}

cql::cql_compression_enum
cql::compression_from_string(const char* compression_string)
{
    if (strcmp(compression_string, "snappy") == 0) {
        return CQL_COMPRESSION_SNAPPY;
    }
    else if (strcmp(compression_string, "") == 0) {
        return CQL_COMPRESSION_NONE;
    }
    else return CQL_COMPRESSION_UNKNOWN;
}

namespace {
    bool cql_library_initialized = false;
}

void
cql::cql_initialize() {
    if(cql_library_initialized)
        return;

    cql_library_initialized = true;
}

void
cql::cql_terminate() {
    if(!cql_library_initialized)
        return;

    cql_library_initialized = false;
}
