#include <boost/preprocessor.hpp>
#include "cql/cql.hpp"

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
		CASE_STRING_FOR_ENUM(CQL_HOST_LOCAL);
		CASE_STRING_FOR_ENUM(CQL_HOST_IGNORE);
		CASE_STRING_FOR_ENUM(CQL_HOST_REMOTE);
	default:
		return "Unknown " BOOST_PP_STRINGIZE(cql_host_distance_enum) " value";
	}
}