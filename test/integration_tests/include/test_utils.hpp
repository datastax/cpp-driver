#pragma once
#include <cql\cql_session.hpp>
class test_utils
{
public:
	static void log_callback(const cql::cql_short_t, const std::string& message);
	static boost::shared_ptr<cql::cql_result_t> query(boost::shared_ptr<cql::cql_session_t>, std::string, cql::cql_consistency_enum cl = cql::CQL_CONSISTENCY_ONE);	
	static std::string get_cql(cql::cql_column_type_enum);

	static const std::string CREATE_KEYSPACE_SIMPLE_FORMAT;
	static const std::string CREATE_KEYSPACE_GENERIC_FORMAT;
	static const std::string SIMPLE_KEYSPACE;
	static const std::string SIMPLE_TABLE;
	static const std::string CREATE_TABLE_SIMPLE_FORMAT;
	static const std::string INSERT_FORMAT;
	static const std::string SELECT_ALL_FORMAT;
	static const std::string SELECT_WHERE_FORMAT;
};