#include "cql/cql.hpp"

#include <cql/cql_session.hpp>
#include <boost/test/unit_test.hpp>
#include "test_utils.hpp"

// This function is called asynchronously every time an event is logged
void test_utils::log_callback(
    const cql::cql_short_t,
    const std::string& message)
	{
		std::cout << "LOG: " << message << std::endl;
	}

boost::shared_ptr<cql::cql_result_t> test_utils::query(
	boost::shared_ptr<cql::cql_session_t> session,
	std::string query_string,
	cql::cql_consistency_enum cl)
	{
		boost::shared_ptr<cql::cql_query_t> _query(
			new cql::cql_query_t(query_string,cl));
		boost::shared_future<cql::cql_future_result_t> query_future = session->query(_query);
		query_future.wait();
		cql::cql_future_result_t query_result = query_future.get();
		return query_result.result;	
	}

std::string test_utils::get_cql(cql::cql_column_type_enum col_type)
	{
		std::map<cql::cql_column_type_enum,std::string> CQL_TYPE;
		CQL_TYPE[cql::CQL_COLUMN_TYPE_CUSTOM] = "custom";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_ASCII] = "ascii";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_BIGINT] = "bigint";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_BLOB] = "blob";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_BOOLEAN] = "boolean";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_COUNTER] = "counter";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_DECIMAL] = "decimal";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_DOUBLE] = "double";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_FLOAT] = "float";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_INT] = "int";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_TEXT] = "text";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_TIMESTAMP] = "timestamp";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_UUID] = "uuid";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_VARCHAR] = "varchar";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_VARINT] = "varint";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_TIMEUUID] = "timeuuid";
		CQL_TYPE[cql::CQL_COLUMN_TYPE_INET] = "inet";

		return CQL_TYPE[col_type];
	}


  const std::string test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %s }";
  const std::string test_utils::CREATE_KEYSPACE_GENERIC_FORMAT = "CREATE KEYSPACE {0} WITH replication = { 'class' : '{1}', {2} }";
  const std::string test_utils::SIMPLE_KEYSPACE = "ks";
  const std::string test_utils::SIMPLE_TABLE = "test";
  const std::string test_utils::CREATE_TABLE_SIMPLE_FORMAT = "CREATE TABLE {0} (k text PRIMARY KEY, t text, i int, f float)";
  const std::string test_utils::INSERT_FORMAT = "INSERT INTO {0} (k, t, i, f) VALUES ('{1}', '{2}', {3}, {4})";
  const std::string test_utils::SELECT_ALL_FORMAT = "SELECT * FROM {0}";
  const std::string test_utils::SELECT_WHERE_FORMAT = "SELECT * FROM {0} WHERE {1}";
