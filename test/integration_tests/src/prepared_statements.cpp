#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

#include "cql/cql_session.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"
#include "cql/cql_execute.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>

#include "cql/cql_decimal.hpp"
#include "cql/cql_varint.hpp"	

struct PREPARED_CCM_SETUP : test_utils::CCM_SETUP {
	PREPARED_CCM_SETUP() : CCM_SETUP(1,0) {}
};

BOOST_FIXTURE_TEST_SUITE( prepared_statements, PREPARED_CCM_SETUP )

template<class T>
boost::shared_ptr<cql::cql_result_t> 
prepared_insert_test(
	boost::shared_ptr<cql::cql_cluster_t> cluster,
	cql::cql_column_type_enum col_type,
	T value_to_insert )
{
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
	if (!session) {
		BOOST_FAIL("Session creation failure.");
	}

	test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
	session->set_keyspace(test_utils::SIMPLE_KEYSPACE);

	test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, test_val %s);") % test_utils::SIMPLE_TABLE % test_utils::get_cql(col_type)));

	test_utils::prepared_query(session,str(boost::format("INSERT INTO %s(tweet_id, test_val) VALUES(0,?);") % test_utils::SIMPLE_TABLE),value_to_insert);

	boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT * FROM %s WHERE tweet_id = 0;") % test_utils::SIMPLE_TABLE));	

	if(result->next())
	{
		session->close();
		return result;
	}
	else BOOST_FAIL("Empty result received.");
}

BOOST_AUTO_TEST_CASE(prepared_insert_bool)
{
	bool to_insert = true;
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_BOOLEAN, to_insert);
	bool res;
	result->get_bool("test_val", res);
	assert(res == to_insert);
}

BOOST_AUTO_TEST_CASE(prepared_insert_double)
{
	double to_insert = 3.141592653589793;
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_DOUBLE, to_insert);
	double res;
	result->get_double("test_val", res);
	assert(res == to_insert);
}

BOOST_AUTO_TEST_CASE(prepared_insert_float)
{
	float to_insert = 3.1415926f;
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_FLOAT, to_insert);
	float res;
	result->get_float("test_val", res);
	assert(res == to_insert);
}

BOOST_AUTO_TEST_CASE(prepared_insert_int64)
{
	boost::int64_t to_insert = 2147483648LL;
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_BIGINT, to_insert);
	boost::int64_t res;
	result->get_bigint("test_val", res);
	assert(res == to_insert);
}

BOOST_AUTO_TEST_CASE(prepared_insert_int32)
{
	int to_insert = 2147483647;
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_INT, to_insert);
	int res;
	result->get_int("test_val", res);
	assert(res == to_insert);
}

BOOST_AUTO_TEST_CASE(prepared_insert_string)
{
	std::string to_insert = "Prepared statement test value";
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_TEXT, to_insert);
	std::string res;
	result->get_string("test_val", res);
	assert(res == to_insert);
}

//    --run_test=prepared_statements/prepared_insert_uuid
BOOST_AUTO_TEST_CASE(prepared_insert_uuid)
{
	cql::cql_uuid_t to_insert("e1e8e8d0-ca0d-11e3-9c1a-0800200c9a66");	
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_UUID, to_insert);
	cql::cql_uuid_t res;
	result->get_uuid("test_val", res);
	assert(res == to_insert);
}

//    --run_test=prepared_statements/prepared_insert_inet_v4
BOOST_AUTO_TEST_CASE(prepared_insert_inet_v4)
{
	boost::asio::ip::address to_insert = boost::asio::ip::address::from_string( "199.123.97.13" );
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_INET, to_insert);
	boost::asio::ip::address res;
	result->get_inet("test_val", res);
	assert(res == to_insert);
}

//    --run_test=prepared_statements/prepared_insert_inet_v6
BOOST_AUTO_TEST_CASE(prepared_insert_inet_v6)
{
	boost::asio::ip::address to_insert = boost::asio::ip::address::from_string( "20a1:0db8:0b0c:0102:3004:f00d:1529:57fb" );
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_INET, to_insert);
	boost::asio::ip::address res;
	result->get_inet("test_val", res);
	assert(res == to_insert);
}

//    --run_test=prepared_statements/prepared_insert_decimal
BOOST_AUTO_TEST_CASE(prepared_insert_decimal)
{
	std::vector< unsigned char > t;
	t.push_back( 0 );
	t.push_back( 0 );
	t.push_back( 0 );
	t.push_back( 0 );
	t.push_back( 1 );
	t.push_back( 197 );
	t.push_back( 157 );

	cql::cql_decimal_t to_insert( t );
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_DECIMAL, to_insert);		
	cql::cql_decimal_t res;
	result->get_decimal("test_val", res);
	cql::cql_decimal_t res2 = res;
	assert(res.get_data() == to_insert.get_data());
}
	
//    --run_test=prepared_statements/prepared_insert_varint
BOOST_AUTO_TEST_CASE(prepared_insert_varint)
{
	std::vector< unsigned char > t;
	t.push_back( 0 );
	t.push_back( 0 );
	t.push_back( 0 );
	t.push_back( 0 );
	t.push_back( 1 );
	t.push_back( 177 );
	t.push_back( 137 );
		
	cql::cql_varint_t to_insert( t );	
	boost::shared_ptr<cql::cql_result_t> result = prepared_insert_test(builder->build(),cql::CQL_COLUMN_TYPE_VARINT, to_insert);
	cql::cql_varint_t res;
	result->get_varint("test_val", res);
	cql::cql_varint_t res2 = res;		
	assert(res.get_data() == to_insert.get_data());
}		

BOOST_AUTO_TEST_SUITE_END()
