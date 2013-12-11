#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>

#include <cql/cql_list.hpp>
#include <cql/cql_set.hpp>
#include <cql/cql_map.hpp>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>



struct CCM_SETUP {

    CCM_SETUP() :conf(cql::get_ccm_bridge_configuration())
	{
		boost::debug::detect_memory_leaks(true);
		int numberOfNodes = 1;
		ccm = cql::cql_ccm_bridge_t::create(conf, "test", numberOfNodes, true);
		ccm_contact_seed = boost::asio::ip::address::from_string(conf.ip_prefix() + "1");
		use_ssl=false;

		builder = cql::cql_cluster_t::builder();
		builder->with_log_callback(&test_utils::log_callback);		
	    builder->add_contact_point(ccm_contact_seed);
		
		if (use_ssl) {
			builder->with_ssl();
		}
	}

    ~CCM_SETUP()         
	{ 
		ccm->remove();
	}

	boost::shared_ptr<cql::cql_ccm_bridge_t> ccm;
	const cql::cql_ccm_bridge_configuration_t& conf;
	boost::asio::ip::address ccm_contact_seed;
	bool use_ssl;
	boost::shared_ptr<cql::cql_builder_t> builder;
};

BOOST_FIXTURE_TEST_SUITE( basics, CCM_SETUP )

boost::shared_ptr<cql::cql_result_t> simple_insert_test(boost::shared_ptr<cql::cql_cluster_t> cluster, cql::cql_column_type_enum col_type, std::string value_to_insert )
{
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
	if (!session) {
		BOOST_FAIL("Session creation failture.");
	}

	test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
	session->set_keyspace(test_utils::SIMPLE_KEYSPACE);

	test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, test_val %s);") % test_utils::SIMPLE_TABLE % test_utils::get_cql(col_type)));
	
	if(col_type == cql::CQL_COLUMN_TYPE_TEXT)
		value_to_insert = std::string("\'") + value_to_insert + "\'";   
	test_utils::query(session, str(boost::format("INSERT INTO %s(tweet_id, test_val) VALUES(0,%s);") % test_utils::SIMPLE_TABLE % value_to_insert));
	
	boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT * FROM %s WHERE tweet_id = 0;") % test_utils::SIMPLE_TABLE));	

	if(result->next())
	{
		session->close();
		return result;
	}
	else assert(false);	
}


//BOOST_AUTO_TEST_CASE(simple_insert_int32)
//{
//	int test_val = 2147483647;
//	std::string conv_val = boost::lexical_cast<std::string>(test_val);
//	boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_INT, conv_val);
//	int res;
//	result->get_int("test_val", res);
//	assert(test_val == res);
//}
//
//BOOST_AUTO_TEST_CASE(simple_insert_int64)
//{
//	INT64 test_val = 2147483648;
//	std::string conv_val = boost::lexical_cast<std::string>(test_val);
//	boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_BIGINT, conv_val);
//	INT64 res;
//	result->get_bigint("test_val", res);
//	assert(test_val == res);
//}
//
//BOOST_AUTO_TEST_CASE(simple_insert_boolean)
//{
//	bool test_val = false;
//	std::string conv_val = "false";
//	boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_BOOLEAN, conv_val);
//	bool res;
//	result->get_bool("test_val", res);
//	assert(test_val == res);
//}
//
//BOOST_AUTO_TEST_CASE(simple_insert_float)
//{
//	float test_val = 3.1415926f;
//	std::string conv_val = boost::lexical_cast<std::string>(test_val);
//	boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_FLOAT, conv_val);
//	float res;
//	result->get_float("test_val", res);
//	assert(test_val == res);
//}
//
//BOOST_AUTO_TEST_CASE(simple_insert_double)
//{
//	double test_val = 3.141592653589793;
//	std::string conv_val = boost::lexical_cast<std::string>(test_val);
//	boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_DOUBLE, conv_val);
//	double res;
//	result->get_double("test_val", res);
//	assert(test_val == res);
//}

BOOST_AUTO_TEST_CASE(simple_insert_string)
{
	std::string test_val = "Test Value.";
	boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_TEXT, test_val);
	std::string res;
	result->get_string("test_val", res);
	assert(test_val == res);
}


BOOST_AUTO_TEST_SUITE_END()
