#define BOOST_TEST_DYN_LINK

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"
#include "policy_tools.hpp"

#include "cql/cql_session.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include "cql/policies/cql_round_robin_policy.hpp"
														
struct CONSISTENCY_UUID_CCM_SETUP : test_utils::CCM_SETUP {
    CONSISTENCY_UUID_CCM_SETUP() : CCM_SETUP(3,0) {}
};			
						

int	count_number_of_valid_bits_in_timestamp( cql::cql_bigint_t ts )
{
	int result( 0 );
			
	for( int i = 1; i < 65; ++i )
	{
		cql::cql_bigint_t t = ts & 0x01;		// take the least significant bit.

		if( t > 0x00 )
			result = i;		

		ts = ts >> 1;
	}	
		
	return result;		
}

cql::cql_uuid_t
convert_timestamp_to_uuid(cql::cql_bigint_t ts)
{			
    std::vector<cql::cql_byte_t> v_bytes(16);
		
	for( int i = 0; i < 16; ++i )		
		v_bytes[ i ] = rand() % 256;	
		
	std::vector< cql::cql_byte_t > chars_vec;
			
	for( int i = 0; i < 8; ++i )
	{	
		cql::cql_bigint_t t = ts & static_cast< cql::cql_bigint_t >( 0xFF );		
		cql::cql_byte_t t2 = static_cast< cql::cql_byte_t >( t );	
		chars_vec.push_back( t2 );					
		ts = ts >> 8;			
	}		
			
	v_bytes[ 3 ] = chars_vec[ 0 ] ;
	v_bytes[ 2 ] = chars_vec[ 1 ] ;
	v_bytes[ 1 ] = chars_vec[ 2 ] ;
	v_bytes[ 0 ] = chars_vec[ 3 ] ;
	v_bytes[ 5 ] = chars_vec[ 4 ] ;
	v_bytes[ 4 ] = chars_vec[ 5 ] ;
	v_bytes[ 7 ] = chars_vec[ 6 ] ;							
	v_bytes[ 6 ] = chars_vec[ 7 ] & 0x0F;		//// take only half of the byte.
	cql::cql_byte_t t6 = 0xF0 & static_cast< cql::cql_byte_t >( rand() % 256 );
	v_bytes[ 6 ] = v_bytes[ 6 ] | t6;
	
    return cql::cql_uuid_t(&v_bytes[0]);
}

cql::cql_bigint_t generate_random_time_stamp()
{													
	int const max_rand( 3600 );

	cql::cql_bigint_t result( rand() % max_rand );

	cql::cql_bigint_t t100 = max_rand;
								
	for( int i = 0; i < 4; ++i )			
	{		
		result = result * t100 + static_cast< cql::cql_bigint_t >( rand() % max_rand );	
	}	
		
	return result;
}

BOOST_FIXTURE_TEST_SUITE( consistency_uuid_tests, CONSISTENCY_UUID_CCM_SETUP )				//// --run_test=consistency_uuid_tests/consistency_uuid_test_1
													

/////  --run_test=consistency_uuid_tests/consistency_uuid_test_1
BOOST_AUTO_TEST_CASE(consistency_uuid_test_1)
{				
	srand( (unsigned)time(NULL) );	
				
	builder->with_load_balancing_policy( boost::shared_ptr< cql::cql_load_balancing_policy_t >( new cql::cql_round_robin_policy_t() ) );
	boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
														
	test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
	session->set_keyspace(test_utils::SIMPLE_KEYSPACE);				
	test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, t1 int, t2 int, t3 uuid, t4 timestamp, t5 uuid );") % test_utils::SIMPLE_TABLE ));
			
	std::map <int, std::string>        uuid_map;
	std::map <int, cql::cql_uuid_t>    uuid_map_2;
	std::map <int, cql::cql_bigint_t>  time_stamp_map;
	std::map <int, cql::cql_bigint_t>  timeuuid_map;
				
	int const number_of_records_in_the_table = 2900;
								
	for (int i = 0; i < number_of_records_in_the_table; ++i)
	{
        cql::cql_uuid_t uuid = cql::cql_uuid_t::create();
		std::string const uuid_string = uuid.to_string();
        cql::cql_uuid_t uuid2(uuid_string);
						
		cql::cql_bigint_t timestamp = generate_random_time_stamp();
		cql::cql_uuid_t timeuuid = convert_timestamp_to_uuid(timestamp);
		std::string const timeuuid_string = timeuuid.to_string();
								
		if (!(uuid == uuid2)) {
			BOOST_FAIL( "Wrong uuid converted to string." );
		}		
		cql::cql_bigint_t const ts = generate_random_time_stamp();
					
		uuid_map.insert( std::make_pair( i, uuid_string ) );		
		uuid_map_2.insert( std::make_pair( i, uuid) );
		time_stamp_map.insert( std::make_pair( i, ts ) );
		timeuuid_map.insert( std::make_pair( i, timestamp ) );
								
		std::string query_string( boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3,t4,t5) VALUES (%d,%d,%d,%s,%d,%d);") % test_utils::SIMPLE_TABLE % i % i % i % uuid_string % ts % timeuuid_string));	
		boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,cql::CQL_CONSISTENCY_ANY));	
		session->query(_query);
	}				
				
	boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT tweet_id,t1,t2,t3,t4,t5 FROM %s LIMIT %d;")%test_utils::SIMPLE_TABLE%(number_of_records_in_the_table + 100)));			
				
	int rec_count( 0 );
			
	while (result->next())
	{			
		cql::cql_int_t cnt1, cnt2, cnt3;
		result->get_int(0,cnt1);
		result->get_int(1,cnt2);
		result->get_int(2,cnt3);
				
		BOOST_REQUIRE( cnt1 == cnt2 );
		BOOST_REQUIRE( cnt1 == cnt3 );
					
		cql::cql_bigint_t timeuuid_1( 0 );			
		result->get_timeuuid( 5, timeuuid_1 );		

		{
			std::map< int, cql::cql_bigint_t >::const_iterator p = timeuuid_map.find( cnt1 );	

			if( p == timeuuid_map.end() )
			{
				BOOST_FAIL("No such key in map of timeuuid.");
			}

			if( p->second != timeuuid_1 )
			{
				BOOST_FAIL("Wrong value of timeuuid.");
			}		
		}			
									
		cql::cql_bigint_t time_stamp_1( 0 );	
		result->get_timestamp( 4, time_stamp_1 );	
								
		{
			std::map< int, cql::cql_bigint_t >::const_iterator pts = time_stamp_map.find( cnt1 );

			if( pts == time_stamp_map.end() )
			{
				BOOST_FAIL("No such key in map of timestamp.");
			}

			if( pts->second != time_stamp_1 )
			{
				BOOST_FAIL("Wrong timestamp");
			}	
		}
					
		std::string uuid_string;
        cql::cql_uuid_t uuid_vec;
			
		{	
			result->get_uuid(3, uuid_string);
			
			std::map< int, std::string >::const_iterator p = uuid_map.find( cnt1 );
			if( p == uuid_map.end() )
			{
				BOOST_FAIL("No such key in map.");
			}
			
			if( uuid_string != p->second )
			{
				BOOST_FAIL("Wrong uuid converted to string.");
			}		
		}		
						
		{		
			result->get_uuid(3, uuid_vec);
				
			std::map< int, cql::cql_uuid_t>::const_iterator p = uuid_map_2.find( cnt1 );
			if( p == uuid_map_2.end() )	
			{		
				BOOST_FAIL("No such key in map.");
			}		
					
			if(!(uuid_vec == p->second))
			{
				BOOST_FAIL("Wrong uuid converted to string.");
			}		
		}		
				
		std::string uuid_string_bis;						
		cql::cql_uuid_t uuid_bis(uuid_string);
		uuid_string_bis = uuid_vec.to_string();
				
		if (uuid_string != uuid_string_bis) {
			BOOST_FAIL("Wrong conversion of uuid.");
		}		
				
		if (!(uuid_vec == uuid_bis)) {
			BOOST_FAIL("Wrong conversion of uuid.");
		}		
								
		++rec_count;
	}			
					
	BOOST_REQUIRE( rec_count == number_of_records_in_the_table );		
}				
				
BOOST_AUTO_TEST_SUITE_END()	
			
	