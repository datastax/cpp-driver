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

struct CONSISTENCY_MY_TESTS_FOR_DIFFERENT_TYPES : test_utils::CCM_SETUP {
    CONSISTENCY_MY_TESTS_FOR_DIFFERENT_TYPES() : CCM_SETUP(4,0) {}
};								
												
BOOST_FIXTURE_TEST_SUITE( consistency_my_tests_types, CONSISTENCY_MY_TESTS_FOR_DIFFERENT_TYPES )				
						
std::string convert_vec_of_bytes_to_str( std::vector< cql::cql_byte_t > const & v )
{			
	std::string result;
			
	for( int i = 0; i < v.size(); ++i )
	{		
		cql::cql_byte_t b[2];
		b[0] = v[ i ] & 0xF0;
		b[1] = v[ i ] & 0x0F;
		b[0] = b[0] >> 4;
		
		for( int j = 0; j < 2; ++j )
		{
			result += ( b[j] < 10 ) ? ( '0' + b[j] ) : ( 'a' + b[j] - 10 );
		}	

		result += " ";	
	}	
		
	return result;	
}			
			
			
			
cql::cql_bigint_t generate_random_int_64()
{
	cql::cql_bigint_t result( 0 );
		
	for( int i = 0; i < 8; ++i )
	{	
		result = result << 8;	
		result = result | static_cast< cql::cql_int_t >( rand() % 256 );
	}	
		
	return result;	
}

	
cql::cql_int_t generate_random_int_32()
{		
	cql::cql_int_t result( 0 );
		
	for( int i = 0; i < 4; ++i )
	{	
		result = result << 8;	
		result = result | static_cast< cql::cql_int_t >( rand() % 256 );
	}	
		
	return result;	
}		


	
double generate_random_double()
{
	cql::cql_bigint_t t1 = generate_random_int_64();
	cql::cql_bigint_t t2 = generate_random_int_64();
		
	if( t2 < -1000 || t2 > 1000 )
	{		
		double r1 = static_cast< double >( t1 );
		double r2 = static_cast< double >( t2 );
		int rnd = rand() % 5;			
		int count = rand() % 5 + 1;		
								
		if( rnd == 0 )		
		{					
			for( int i = 0; i < count; ++i )
				r1 = r1 / ( ( static_cast< double >( rand() ) + 100.0 ) );	 
		}
		else if( rnd == 1 )
		{		
			for( int i = 0; i < count; ++i )
				r2 = r2 / ( ( static_cast< double >( rand() ) + 100.0 ) );	 
		}	
				
		double r3 = r1 / r2;
		return r3;	
	}			
	else		
		return static_cast< double >( t1 ) / static_cast< double >( 1000000.0 );	
}				
				
				
				
/////  --run_test=consistency_my_tests_types/consistency_my_tests_2
BOOST_AUTO_TEST_CASE(consistency_my_tests_2)
{					
	srand( (unsigned)time(NULL) );	
					
	builder->with_load_balancing_policy( boost::shared_ptr< cql::cql_load_balancing_policy_t >( new cql::cql_round_robin_policy_t() ) );
	boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
														
	test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
	session->set_keyspace(test_utils::SIMPLE_KEYSPACE);					
			
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			
	{	//// 1. Check the 32-int.	
		test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 decimal, t3 bigint );") % test_utils::SIMPLE_TABLE ));
					
		std::map< int, cql::cql_int_t > int_map;
					
		int const integer_number_of_rows = 400;
																
		for( int i = 0; i < integer_number_of_rows; ++i )
		{											
			cql::cql_int_t ii = generate_random_int_32();

			if( i < 10 )		//// check also the values from -5 to 5. 
				ii = i - 5;
				
			int_map.insert( std::make_pair( i, ii ) );
			std::string query_string( boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,%d,%d);") % test_utils::SIMPLE_TABLE % i % i % ii % i ) );	
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,cql::CQL_CONSISTENCY_ANY));	
			session->query(_query);										
		}		
			
		boost::this_thread::sleep(boost::posix_time::seconds(15));
				
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT t1, t2, t3 FROM %s LIMIT %d;") % test_utils::SIMPLE_TABLE % 900 ) );		
										
		int number_of_rows_selected( 0 );
		while (result->next())
		{			
			++number_of_rows_selected;
			cql::cql_bigint_t t1( 0 ), t2( 0 );
						
			result->get_bigint( 0, t1 );
			result->get_bigint( 2, t2 );							
			
			if( result->get_decimal_is_int( 1 ) )
			{
				cql::cql_int_t r( 0 );
				result->get_decimal_int( 1, r );
						
				std::map< int, cql::cql_int_t >::const_iterator p = int_map.find( t1 );
				if( p == int_map.end() )
				{					
					BOOST_FAIL( "There is no such element in INT32 map." );
				}			
						
				if( r != p->second )
				{			
					BOOST_FAIL( "The value of INT32 is not correct. " );
				}	
					
				{	//// retrieve it also with int64 and compare the results.	
					
					cql::cql_bigint_t bi( 0 );
					result->get_decimal_int_64( 1, bi );

					cql::cql_bigint_t bi2 = r;

					if( bi2 != bi )
					{	
						BOOST_FAIL( "An int32 value retrieved as int64 gave a different result." );
					}
				}	
			}		
			else	
			{		
				BOOST_FAIL( "An INT32 value is consisdered as a not valid int32. " );
			}																	
		}				

		if( number_of_rows_selected != integer_number_of_rows )
		{		
			BOOST_FAIL( "INT32. The number of selected rows is wrong. " );
		}				
	}					
						
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
							
	{	//// 2. Check the 64-int.	
						
		std::string const table_name = test_utils::SIMPLE_TABLE + "_int64";
					
		test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 decimal, t3 bigint );") % table_name ));
					
		std::map< int, cql::cql_bigint_t > int64_map;
					
		int const integer_number_of_rows = 400;
											
		for( int i = 0; i < integer_number_of_rows; ++i )
		{											
			cql::cql_bigint_t ii = generate_random_int_64();	
					
			if( i < 10 )		//// check also the values from -5 to 5. 
			{		
				ii = i - 5;
			}		
			else if( i < 70 )
			{				
				ii = ii / ( static_cast< cql::cql_bigint_t >( rand() ) * static_cast< cql::cql_bigint_t >( rand() ) * static_cast< cql::cql_bigint_t >( i ) + static_cast< cql::cql_bigint_t >( 10 ) );		// use also small numbers.	
			}				
					
			int64_map.insert( std::make_pair( i, ii ) );
			std::string query_string( boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,%d,%d);") % table_name % i % i % ii % i ) );	
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,cql::CQL_CONSISTENCY_ANY));	
			session->query(_query);										
		}			

		boost::this_thread::sleep(boost::posix_time::seconds(15));
					
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT t1, t2, t3 FROM %s LIMIT %d;") % table_name % 900 ) );		
										
		int number_of_rows_selected( 0 );
		while (result->next())
		{					
			++number_of_rows_selected;

			cql::cql_bigint_t t1( 0 ), t2( 0 );
						
			result->get_bigint( 0, t1 );
			result->get_bigint( 2, t2 );							
					

			if( result->get_decimal_is_int_64( 1 ) )
			{
				cql::cql_bigint_t r( 0 );
				result->get_decimal_int_64( 1, r );
					
				std::map< int, cql::cql_bigint_t >::const_iterator p = int64_map.find( t1 );
				if( p == int64_map.end() )
				{					
					BOOST_FAIL( "There is no such element in INT64 map." );
				}		

				if( r != p->second )
				{
					BOOST_FAIL( "The value of INT64 is not correct. " );
				}

				if( result->get_decimal_is_int( 1 ) )		//// the value is so small that it should be able to retrieve as int32.
				{				
					cql::cql_int_t i32( 0 );
					result->get_decimal_int( 1, i32 );
					
					cql::cql_bigint_t i64 = i32;
					if( i64 != r )
					{	
						BOOST_FAIL( "A small int32 variable retrieved as int32 has wrong value." );
					}	
				}															
			}
			else
			{			
				BOOST_FAIL( "Error. A valid int64 is reported as an invalid int64. " );
			}
		}				
						
		if( number_of_rows_selected != integer_number_of_rows )
		{				
			BOOST_FAIL( "INT64. The number of selected rows is wrong. " );
		}						
	}					

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				
	{	//// 3. Check the double	
						
		std::string const table_name = test_utils::SIMPLE_TABLE + "_double_test";
			
		test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 decimal, t3 bigint );") % table_name ));
					
		std::map< int, double > double_map;
						
		int const integer_number_of_rows = 400;	
													
		for( int i = 0; i < integer_number_of_rows; ++i )
		{					
			double ii = generate_random_double();	
					
			if( i < 10 )
				ii = i - 5;	
												
			double_map.insert( std::make_pair( i, ii ) );				
			std::string query_string( boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,%1.25d,%d);") % table_name % i % i % ii % i ) );	
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,cql::CQL_CONSISTENCY_ANY));	
			session->query(_query);										
		}					
			
		boost::this_thread::sleep(boost::posix_time::seconds(15));
					
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT t1, t2, t3 FROM %s LIMIT %d;") % table_name % 900 ) );		
										
		int number_of_rows_selected( 0 );
		while (result->next())
		{					
			++number_of_rows_selected;

			cql::cql_bigint_t t1( 0 ), t2( 0 );
						
			result->get_bigint( 0, t1 );
			result->get_bigint( 2, t2 );							
								
			if( result->get_decimal_is_double( 1 ) )
			{		
				double r( 0 );
				result->get_decimal_double( 1, r );
					
				std::map< int, double >::const_iterator p = double_map.find( t1 );
				if( p == double_map.end() )
				{					
					BOOST_FAIL( "There is no such element in double map." );
				}		
						
				if( r != p->second )
				{							
					if( p->second != 0.0 )
					{			
						double const diff = ( r - p->second ) / p->second;
							
						if( diff > 1.0e-015 || diff < -1.0e-015 )
						{		
							std::string dr1 = boost::str(boost::format("%1.25d") % r );
							std::string dr2 = boost::str(boost::format("%1.25d") % p->second );
							BOOST_FAIL( "The value of double is not correct. " + dr1 + " " + dr2 );
						}	
					}													
				}				
							
				if( result->get_decimal_is_int_64( 1 ) )		//// this value also is reported as a valid int64. 
				{								
					cql::cql_bigint_t bi( 0 );
					result->get_decimal_int_64( 1, bi );
						
					double bi2 = bi;
						
					if( bi2 != r )
					{	
						BOOST_FAIL( "The value retrieved as int64 gave a different result." );
					}	
				}		
			}					
			else		
			{
				std::cout << "Not a valid double value" << std::endl;	
			}
		}		
				
		if( number_of_rows_selected != integer_number_of_rows )
		{					
			BOOST_FAIL( "double. The number of selected rows is wrong. " );
		}				
	}							
}					
				
BOOST_AUTO_TEST_SUITE_END()	
				
				