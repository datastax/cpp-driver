#ifndef CQL_METADATA_H_
#define CQL_METADATA_H_

#include <vector>	
#include <boost/shared_ptr.hpp>
	
namespace cql 
{
	class cql_host_t;			
	class cql_metadata_t			// the mock-object for testing dcaware_round_robin_balancing_policy. 				
	{								// this mock-object enables to create the virtual nodes to test balancing node selection policy
	public:			
				
		void get_hosts(std::vector<boost::shared_ptr<cql_host_t> >& collection) const 
		{ 
			collection = _collection; 
		};	
			
		std::vector<boost::shared_ptr<cql_host_t> > _collection;
	};				
}					
					
#endif // CQL_METADATA_H_
						
#include "../src/cql/policies/cql_dcaware_round_robin_balancing_policy.cpp"			// the definition of dcaware_round_balancing_policy must be added to cql_test project again, to exist in the main cql_test.exe. 
																					// It must be called locally, to call the mock-object instead of the real cql_metadata_t class.		
					
#include <boost/test/unit_test.hpp>
#include "cql/policies/cql_constant_reconnection_policy_t.hpp"
					
namespace cql		
{								
	class cql_cluster_dcaware_testing_t : public cql::cql_cluster_t		//// a mock-object for testing the dcaware_round_robin_policy. Enables to manually create a lot of virtual nodes.	
	{				
	public:				
		cql_cluster_dcaware_testing_t() { m_metadata_dcaware.reset( new cql_metadata_t() ); }	
					
		virtual boost::shared_ptr<cql_metadata_t> metadata() const 
		{			
			return m_metadata_dcaware; 
		}				
					
		virtual boost::shared_ptr<cql_session_t> connect(){ return boost::shared_ptr<cql_session_t>(); }
		virtual boost::shared_ptr<cql_session_t> connect(const std::string& keyspace){ return boost::shared_ptr<cql_session_t>(); }
		virtual void shutdown(int timeout_ms = -1){}	
					
		boost::shared_ptr< cql_metadata_t > m_metadata_dcaware;
	};				
}					
					
					
class DC_AWARE_ALGORITHM_TEST
{
};

					
int get_last_part_of_ip( std::string ip )		// get the last part of IP address. 
{	
	int p1 =  ip.rfind( ":" );
	int p2 =  ip.rfind( "." );	
	std::string ret = ip.substr( p2 + 1, ( p1 - p2 - 1 ) );	
	int ret2 = atoi( ret.c_str() );
	return ret2;
}	


std::string Tx( int i )		// simple conversion from int to string.	
{
	return boost::str(boost::format("%d") % i );
}	
					

BOOST_FIXTURE_TEST_SUITE( dcaware_algorithm_test, DC_AWARE_ALGORITHM_TEST )     //// dcaware_algorithm_test/dc_aware_algorithm
					
BOOST_AUTO_TEST_CASE(dc_aware_algorithm)
{								
	cql::cql_cluster_dcaware_testing_t cluster_dcaware_testing;	
					
	srand( (unsigned)time(NULL) );
										
	int the_number_of_data_centers( 30 );		
	int the_number_of_nodes_in_each_data_center( 40 );	
	int the_limit_of_number_of_queries_for_each_remote_data_center( 20 );	
						
	//// populate the cluster with many nodes in many remote datacenters.	
	for( int i1 = 10; i1 < 10 + the_number_of_data_centers; ++i1 )		// the number of remote data centers.
	{					
		for( int i2 = 10; i2 < 10 + the_number_of_nodes_in_each_data_center; ++i2 )	// the number of nodes in each data centers.
		{				
			std::string add = "192.168." + Tx( i1 ) + "." + Tx( i2 );
			boost::asio::ip::address address( boost::asio::ip::address::from_string( add ) );	
			std::string kkk = address.to_string();		
			cql::cql_endpoint_t  end_point( address, 30000 );			
			std::string end_point_data = end_point.to_string();		
			boost::shared_ptr<cql::cql_host_t> new_host = cql::cql_host_t::create( end_point, boost::shared_ptr< cql::cql_reconnection_policy_t>( new cql::cql_constant_reconnection_policy_t( boost::posix_time::seconds(1))) );
			new_host->set_location_info( "dc" + Tx( i1 ), "rack" + Tx( i1 ) );		// set the datacenter name and rack name.	
			cluster_dcaware_testing.metadata()->_collection.push_back( new_host );
		}							
	}								
										


		
	{   //// Select only the local nodes. Ask for a local node which exists. Count the queries to each node. It should be linear round-robin on local nodes.	
		//// The same nodes should be taken many times. 
		cql::cql_dcaware_round_robin_balancing_policy_t dc_balancing( "dc10", the_limit_of_number_of_queries_for_each_remote_data_center );	
		dc_balancing.init( &cluster_dcaware_testing );	
					
		boost::shared_ptr< cql::cql_query_plan_t> query_plan_dc = dc_balancing.new_query_plan( boost::shared_ptr< cql::cql_query_t>() );
								
		std::map< std::string, long >	dc_query_quantity;	
		std::map< std::string, long >	ip_query_quantity;	
							
		for( int i = 0; i < cluster_dcaware_testing.metadata()->_collection.size(); ++i)
		{				
			boost::shared_ptr< cql::cql_host_t> host = query_plan_dc->next_host_to_query();			// Returns next host to query.
			if( host.get() != NULL )
			{
				std::map< std::string, long >::iterator p = dc_query_quantity.find( host->datacenter() );
										
				if( p == dc_query_quantity.end() )
				{	
					dc_query_quantity.insert( std::make_pair( host->datacenter(), 1 ) );
				}	
				else
				{
					++( p->second );	
				}	
					
				std::map< std::string, long >::iterator p2 = ip_query_quantity.find( host->endpoint().to_string() );
										
				if( p2 == ip_query_quantity.end() )
				{	
					ip_query_quantity.insert( std::make_pair( host->endpoint().to_string(), 1 ) );
				}	
				else
				{	
					++( p2->second );	
				}											
			}	
			else
			{
				break;	
			}
		}				
							
		BOOST_REQUIRE( dc_query_quantity.size() == 1 );				//// there should be only one datacenter which was queried.
						
		std::map< std::string, long >::const_iterator p2 = dc_query_quantity.find( "dc10" );		//// check if this is the local datacenter.		
			
		BOOST_REQUIRE( p2 != dc_query_quantity.end() );
		BOOST_REQUIRE( p2->second == cluster_dcaware_testing.metadata()->_collection.size() );	
						
		for( std::map< std::string, long >::const_iterator p = ip_query_quantity.begin(); p != ip_query_quantity.end(); ++p )
		{																// each node should be taken the same number of times,
			BOOST_REQUIRE( p->second == the_number_of_data_centers );	// because this is the linear round-robin.	
		}									
	}							
								
													


	{ // Select only the remote nodes. Ask for a local node which does not exist. Count the queries to each last number of IP address.	
		cql::cql_dcaware_round_robin_balancing_policy_t dc_balancing( "dc1", the_limit_of_number_of_queries_for_each_remote_data_center );	
		dc_balancing.init( &cluster_dcaware_testing );	
				
		boost::shared_ptr< cql::cql_query_plan_t> query_plan_dc = dc_balancing.new_query_plan( boost::shared_ptr< cql::cql_query_t>() );
								
		std::map< std::string, long > dc_query_quantity;		// how many queries for each data centers.
		std::map< long, long > last_ip_part_query_quantity;		// how many queries for each last part of ip address.	
								
		for( int i2 = 10; i2 < 10 + the_number_of_nodes_in_each_data_center; ++i2 )	// the number of nodes in each data centers.
		{			
			last_ip_part_query_quantity.insert( std::make_pair( i2, 0 ) );		//// prepare the map for counting the number of queries for each last part of address.	
		}				
								
		int const query_qnt = the_number_of_data_centers * the_limit_of_number_of_queries_for_each_remote_data_center / 2;		// how many queries. 
					
		for( int i = 0; i < query_qnt; ++i)
		{				
			boost::shared_ptr< cql::cql_host_t> host = query_plan_dc->next_host_to_query();			// Returns next host to query.
			if( host.get() != NULL )
			{															
				int const last_part_of_ip = get_last_part_of_ip( host->endpoint().to_string() );
						
				std::map< std::string, long >::iterator p = dc_query_quantity.find( host->datacenter() );
										
				if( p == dc_query_quantity.end() )
				{	
					dc_query_quantity.insert( std::make_pair( host->datacenter(), 1 ) );
				}	
				else
				{
					++( p->second );	
				}	

				std::map< long, long >::iterator p3 = last_ip_part_query_quantity.find( last_part_of_ip );
				if( p3 != last_ip_part_query_quantity.end() )
				{	
					++( p3->second );	
				}			
			}				
			else			
			{				
				break;			
			}					
		}						
								
		std::vector< int > occu;						//// the number of queries to each fourth part of IP address. 
				
		for( std::map< long, long >::const_iterator p4 = last_ip_part_query_quantity.begin(); p4 != last_ip_part_query_quantity.end(); ++p4 )
			occu.push_back( p4->second );		
					
		if( occu.size() < 4 )
		{		
			BOOST_REQUIRE( false );				//// to few nodes to compute the reliable reulsts of the test.
		}		
				
		std::sort( occu.begin(), occu.end() );			//// Sort by the number of queries by each last number of IP address.
				
		int index1 = occu.size() / 4;					//// Take the value of the vector of about 25% of the length and of about the 75% of the length.
		int index2 = index1 * 3;						//// Compute what is the difference. There should be small difference. The less the better. 
			
		if( index1 < occu.size() && index2 < occu.size() )
		{	
			int lowValue = occu[ index1 ];					
			int highValue = occu[ index2 ];					
							
			BOOST_REQUIRE( lowValue > 0 );					
			BOOST_REQUIRE( highValue / lowValue < 3 );
		}				
	}					
									
}						
						
BOOST_AUTO_TEST_SUITE_END()	
			
