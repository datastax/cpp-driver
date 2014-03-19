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
							
#include "consistency_tests.hpp"
							
struct CONSISTENCY_CCM_SETUP : test_utils::CCM_SETUP {
    CONSISTENCY_CCM_SETUP() : CCM_SETUP(3,0) {}
};			
			
						
BOOST_FIXTURE_TEST_SUITE( consistency_tests, CONSISTENCY_CCM_SETUP )				//// consistency_tests/testRFOneTokenAware
													

///   	--run_test=consistency_tests/testRFOneTokenAware
BOOST_AUTO_TEST_CASE(testRFOneTokenAware)
{												
	builder->with_load_balancing_policy( boost::shared_ptr< cql::cql_load_balancing_policy_t >( new cql::cql_round_robin_policy_t() ) );
	ContinueTheConsistencyTest( ccm, builder );			
}			
			
BOOST_AUTO_TEST_SUITE_END()	
			
			
			


std::string GetNameOfConsistency( cql::cql_consistency_enum const a_consistencyName )
{
	int i = static_cast< int >( a_consistencyName );
	if( i < 0 || i > 7 )
		return "????";
		
	static std::string constistNameArr[] = { 
			"ANY 	    ", 
			"ONE         ", 
			"TWO         ", 
			"THREE       ", 
			"QUORUM      ", 
			"ALL         ", 
			"LOCAL_QUORUM", 
			"EACH_QUORUM " };
		
	return constistNameArr[ i ];
}	
	
	
std::string GetResultName( int result )
{
	return ( result == 0 ? "Ok    " : "Failed" );
}						


void ContinueTheConsistencyTest(	
	boost::shared_ptr<cql::cql_ccm_bridge_t> ccm,
	boost::shared_ptr<cql::cql_builder_t> builder )
{
	boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
					
	if (!session) 
	{			
		BOOST_FAIL("Session creation failure.");
	}		
				
	policy_tools::create_schema(session, 1);
			
	policy_tools::init(session, 12, cql::CQL_CONSISTENCY_ONE);		//// make the table and 
	policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);		//// make 12 reads from the nodes.		
	policy_tools::show_coordinators();								//// Show what nodes are read from. 
						
					
	ccm->decommission(2);											//// kill node number 2
	boost::this_thread::sleep(boost::posix_time::seconds(20));		//// wait for node 1 to be down
					
	policy_tools::reset_coordinators();		
	policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);		//// make 12 reads from the nodes.
	policy_tools::show_coordinators();								//// Show what nodes are read from. 
						
	std::vector< cql::cql_consistency_enum > failConsistencyV;
					
	failConsistencyV.push_back( cql::CQL_CONSISTENCY_ANY );
	failConsistencyV.push_back( cql::CQL_CONSISTENCY_ONE );
	failConsistencyV.push_back( cql::CQL_CONSISTENCY_TWO );
	failConsistencyV.push_back( cql::CQL_CONSISTENCY_THREE );
	failConsistencyV.push_back( cql::CQL_CONSISTENCY_QUORUM );
	failConsistencyV.push_back( cql::CQL_CONSISTENCY_ALL );
	failConsistencyV.push_back( cql::CQL_CONSISTENCY_LOCAL_QUORUM );
	failConsistencyV.push_back( cql::CQL_CONSISTENCY_EACH_QUORUM );
			
	std::map< cql::cql_consistency_enum, std::pair< int, int > > resultsTogether;	
			
	for( std::size_t i = 0; i < failConsistencyV.size(); ++i )
	{		
		int initResult = 1;
		try	
		{	
			initResult = policy_tools::init( session, 12, failConsistencyV[ i ] );
		}	//// zero means that the opertion performed with SUCCESS 
		catch( ... )
		{
			initResult = 1;
		}

		int queryResult = 1;
		try
		{
			queryResult = policy_tools::query( session, 12, failConsistencyV[ i ] );		//// make 12 reads from the nodes.
		}	//// zero means that the opertion performed with SUCCESS 
		catch( ... )
		{
			queryResult = 1;
		}

		resultsTogether.insert( std::make_pair( failConsistencyV[ i ], std::make_pair( initResult, queryResult ) ) );
	}		
		
	std::cout << std::endl << std::endl;
	std::cout << "RESULTS FOR ALL CONSISTENCIES: " << std::endl;
	std::cout << "CONSISTENCY  INSERT SELECT " << std::endl;
	for( std::map< cql::cql_consistency_enum, std::pair< int, int > >::const_iterator p = resultsTogether.begin(); p != resultsTogether.end(); ++p )
	{
		std::cout << GetNameOfConsistency( p->first ) << " " << GetResultName( p->second.first ) << " " <<  GetResultName( p->second.second ) << std::endl;
	}	
		
	std::cout << std::endl << std::endl << "Press any key...";
			
	{		
		std::string k;
		std::cin >> k;		//// Wait for the user to read the results from the screen.	
	}		
}			
