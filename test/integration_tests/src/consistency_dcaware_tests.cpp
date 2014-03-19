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
#include "cql/policies/cql_dcaware_round_robin_balancing_policy.hpp"				

#include "consistency_tests.hpp"
							
struct CONSISTENCY_CCM_DC_AWARE_SETUP : test_utils::CCM_SETUP {
    CONSISTENCY_CCM_DC_AWARE_SETUP() : CCM_SETUP(3,3) {}			// create 3 local and 3 remote data centers.	
};				
																				
BOOST_FIXTURE_TEST_SUITE( consistency_dcaware_tests, CONSISTENCY_CCM_DC_AWARE_SETUP )		

//// consistency_dcaware_tests/testDcAwareRFOneTokenAware
BOOST_AUTO_TEST_CASE(testDcAwareRFOneTokenAware)			//// Ask local nodes. At first nodes 1,2,3, later only nodes 1,3 after the second is removed.
{
	builder->with_load_balancing_policy( boost::shared_ptr< cql::cql_load_balancing_policy_t >( new cql::cql_dcaware_round_robin_balancing_policy_t( "dc1" ) ) );  	
	ContinueTheConsistencyTest( ccm, builder );			
}		

	
//// consistency_dcaware_tests/testDcAwareSecondRFOneTokenAware	
BOOST_AUTO_TEST_CASE(testDcAwareSecondRFOneTokenAware)			//// Ask local nodes, it means the nodes 4,5,6.	Nodes 1,2,3 should be ignored as remote nodes.	
{
	builder->with_load_balancing_policy( boost::shared_ptr< cql::cql_load_balancing_policy_t >( new cql::cql_dcaware_round_robin_balancing_policy_t( "dc2" ) ) );  	
	ContinueTheConsistencyTest( ccm, builder );			
}		

									
//// consistency_dcaware_tests/testDcAwareRemoteOnlyRFOneTokenAware	
BOOST_AUTO_TEST_CASE(testDcAwareRemoteOnlyRFOneTokenAware)			//// Ask all six nodes ( 1,2,3,4,5,6 ) because all nodes are treaded as remote nodes. There are no data centers named: "treat_all_as_remote"
{
	builder->with_load_balancing_policy( boost::shared_ptr< cql::cql_load_balancing_policy_t >( new cql::cql_dcaware_round_robin_balancing_policy_t( "treat_all_as_remote", 150 ) ) );  	
	ContinueTheConsistencyTest( ccm, builder );			
}		

		
////// consistency_dcaware_tests/testDcAwareRemoteFailAlwaysOnlyRFOneTokenAware	
//BOOST_AUTO_TEST_CASE(testDcAwareRemoteFailAlwaysOnlyRFOneTokenAware)			//// This test should fail. We connect only to remote nodes, but the limit is zero as default. 
//{
//	builder->with_load_balancing_policy( boost::shared_ptr< cql::cql_load_balancing_policy_t >( new cql::cql_dcaware_round_robin_balancing_policy_t( "treat_all_as_remote" ) ) );  	
//	ContinueTheConsistencyTest( ccm, builder );			
//}		


BOOST_AUTO_TEST_SUITE_END()	