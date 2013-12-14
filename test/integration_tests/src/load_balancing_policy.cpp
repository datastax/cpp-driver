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

#include "cql/cql_list.hpp"
#include "cql/cql_set.hpp"
#include "cql/cql_map.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>


struct MY_CCM_SETUP : test_utils::CCM_SETUP {
    MY_CCM_SETUP() : CCM_SETUP(2) {}
};


 void create_schema(boost::shared_ptr<cql::cql_session_t> session, int replicationFactor)
    {		
		test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % replicationFactor));
		session->set_keyspace(test_utils::SIMPLE_KEYSPACE);
		test_utils::query(session, str(boost::format("CREATE TABLE %s (k int PRIMARY KEY, i int)") % test_utils::SIMPLE_TABLE));
    }

 void init(boost::shared_ptr<cql::cql_session_t> session,int n, cql::cql_consistency_enum cl, bool batch = false)
 {
	 for( int i =0; i< n; ++i)	 
		test_utils::query(session, str(boost::format("INSERT INTO %s(k, i) VALUES (0, 0)") % test_utils::SIMPLE_TABLE), cl);
 }

 std::map<boost::asio::ip::address, int> coordinators;
 void add_coordinator(boost::asio::ip::address coord_addr)
 {
	 if(coordinators.count(coord_addr) == 0)
		coordinators.insert(std::make_pair(coord_addr, 1));
	 else
		 coordinators[coord_addr] += 1;
 }
 void reset_coordinators()
 {
	 coordinators.clear();	
 }
 void assertQueried(boost::asio::ip::address coord_addr, int n)
 {
	 if(coordinators.count(coord_addr)!=0)	 
		 assert(coordinators[coord_addr] == n);
	 else
		 assert(n == 0);	 
 }
void query(boost::shared_ptr<cql::cql_session_t> session, int n, cql::cql_consistency_enum cl)
{
	for (int i = 0; i < n; ++i)
	{
		boost::shared_ptr<cql::cql_query_t> _query(
			new cql::cql_query_t(str(boost::format("SELECT * FROM %s WHERE k = 0") % test_utils::SIMPLE_TABLE),cl));
		boost::shared_future<cql::cql_future_result_t> query_future = session->query(_query);
		query_future.wait();
		
		cql::cql_future_result_t query_result = query_future.get();
		std::cout << query_result.client->endpoint().to_string();

		add_coordinator(query_result.client->endpoint().address());				
	}
}

BOOST_FIXTURE_TEST_SUITE( _load_balancing_policy, MY_CCM_SETUP )

BOOST_AUTO_TEST_CASE( round_robin )
{
		boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

		if (!session) {
			BOOST_FAIL("Session creation failture.");
		}
		
		create_schema(session, 1);
		
        init(session, 12, cql::CQL_CONSISTENCY_ONE);
        query(session, 12, cql::CQL_CONSISTENCY_ONE);
		
		boost::asio::ip::address host1 = boost::asio::ip::address::from_string(conf.ip_prefix() + "1");
		boost::asio::ip::address host2 = boost::asio::ip::address::from_string(conf.ip_prefix() + "2");
	
		assertQueried(host1, 6);
		assertQueried(host2, 6);

		session->close();
		cluster->shutdown();
}

BOOST_AUTO_TEST_SUITE_END()
