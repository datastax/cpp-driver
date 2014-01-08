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


struct LB_CCM_SETUP : test_utils::CCM_SETUP {
    LB_CCM_SETUP() : CCM_SETUP(2,0) {}
};


BOOST_FIXTURE_TEST_SUITE( _load_balancing_policy, LB_CCM_SETUP )

BOOST_AUTO_TEST_CASE( round_robin )
{
	boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

	if (!session) {
		BOOST_FAIL("Session creation failure.");
	}

	policy_tools::create_schema(session, 1);


	policy_tools::init(session, 12, cql::CQL_CONSISTENCY_ONE);
	policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);

	boost::asio::ip::address host1 = boost::asio::ip::address::from_string(conf.ip_prefix() + "1");
	boost::asio::ip::address host2 = boost::asio::ip::address::from_string(conf.ip_prefix() + "2");

	policy_tools::assertQueried(host1, 6);
	policy_tools::assertQueried(host2, 6);

	policy_tools::reset_coordinators();
	ccm->bootstrap(3);

	boost::this_thread::sleep(boost::posix_time::seconds(15)); // wait for node 3 to be up
	boost::asio::ip::address host3 = boost::asio::ip::address::from_string(conf.ip_prefix() + "3");

	policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);

	policy_tools::assertQueried(host1, 4);
	policy_tools::assertQueried(host2, 4);
	policy_tools::assertQueried(host3, 4);

	policy_tools::reset_coordinators();
	ccm->decommission(1);
	boost::this_thread::sleep(boost::posix_time::seconds(15)); // wait for node 1 to be down

	policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);

	policy_tools::assertQueried(host2, 6);
	policy_tools::assertQueried(host3, 6);

	policy_tools::reset_coordinators();
	session->close();
	cluster->shutdown();
}
BOOST_AUTO_TEST_SUITE_END()
struct LB_CCM_SETUP_2DC : test_utils::CCM_SETUP {
	LB_CCM_SETUP_2DC() : CCM_SETUP(2,2){}
};

BOOST_FIXTURE_TEST_SUITE(_load_balancing_policy2DCs, LB_CCM_SETUP_2DC)

BOOST_AUTO_TEST_CASE( round_robin_2DCs )
{
	boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

	if (!session) {
		BOOST_FAIL("Session creation failture.");
	}

	policy_tools::create_schema(session, 1);

	policy_tools::init(session, 12, cql::CQL_CONSISTENCY_ONE);
	policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);

	boost::asio::ip::address host1 = boost::asio::ip::address::from_string(conf.ip_prefix() + "1");
	boost::asio::ip::address host2 = boost::asio::ip::address::from_string(conf.ip_prefix() + "2");
	boost::asio::ip::address host3 = boost::asio::ip::address::from_string(conf.ip_prefix() + "3");
	boost::asio::ip::address host4 = boost::asio::ip::address::from_string(conf.ip_prefix() + "4");

	policy_tools::assertQueried(host1, 3);
	policy_tools::assertQueried(host2, 3);
	policy_tools::assertQueried(host3, 3);
	policy_tools::assertQueried(host4, 3);

	policy_tools::reset_coordinators();

	ccm->bootstrap(5,"dc2");
	boost::asio::ip::address host5 = boost::asio::ip::address::from_string(conf.ip_prefix() + "5");
	ccm->decommission(1);
	boost::this_thread::sleep(boost::posix_time::seconds(60));	

	policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);

	policy_tools::assertQueried(host1, 0);
	policy_tools::assertQueried(host2, 3);
	policy_tools::assertQueried(host3, 3);
	policy_tools::assertQueried(host4, 3);
	policy_tools::assertQueried(host5, 3);

	policy_tools::reset_coordinators();
	session->close();
	cluster->shutdown();
}
BOOST_AUTO_TEST_SUITE_END()
