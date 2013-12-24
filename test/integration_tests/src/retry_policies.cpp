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


struct RETRY_CCM_SETUP : test_utils::CCM_SETUP {
	RETRY_CCM_SETUP() : CCM_SETUP(2,0) {}
};


BOOST_FIXTURE_TEST_SUITE( retry_policies, RETRY_CCM_SETUP )

BOOST_AUTO_TEST_CASE( default_retry_policy )
{
	boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

	if (!session) {
		BOOST_FAIL("Session creation failture.");
	}

	policy_tools::create_schema(session, 1);

	boost::asio::ip::address host1 = boost::asio::ip::address::from_string(conf.ip_prefix() + "1");
	boost::asio::ip::address host2 = boost::asio::ip::address::from_string(conf.ip_prefix() + "2");

	policy_tools::init(session, 12, cql::CQL_CONSISTENCY_ONE);
	policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);

	policy_tools::assertQueried(host1, 6);
	policy_tools::assertQueried(host2, 6);

	policy_tools::reset_coordinators();

	// Test reads	
	bool successfulQuery = false;
	bool readTimeoutOnce = false;
	bool unavailableOnce = false;
	bool restartOnce = false;
	for (int i = 0; i < 100; ++i)
	{
		// Force a ReadTimeoutException to be performed once
		if (!readTimeoutOnce)
		{
			ccm->kill(2);
		}

		// Force an UnavailableException to be performed once
		if (readTimeoutOnce && !unavailableOnce)
		{				
			test_utils::waitForDownWithWait(host2, cluster, 5);				
		}

		// Bring back node to ensure other errors are not thrown on restart
		if (unavailableOnce && !restartOnce)
		{
			ccm->start(2);
			restartOnce = true;
		}

		int error_code = policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);
		if( error_code != 0)
		{
			if(error_code == 4608) // cql_read_timeout_exception
				readTimeoutOnce = true;
			if(error_code == 4096) // cql_unavailable_exception								
				unavailableOnce = true;				
		}

		if (restartOnce)
			successfulQuery = true;		
	}

		// Ensure the full cycle was completed	
		BOOST_REQUIRE_MESSAGE(successfulQuery, "Hit testing race condition. [Never completed successfully.] (Shouldn't be an issue.):\n");
		BOOST_REQUIRE_MESSAGE(readTimeoutOnce, "Hit testing race condition. [Never encountered a ReadTimeoutException.] (Shouldn't be an issue.):\n");
		BOOST_REQUIRE_MESSAGE(unavailableOnce, "Hit testing race condition. [Never encountered an UnavailableException.] (Shouldn't be an issue.):\n");

		// A weak test to ensure that the nodes were contacted
		policy_tools::assertQueriedAtLeast(host1, 1);
		policy_tools::assertQueriedAtLeast(host2, 1);

		policy_tools::reset_coordinators();

		// Test writes
		successfulQuery = false;
		bool writeTimeoutOnce = false;
		unavailableOnce = false;
		restartOnce = false;
		for (int i = 0; i < 100; ++i)
		{
			// Force a WriteTimeoutException to be performed once
			if (!writeTimeoutOnce)
			{
				ccm->kill(2);
			}

			// Force an UnavailableException to be performed once
			if (writeTimeoutOnce && !unavailableOnce)
			{
				test_utils::waitForDownWithWait(host2, cluster, 5);
			}

			// Bring back node to ensure other errors are not thrown on restart
			if (unavailableOnce && !restartOnce)
			{
				ccm->start(2);
				restartOnce = true;
			}

			int error_code = policy_tools::init(session, 12, cql::CQL_CONSISTENCY_ONE);
			if( error_code != 0)
			{
				if(error_code == 4352) // cql_write_timeout_exception
					writeTimeoutOnce = true;
				if(error_code == 4096) // cql_unavailable_exception								
					unavailableOnce = true;				
			}

			if (restartOnce)
				successfulQuery = true;
		}
		// Ensure the full cycle was completed
		BOOST_REQUIRE_MESSAGE(successfulQuery, "Hit testing race condition. [Never completed successfully.] (Shouldn't be an issue.):\n");
		BOOST_REQUIRE_MESSAGE(writeTimeoutOnce, "Hit testing race condition. [Never encountered a ReadTimeoutException.] (Shouldn't be an issue.):\n");
		BOOST_REQUIRE_MESSAGE(unavailableOnce, "Hit testing race condition. [Never encountered an UnavailableException.] (Shouldn't be an issue.):\n");


		// Test batch writes
		successfulQuery = false;
		writeTimeoutOnce = false;
		unavailableOnce = false;
		restartOnce = false;
		for (int i = 0; i < 100; ++i)
		{
			// Force a WriteTimeoutException to be performed once
			if (!writeTimeoutOnce)
			{
				ccm->kill(2);
			}

			// Force an UnavailableException to be performed once
			if (writeTimeoutOnce && !unavailableOnce)
			{
				test_utils::waitForDownWithWait(host2, cluster, 5);
			}

			// Bring back node to ensure other errors are not thrown on restart
			if (unavailableOnce && !restartOnce)
			{
				ccm->start(2);
				restartOnce = true;
			}

			int error_code = policy_tools::init(session, 12, cql::CQL_CONSISTENCY_ONE, true);
			if( error_code != 0)
			{
				if(error_code == 4352) // cql_write_timeout_exception
					writeTimeoutOnce = true;
				if(error_code == 4096) // cql_unavailable_exception								
					unavailableOnce = true;				
			}

			if (restartOnce)
				successfulQuery = true;
		}
		// Ensure the full cycle was completed
		BOOST_REQUIRE_MESSAGE(successfulQuery, "Hit testing race condition. [Never completed successfully.] (Shouldn't be an issue.):\n");
		BOOST_REQUIRE_MESSAGE(writeTimeoutOnce, "Hit testing race condition. [Never encountered a ReadTimeoutException.] (Shouldn't be an issue.):\n");
		BOOST_REQUIRE_MESSAGE(unavailableOnce, "Hit testing race condition. [Never encountered an UnavailableException.] (Shouldn't be an issue.):\n");

		session->close();
		cluster->shutdown();
}

BOOST_AUTO_TEST_SUITE_END()