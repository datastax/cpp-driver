#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"
#include "policy_tools.hpp"
#include "cql/exceptions/cql_no_host_available_exception.hpp"

#include "cql/cql_session.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"

#include "cql/policies/cql_exponential_reconnection_policy_t.hpp"

#include <boost/test/unit_test.hpp>

#include <cmath>

struct RECONN_POLICY_CCM_SETUP : test_utils::CCM_SETUP {
    RECONN_POLICY_CCM_SETUP() : CCM_SETUP(1,0) {}
};

BOOST_AUTO_TEST_CASE(exponential_policy_construction_test)
{
    try {
        cql::cql_exponential_reconnection_policy_t erp(boost::posix_time::milliseconds(-10),
                                                       boost::posix_time::milliseconds( 10));
        BOOST_ERROR("Expected exception: negative base delay");
    }
    catch(std::invalid_argument& e) {
        // Exception = good
    }
    try {
        cql::cql_exponential_reconnection_policy_t erp(boost::posix_time::milliseconds( 10),
                                                       boost::posix_time::milliseconds(-10));
        BOOST_ERROR("Expected exception: negative max delay");
    }
    catch(std::invalid_argument& e) {
        // Exception = good
    }
    try {
        // Mind the `microseconds' in first line
        cql::cql_exponential_reconnection_policy_t erp(boost::posix_time::microseconds(10),
                                                       boost::posix_time::milliseconds(20));
        BOOST_ERROR("Expected exception: base delay < 1ms");
    }
    catch(std::invalid_argument& e) {
        // Exception = good
    }
    try {
        cql::cql_exponential_reconnection_policy_t erp(boost::posix_time::milliseconds(20),
                                                       boost::posix_time::milliseconds(10));
        BOOST_ERROR("Expected exception: base delay > max_delay");
    }
    catch(std::invalid_argument& e) {
        // Exception = good
    }
}

BOOST_AUTO_TEST_CASE(exponential_policy_delays_test)
{
    cql::cql_exponential_reconnection_policy_t erp(boost::posix_time::seconds(1 ),
                                                   boost::posix_time::seconds(32));
    
    boost::shared_ptr<cql::cql_reconnection_schedule_t> schedule = erp.new_schedule();
    BOOST_CHECK_EQUAL(schedule->get_delay(), boost::posix_time::seconds(1 ));
    BOOST_CHECK_EQUAL(schedule->get_delay(), boost::posix_time::seconds(2 ));
    BOOST_CHECK_EQUAL(schedule->get_delay(), boost::posix_time::seconds(4 ));
    BOOST_CHECK_EQUAL(schedule->get_delay(), boost::posix_time::seconds(8 ));
    BOOST_CHECK_EQUAL(schedule->get_delay(), boost::posix_time::seconds(16));
    BOOST_CHECK_EQUAL(schedule->get_delay(), boost::posix_time::seconds(32));
    BOOST_CHECK_EQUAL(schedule->get_delay(), boost::posix_time::seconds(32));
    
    // Overflow test
    for (int i = 0; i < 64; ++i) {
        schedule->get_delay();
    }
    BOOST_CHECK_EQUAL(schedule->get_delay(), boost::posix_time::seconds(32));
}
    
BOOST_FIXTURE_TEST_SUITE( reconnection_policy_integration_tests, RECONN_POLICY_CCM_SETUP )

BOOST_AUTO_TEST_CASE( exp_reconnection_policy_integration_test )
{
    /* Please note: this test is suitable ONLY IF `builder' has exponential
       reconnection policy. This is the default at the time of this writing,
       but may change some day. If it does, please provide RECONN_POLICY_CCM_SETUP
       class with reconn policy explicitly set to cql_exponential_reconnection_policy_t. */
    
    boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
    builder->add_contact_point(ccm_contact_seed);
    
    const boost::shared_ptr<cql::cql_reconnection_policy_t> policy
        = builder->configuration()->policies().reconnection_policy();
    boost::shared_ptr<cql::cql_reconnection_schedule_t> schedule
        = policy->new_schedule();
    
    // We need `restart_time' to be equal to three full cycles [in seconds] + two extra seconds.
    long restart_time = 2;
    restart_time += schedule->get_delay().total_seconds();
    restart_time += schedule->get_delay().total_seconds();
    restart_time += schedule->get_delay().total_seconds();
    
    // retry_time is the expected time before 4th reconnection
    const long retry_time = restart_time - 2 + schedule->get_delay().total_seconds();
    const long break_time = 2*retry_time + 2;
    
    if (use_ssl) {
        builder->with_ssl();
    }
    
    boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
	if (!session) {
		BOOST_FAIL("Session creation failure.");
	}
    
	policy_tools::create_schema(session, 1);
    
	boost::asio::ip::address host = boost::asio::ip::address::from_string(conf.ip_prefix() + "1");
    
	policy_tools::init(session, 12, cql::CQL_CONSISTENCY_ONE);
	policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);
	policy_tools::assertQueried(host, 12);
	policy_tools::reset_coordinators();
    
    ccm->stop(1);
    boost::posix_time::ptime now, then =
        boost::posix_time::second_clock::local_time();

    // Ensure that the node is down.
    try {
        policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);
        BOOST_FAIL("Test race condition where node has not shut off quickly enough.");
    }
    catch(cql::cql_no_host_available_exception& e) {
        // Exception = good
    }
    
	policy_tools::reset_coordinators(); // Just in case.
    
    long elapsed_seconds;
    bool restarted = false;
    
    while (true) {
        now = boost::posix_time::second_clock::local_time();
        elapsed_seconds = (now - then).total_seconds();
        
        if (!restarted && elapsed_seconds > restart_time) {
            ccm->start(1);
            restarted = true;
        }

        try {
            policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);
            policy_tools::assertQueried(host, 12);
            policy_tools::reset_coordinators();
            
            // Ensure the time when the query completes successfully is what was expected
            if (::abs(retry_time - elapsed_seconds) > 6) {
                BOOST_FAIL(boost::str(boost::format("Waited %1% seconds instead of expected %2% seconds")
                                                    % elapsed_seconds
                                                    % retry_time));
            }
        }
        catch (cql::cql_no_host_available_exception& e) {
            boost::this_thread::sleep(boost::posix_time::seconds(1));
            continue;
        }
        
        boost::this_thread::sleep(boost::posix_time::seconds(break_time));

        // Query once again, just to be sure.
        policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);
        policy_tools::assertQueried(host, 12);
        policy_tools::reset_coordinators();

        ccm->stop(1);
        restarted = false;
        then = boost::posix_time::second_clock::local_time();
        
        // Ensure that the node is down.
        try {
            policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);
            BOOST_FAIL("Test race condition where node has not shut off quickly enough.");
        }
        catch(cql::cql_no_host_available_exception& e) {
            // Exception = good
        }
        
        // Upon reconnection the host gets a new reconnection schedule, so we can run the same test again.
        while (true) {
            now = boost::posix_time::second_clock::local_time();
            elapsed_seconds = (now - then).total_seconds();
            
            if (!restarted && elapsed_seconds > restart_time) {
                ccm->start(1);
                restarted = true;
            }
            
            try {
                policy_tools::query(session, 12, cql::CQL_CONSISTENCY_ONE);
                policy_tools::assertQueried(host, 12);
                policy_tools::reset_coordinators();
                
                // Ensure the time when the query completes successfully is what was expected
                if (::abs(retry_time - elapsed_seconds) > 3) {
                    BOOST_FAIL(boost::str(boost::format("Waited %1% seconds instead of expected %2% seconds")
                                                        % elapsed_seconds
                                                        % retry_time));
                }
            }
            catch (cql::cql_no_host_available_exception& e) {
                boost::this_thread::sleep(boost::posix_time::seconds(1));
                continue;
            }
            break;
        }
        break;
    }
    
    policy_tools::reset_coordinators();
    session->close();
    cluster->shutdown();
}

BOOST_AUTO_TEST_SUITE_END()
