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

#include "cql/policies/cql_exponential_reconnection_policy_t.hpp"

#include <boost/test/unit_test.hpp>

struct MY_CCM_SETUP : test_utils::CCM_SETUP {
    MY_CCM_SETUP() : CCM_SETUP(1) {}
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
    BOOST_CHECK_EQUAL(schedule->get_delay(), boost::posix_time::seconds(32));
}
    
BOOST_FIXTURE_TEST_SUITE( reconnection_policy_test, MY_CCM_SETUP )

BOOST_AUTO_TEST_SUITE_END()
