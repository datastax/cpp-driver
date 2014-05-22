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
#include "cql/cql_query_trace.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>

#include <list>

struct TRACING_CCM_SETUP : test_utils::CCM_SETUP {
    TRACING_CCM_SETUP() : CCM_SETUP(2,0) {}
};

BOOST_FIXTURE_TEST_SUITE( tracing_test_suite, TRACING_CCM_SETUP )

// --run_test=tracing_test_suite/simple_test
BOOST_AUTO_TEST_CASE(simple_test)
{
    boost::shared_ptr<cql::cql_cluster_t> cluster = builder->build();
    boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
    if (!session) {
        BOOST_FAIL("Session creation failure.");
    }
    
    boost::shared_ptr<cql::cql_query_t> some_query(
        new cql::cql_query_t("SELECT * from system.schema_keyspaces"));
    some_query->enable_tracing();
    
    boost::shared_future<cql::cql_future_result_t> future_result = session->query(some_query);
    if (!(future_result.timed_wait(boost::posix_time::seconds(10)))) {
        BOOST_FAIL("Traced query timed out");
    }

    boost::shared_ptr<cql::cql_result_t> result = future_result.get().result;
    cql::cql_uuid_t tracing_id;
    if (!(result->get_tracing_id(tracing_id))) {
        BOOST_FAIL("Failed to read the tracing ID");
    }
    
    cql::cql_query_trace_t tracer(tracing_id, session);
    
    std::list<cql::cql_trace_event_t> events;
    if (!(tracer.get_events(events))) {
        boost::this_thread::sleep(boost::posix_time::seconds(5));
        if (!(tracer.get_events(events))) {
            BOOST_FAIL("Failed to read the list of events");
        }
    }
    
    BOOST_REQUIRE(events.size() != 0);
}

BOOST_AUTO_TEST_SUITE_END()
