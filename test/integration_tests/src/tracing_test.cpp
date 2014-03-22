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

struct TRACING_CCM_SETUP : test_utils::CCM_SETUP {
    TRACING_CCM_SETUP() : CCM_SETUP(4,0) {}
};

BOOST_FIXTURE_TEST_SUITE( tracing_test_suite, TRACING_CCM_SETUP )

BOOST_AUTO_TEST_CASE(simple_test)
{
}

BOOST_AUTO_TEST_SUITE_END()
