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

#include "cql/exceptions/cql_driver_internal_error.hpp"
#include "cql/internal/cql_message_result_impl.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>

struct COMPRESSION_CCM_SETUP : test_utils::CCM_SETUP {
    COMPRESSION_CCM_SETUP() : CCM_SETUP(1,0) {}
};

BOOST_FIXTURE_TEST_SUITE( compression_test_suite, COMPRESSION_CCM_SETUP )

// --run_test=compression_test_suite/snappy_insert_test
BOOST_AUTO_TEST_CASE(snappy_insert_test)
{
    try {
        builder->with_compression(cql::CQL_COMPRESSION_SNAPPY);
    }
    catch (cql::cql_driver_internal_error_exception& e) {
        BOOST_MESSAGE("Warning: snappy compression is unavailable. Test case omitted.");
        return;
    }
    
	boost::shared_ptr<cql::cql_cluster_t> cluster = builder->build();
	boost::shared_ptr<cql::cql_session_t> session = cluster->connect();
	if (!session) {
		BOOST_FAIL("Session creation failure.");
	}

	test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
	session->set_keyspace(test_utils::SIMPLE_KEYSPACE);
	test_utils::query(session, str(boost::format("CREATE TABLE %s(idx bigint PRIMARY KEY, val text);") % test_utils::SIMPLE_TABLE));

    boost::shared_ptr<cql::cql_result_t> result;
    {
        boost::shared_ptr<cql::cql_query_t> insert(
            new cql::cql_query_t(str(boost::format("INSERT INTO %s (idx, val) VALUES (123, '%s');")
                                                   % test_utils::SIMPLE_TABLE
                                                   % test_utils::lorem_ipsum)));
        insert->enable_compression();
    
        boost::shared_future<cql::cql_future_result_t> future_result = session->query(insert);
        if (!future_result.timed_wait(boost::posix_time::seconds(10))) {
            BOOST_FAIL("Insert timed out.");
        }
    }
    {
        boost::shared_ptr<cql::cql_query_t> select(
            new cql::cql_query_t(str(boost::format("SELECT val FROM %s WHERE idx = 123;")
                                                    % test_utils::SIMPLE_TABLE)));
    
        boost::shared_future<cql::cql_future_result_t> future_result = session->query(select);
        if (!future_result.timed_wait(boost::posix_time::seconds(10))) {
            BOOST_FAIL("Select timed out.");
        }
        result = future_result.get().result;
    }
    
    std::string lorem_ipsum_received;
    if (!result->next()) {
        BOOST_FAIL("Received an empty result.");
    }
    result->get_text("val", lorem_ipsum_received);
    
    BOOST_REQUIRE(lorem_ipsum_received == test_utils::lorem_ipsum);
    
    boost::shared_ptr<cql::cql_message_result_impl_t> result_downcast
        = boost::dynamic_pointer_cast<cql::cql_message_result_impl_t>(result);
    
    if (result_downcast) {
        if (!result_downcast->is_compressed()) {
            BOOST_MESSAGE("Received uncompressed response. The results of this test may not be reliable.");
        }
    }
    else {
        BOOST_MESSAGE("Downcast failed. The results of this test may not be reliable.");
    }
}

BOOST_AUTO_TEST_SUITE_END()
