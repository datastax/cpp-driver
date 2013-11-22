#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"

#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>

struct CCM_SETUP {
    CCM_SETUP() :conf(cql::get_ccm_bridge_configuration())
	{
		boost::debug::detect_memory_leaks(true);
		int numberOfNodes = 1;
		ccm = cql::cql_ccm_bridge_t::create(conf, "test", numberOfNodes, true);
		ccm_contact_seed = boost::asio::ip::address::from_string(conf.ip_prefix()+"1");
		use_ssl=false;
	}

    ~CCM_SETUP()         
	{ 
		ccm->remove();
	}

	boost::shared_ptr<cql::cql_ccm_bridge_t> ccm;
	const cql::cql_ccm_bridge_configuration_t& conf;
	boost::asio::ip::address ccm_contact_seed;
	bool use_ssl;
};

BOOST_FIXTURE_TEST_SUITE( simple_test1, CCM_SETUP )

// This function is called asynchronously every time an event is logged
void
log_callback(
    const cql::cql_short_t,
    const std::string& message)
{
    std::cout << "LOG: " << message << std::endl;
}

BOOST_AUTO_TEST_CASE(test1)
{
		boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
		builder->with_log_callback(&log_callback);
		
    builder->add_contact_point(ccm_contact_seed);

		if (use_ssl) {
			builder->with_ssl();
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

	if (!session) {
		BOOST_FAIL("Session creation failture.");
	}

            // write a query that switches current keyspace to "system"
            boost::shared_ptr<cql::cql_query_t> use_system(
                new cql::cql_query_t("USE system;", cql::CQL_CONSISTENCY_ONE));

            // send the query to Cassandra
            boost::shared_future<cql::cql_future_result_t> future = session->query(use_system);

            // wait for the query to execute
            future.wait();

//	session->close();
//	cluster->shutdown();

}

BOOST_AUTO_TEST_SUITE_END()
