#include <boost/test/unit_test.hpp>
#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"

#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>

struct CCM_SETUP {
    CCM_SETUP() :conf(cql::get_ccm_bridge_configuration())
	{
		int numberOfNodes = 1;
		ccm = cql::cql_ccm_bridge_t::create(conf, "test", numberOfNodes, true);
	}

    ~CCM_SETUP()         
	{ 
		ccm->remove();
	}

	boost::shared_ptr<cql::cql_ccm_bridge_t> ccm;
	const cql::cql_ccm_bridge_configuration_t& conf;
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
		const std::string& host = conf.ip_prefix()+"1";
		const bool use_ssl=false;
        builder->add_contact_point(boost::asio::ip::address::from_string(host));

		if (use_ssl) {
			builder->with_ssl();
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

		if (session) {

            // write a query that switches current keyspace to "system"
            boost::shared_ptr<cql::cql_query_t> use_system(
                new cql::cql_query_t("USE system;", cql::CQL_CONSISTENCY_ONE));

            // send the query to Cassandra
            boost::shared_future<cql::cql_future_result_t> future = session->query(use_system);

            // wait for the query to execute
            future.wait();

			session->close();
		}

		cluster->shutdown();

		BOOST_CHECK_EQUAL(1, 1);
}

BOOST_AUTO_TEST_SUITE_END()
