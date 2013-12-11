#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"

#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>

#include <cql/cql_metadata.hpp>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread/future.hpp>

struct CCM_SETUP1 {
    CCM_SETUP1() : conf(cql::get_ccm_bridge_configuration())
	{
		boost::debug::detect_memory_leaks(true);
		int numberOfNodes = 3;
		ccm = cql::cql_ccm_bridge_t::create(conf, "test", numberOfNodes, true);
		ccm_contact_seed = boost::asio::ip::address::from_string(conf.ip_prefix() + "1");
		use_ssl=false;
	}

    ~CCM_SETUP1()
	{ 
		ccm->remove();
	}

	boost::shared_ptr<cql::cql_ccm_bridge_t> ccm;
	const cql::cql_ccm_bridge_configuration_t& conf;
	boost::asio::ip::address ccm_contact_seed;
	bool use_ssl;
    
    cql::cql_host_state_changed_info_t::new_host_state_enum expected_host_state_change;
};

BOOST_FIXTURE_TEST_SUITE( event_test, CCM_SETUP1 )

// This function is called asynchronously every time an event is logged
void
log_callback(
    const cql::cql_short_t,
    const std::string& message)
{
    std::cout << "LOG: " << message << std::endl;
}

void
state_change_callback(
    boost::promise<cql::cql_host_state_changed_info_t::new_host_state_enum>& promise,
    boost::shared_ptr<cql::cql_host_state_changed_info_t> info)
{
    promise.set_value(info->new_state());
}

BOOST_AUTO_TEST_CASE(status_event_down)
{
    boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
    builder->with_log_callback(&log_callback);
		
    builder->add_contact_point(ccm_contact_seed);

    if (use_ssl) {
        builder->with_ssl();
    }
    
    boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
    
    boost::promise<      cql::cql_host_state_changed_info_t::new_host_state_enum>
        event_handled_promise;
    boost::shared_future<cql::cql_host_state_changed_info_t::new_host_state_enum>
        event_handled_future = event_handled_promise.get_future();
    
    cluster->metadata()->on_host_state_changed(boost::bind(state_change_callback,
                                                           boost::ref(event_handled_promise),
                                                           _1));

    ccm->kill(2);
    // Take some time to let the event propagate.
    if (event_handled_future.timed_wait(boost::posix_time::seconds(60))) {
        if (event_handled_future.get() != cql::cql_host_state_changed_info_t::NEW_HOST_STATE_DOWN) {
            BOOST_ERROR("Event received, but state change misinterpreted");
        }
    } else {
        BOOST_ERROR("Timeout expired - no event received");
    }
    
	cluster->shutdown();
}

BOOST_AUTO_TEST_SUITE_END()
