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
		int numberOfNodes = 1;
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

boost::condition_variable cond;
boost::mutex mut;

volatile cql::cql_host_state_changed_info_t::new_host_state_enum new_state;
volatile bool is_ready;

void
state_change_callback(
    boost::shared_ptr<cql::cql_host_state_changed_info_t> info)
{
    // Let the main thread hit the condition. (TODO: any better idea?)
    boost::this_thread::sleep(boost::posix_time::seconds(1));
    
    {
        boost::lock_guard<boost::mutex> lock(mut);
        new_state = info->new_state();
        is_ready = true;
    }
    cond.notify_one();
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
    cluster->metadata()->on_host_state_changed(&state_change_callback);
    
    // Test the event generated when stopping a host:
    { // Scope limiter for `lock'
        boost::unique_lock<boost::mutex> lock(mut);
        new_state = (cql::cql_host_state_changed_info_t::new_host_state_enum)(-1);
        is_ready = false;
        ccm->stop(2);
        while(!is_ready) {
            cond.timed_wait(lock, boost::posix_time::seconds(30));
        }

        if (new_state != cql::cql_host_state_changed_info_t::NEW_HOST_STATE_DOWN) {
            BOOST_ERROR("Event received, but with wrong state change description");
        }
    }

    // Test the event generated when starting a host:
    { // Scope limiter for `lock'
        boost::unique_lock<boost::mutex> lock(mut);
        new_state = (cql::cql_host_state_changed_info_t::new_host_state_enum)(-1);
        is_ready = false;
        ccm->start(2);
        while(!is_ready) {
            cond.timed_wait(lock, boost::posix_time::seconds(30));
        }
        
        if (new_state != cql::cql_host_state_changed_info_t::NEW_HOST_STATE_UP) {
            
            // We will give ourselves another chance - this is because HOST_STATE_DOWN
            // was being fired (again) before HOST_STATE_UP at the time of this writing.
            new_state = (cql::cql_host_state_changed_info_t::new_host_state_enum)(-1);
            is_ready = false;
            while(!is_ready) {
                cond.timed_wait(lock, boost::posix_time::seconds(30));
            }
            
            if (new_state != cql::cql_host_state_changed_info_t::NEW_HOST_STATE_UP) {
                BOOST_ERROR("Event received, but with wrong state change description");
            }
        }
    }
    
	cluster->shutdown();
}

BOOST_AUTO_TEST_SUITE_END()
