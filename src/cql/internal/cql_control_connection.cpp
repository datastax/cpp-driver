#include <exception>
#include <iterator>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "cql/internal/cql_control_connection.hpp"
#include "cql/internal/cql_cluster_impl.hpp"
#include "cql/exceptions/cql_exception.hpp"

namespace cql {
    
cql_control_connection_t::cql_control_connection_t(cql_cluster_t&                         cluster,
                                                   boost::asio::io_service&               io_service,
                                                   boost::shared_ptr<cql_configuration_t> configuration)
    : _cluster(cluster)
    , _io_service(io_service)
    , _configuration(configuration)
    , _timer(io_service)
    , _log_callback(_configuration->client_options().log_callback())
{
    cql_session_callback_info_t session_callbacks;

    cql::cql_session_t::cql_client_callback_t client_factory;
    boost::asio::ssl::context* ssl_context =
        _configuration->protocol_options()
            .ssl_context().get();
    
    if (ssl_context != 0) {
        client_factory = client_ssl_functor_t(
                                              _io_service,
                                              const_cast<boost::asio::ssl::context&>(*ssl_context),
                                              _log_callback);
    }
    else {
        client_factory = client_functor_t(_io_service, _log_callback);
    }
    
    session_callbacks.set_client_callback(client_factory);
    //session_callbacks.set_ready_callback(xxx);  // TODO(JS)
    //session_callbacks.set_defunct_callback(xx); // TODO(JS)
    session_callbacks.set_log_callback(_log_callback);
    
    boost::scoped_ptr<cql_reconnection_policy_t> _reconnection_policy_tmp(
        new cql_exponential_reconnection_policy_t(boost::posix_time::seconds(2),
                                                  boost::posix_time::minutes(5)));
    
    _reconnection_schedule = _reconnection_policy_tmp->new_schedule();
    
    _session = boost::shared_ptr<cql_session_impl_t>(
                    new cql_session_impl_t(session_callbacks, configuration));
}

void
cql_control_connection_t::init()
{
    _session->init(_io_service);
    setup_control_connection();
}
    
void
cql_control_connection_t::shutdown(int timeout_ms)
{
    if(_is_open.exchange(false, boost::memory_order_acq_rel)) {
        _timer.cancel();
        boost::static_pointer_cast<cql_session_t>(_session)->close();
    }
}
    
cql_control_connection_t::~cql_control_connection_t()
{
    try {
        shutdown();
    }
    catch(...) {
        if(_log_callback)
            _log_callback(CQL_LOG_ERROR, "Error while shutting down control connection");
    }
}

void
cql_control_connection_t::metadata_hosts_event(void* sender,
                                               boost::shared_ptr<cql_event_t> event)
{
}
    
void
cql_control_connection_t::setup_event_listener()
{
    boost::shared_ptr<cql_query_plan_t> query_plan = _configuration->policies()
        .load_balancing_policy()
            ->new_query_plan(boost::shared_ptr<cql_query_t>());
    
    cql_stream_t stream;
    std::list<cql_endpoint_t> tried_hosts;
    
    _active_connection = _session->connect(query_plan, &stream, &tried_hosts);
}
    
void
cql_control_connection_t::refresh_node_list_and_token_map()
{
    if(_log_callback)
        _log_callback(CQL_LOG_INFO, "Refreshing node list and token map...");
    
    boost::shared_future<cql_future_result_t> query_future_result = _active_connection
        ->query(boost::make_shared<cql_query_t>(cql_query_t(select_peers_expression())));
    
    if(query_future_result.timed_wait(boost::posix_time::seconds(10))) { // THINKOF(JS): do blocking wait?
        cql_future_result_t query_result = query_future_result.get();
        
        if(query_result.error.is_err()) {
            if(_log_callback) _log_callback(CQL_LOG_ERROR, query_result.error.message);
            return;
        }
        
        boost::shared_ptr<cql::cql_result_t> result = query_result.result;
        // TODO(JS) : process the result
            
    }
    else {
        if(_log_callback)
            _log_callback(CQL_LOG_ERROR, "Query timed out");
    }
}
    
void
cql_control_connection_t::conn_cassandra_event(void* sender,
                                               boost::shared_ptr<cql_event_t> event)
{
}
 
bool
cql_control_connection_t::refresh_hosts()
{
    boost::mutex::scoped_lock lock(_mutex);
    try {
        if(_is_open) {
            refresh_node_list_and_token_map();
            return true;
        }
        else return false;
    }
    catch(cql_exception& ex) {
        if(_log_callback)
            _log_callback(CQL_LOG_ERROR, ex.what());
        return false;
    }
}
    
void
cql_control_connection_t::setup_control_connection(bool refresh_only)
{
    boost::mutex::scoped_lock lock(_mutex);
    
    if(_log_callback) _log_callback(CQL_LOG_INFO,
            refresh_only ? "Refreshing control connection..."
                         : "Setting up control connection...");
    
    try {
        _timer.cancel();
        
        if(!refresh_only) setup_event_listener();
        refresh_node_list_and_token_map();
        
        _is_open = true;
        if(_log_callback) _log_callback(CQL_LOG_INFO,
                refresh_only ? "Done refreshing control connection."
                             : "Done setting up control connection.");
    }
    catch (cql_exception& ex) {
        _is_open = false;
        
        _timer.expires_from_now(_reconnection_schedule->get_delay());
        _timer.async_wait(boost::bind(
            &cql_control_connection_t::reconnection_callback, this));
        
        if(_log_callback)
            _log_callback(CQL_LOG_ERROR, ex.what());
    }
}

void
cql_control_connection_t::reconnection_callback()
{
    try
    {
        setup_control_connection();
    }
    catch (cql_exception& ex)
    {
        if(_log_callback)
            _log_callback(CQL_LOG_ERROR, ex.what());
    }
}
    
} // namespace cql