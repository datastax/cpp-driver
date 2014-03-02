/*
 *      Copyright (C) 2013 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include <exception>
#include <vector>
#include <map>
#include <string>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/make_shared.hpp>
#include <boost/tuple/tuple.hpp>

#include "cql/internal/cql_control_connection.hpp"
#include "cql/internal/cql_connection_impl.hpp"
#include "cql/internal/cql_cluster_impl.hpp"
#include "cql/exceptions/cql_exception.hpp"
#include "cql/cql_set.hpp"
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_socket.hpp"
#include "cql/internal/cql_serialization.hpp"

namespace cql {

cql_control_connection_t::cql_control_connection_t(
    cql_cluster_t&                         cluster,
    boost::asio::io_service&               io_service,
    boost::shared_ptr<cql_configuration_t> configuration) :
    _is_open(false),
    _cluster(cluster),
    _io_service(io_service),
    _configuration(configuration),
    _timer(io_service),
    _log_callback(_configuration->client_options().log_callback())
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
        new cql_exponential_reconnection_policy_t(
            boost::posix_time::seconds(2),
            boost::posix_time::minutes(5)));

    _reconnection_schedule = _reconnection_policy_tmp->new_schedule();

    _session = boost::shared_ptr<cql_session_impl_t>(
                    new cql_session_impl_t(session_callbacks, configuration));
}

void
cql_control_connection_t::init()
{
    if (!_is_open) {
        _session->init(_io_service);
        setup_control_connection();
    }
}

void
cql_control_connection_t::shutdown(int timeout_ms)
{
    boost::mutex::scoped_lock lock(_mutex);
    // NOTE: seems that session does not support timeout?
    _is_open = false;
    _timer.cancel();
    boost::static_pointer_cast<cql_session_t>(_session)->close();
}

cql_control_connection_t::~cql_control_connection_t()
{
    try {
        shutdown();
    }
    catch(...) {
        if (_log_callback) {
            _log_callback(CQL_LOG_ERROR, "Error while shutting down control connection");
        }
    }
}

void
cql_control_connection_t::metadata_hosts_event(
    void*                          sender,
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

    if (_active_connection) {
        _active_connection->close();
    }
    _active_connection = _session->connect(query_plan, &stream, &tried_hosts);
    
    std::list<std::string> events;
    events.push_back(CQL_EVENT_TOPOLOGY_CHANGE);
    events.push_back(CQL_EVENT_STATUS_CHANGE);
    events.push_back(CQL_EVENT_SCHEMA_CHANGE);

    _active_connection->set_events(
        boost::bind(&cql_control_connection_t::conn_cassandra_event, this, _1, _2),
        events);
    
    boost::shared_ptr<cql_connection_impl_t<cql_socket_t> > active_connection_impl
        = boost::dynamic_pointer_cast<cql_connection_impl_t<cql_socket_t> >(_active_connection);
    if (active_connection_impl) {
        active_connection_impl->events_register();
    }
    else if (_log_callback) {
        _log_callback(CQL_LOG_ERROR, "Unreachable code reached while setting up event listener for control connection");
    }
}

cql_host_t::ip_address_t
cql_control_connection_t::make_ipv4_address_from_bytes(
    const cql::cql_byte_t* data)
{
    return cql_host_t::ip_address_t::from_string(
        decode_ipv4_from_bytes(data));
}

void
cql_control_connection_t::refresh_node_list_and_token_map()
{
    if (_log_callback) {
        _log_callback(CQL_LOG_INFO, "Refreshing node list and token map...");
    }

    boost::shared_ptr<cql_metadata_t> clusters_metadata = _cluster.metadata();
    std::string partitioner;

    // (ip_address, data_center, rack, tokens):
    std::vector<cql_host_t::ip_address_t>    found_hosts;
    std::vector<std::string>                 data_centers,
                                             racks;
    std::vector<std::map<std::string,bool> > all_tokens;

    std::map<cql_host_t::ip_address_t, std::map<std::string,bool> > token_map;

    {// SELECT PEERS
        cql_stream_t stream = _active_connection->acquire_stream();
        boost::shared_ptr<cql_query_t> query = boost::make_shared<cql_query_t>(                                                                               select_peers_expression());
        query->set_stream(stream);
        boost::shared_future<cql_future_result_t> query_future_result =
            _active_connection->query(query);
        
        if (query_future_result.timed_wait(boost::posix_time::seconds(10))) {
            cql_future_result_t query_result = query_future_result.get();

            if (query_result.error.is_err()) {
                if (_log_callback) {
                    _log_callback(CQL_LOG_ERROR, query_result.error.message);
                }
                return;
            }

            boost::shared_ptr<cql::cql_result_t> result = query_result.result;
            while (result->next())
            {
                cql_host_t::ip_address_t peer_address;
                bool output = false;

                if (!(result->is_null("rpc_address", output)) && !output)
                {
                    cql::cql_byte_t* data = NULL;
                    cql::cql_int_t size = 0;
                    result->get_data("rpc_address", &data, size);
                    if (size == 4)
                        peer_address = make_ipv4_address_from_bytes(data);
                }
                if (peer_address == ::boost::asio::ip::address())
                {
                    if (!(result->is_null("peer", output)) && !output)
                    {
                        cql::cql_byte_t* data = NULL;
                        cql::cql_int_t size = 0;
                        result->get_data("peer", &data, size);
                        if (size == 4) {
                            peer_address = make_ipv4_address_from_bytes(data);
                        }
                    }
                    else if (_log_callback) {
                        _log_callback(CQL_LOG_ERROR, "No rpc_address found for host in peers system table.");
                    }
                }
                if (!(peer_address == ::boost::asio::ip::address()))
                {
                    std::string peer_data_center, peer_rack;
                    cql_set_t* peer_tokens_set = NULL;

                    result->get_string("data_center", peer_data_center);
                    result->get_string("rack", peer_rack);
                    result->get_set("tokens", &peer_tokens_set);

                    found_hosts.push_back(peer_address);
                    data_centers.push_back(peer_data_center);
                    racks.push_back(peer_rack);

                    all_tokens.push_back(std::map<std::string,bool>());
					if(peer_tokens_set!=NULL)
					{
						for (size_t i = 0u; i < peer_tokens_set->size(); ++i) {
							std::string single_peers_token;
							peer_tokens_set->get_string(i, single_peers_token);
							all_tokens.back()[single_peers_token] = true;
						}
						
						delete peer_tokens_set;
					}
                }
            }
        }
        else {
            if (_log_callback) {
                _log_callback(CQL_LOG_ERROR, "Select-peers query timed out");
            }
            return;
        }
    }
    {// SELECT LOCAL
        cql_stream_t stream = _active_connection->acquire_stream();
        boost::shared_ptr<cql_query_t> query = boost::make_shared<cql_query_t>(                                                                               select_local_expression());
        query->set_stream(stream);
        boost::shared_future<cql_future_result_t> query_future_result =
            _active_connection->query(query);

        if (query_future_result.timed_wait(boost::posix_time::seconds(10))) {
            cql_future_result_t query_result = query_future_result.get();

            if (query_result.error.is_err()) {
                if (_log_callback) {
                    _log_callback(CQL_LOG_ERROR, query_result.error.message);
                }
                return;
            }

            boost::shared_ptr<cql_host_t> local_host = clusters_metadata->get_host(_active_connection->endpoint());
            boost::shared_ptr<cql::cql_result_t> result = query_result.result;
            if (result->next())
            {
                std::string cluster_name;
                if (result->get_string("cluster_name", cluster_name)) {
                    clusters_metadata->set_cluster_name(cluster_name);
                }
                if (local_host) {
                    std::string local_data_center, local_rack;
                    result->get_string("data_center", local_data_center);
                    result->get_string("rack", local_rack);
                    local_host->set_location_info(local_data_center, local_rack);

                    result->get_string("partitioner", partitioner);

                    cql_set_t* local_tokens_set;
                    result->get_set("tokens", &local_tokens_set);

                    if (partitioner != "" && local_tokens_set->size() > 0u) {
                        std::map<std::string,bool> local_tokens_map;
                        for (size_t i = 0u; i < local_tokens_set->size(); ++i) {
                            std::string single_local_token;
                            local_tokens_set->get_string(i, single_local_token);
                            local_tokens_map[single_local_token] = true;
                        }

                        if (token_map.find(local_host->address()) == token_map.end()) {
                            token_map[local_host->address()] = std::map<std::string,bool>();
                        }
                        token_map[local_host->address()].insert(local_tokens_map.begin(),
                                                                local_tokens_map.end());
                    }

                    delete local_tokens_set;
                }
            }

        }
        else {
            if (_log_callback) {
                _log_callback(CQL_LOG_ERROR, "Select-local query timed out");
            }
            return;
        }
    }

    // Now we will determine the port number, assuming there is one (common for whole cluster).
    // Otherwise, we use default 9042.
    int port_number;
    {
        std::vector<cql_endpoint_t> current_endpoints;
        clusters_metadata->get_endpoints(current_endpoints);
        port_number = current_endpoints.empty() ? cql_builder_t::DEFAULT_PORT
        : current_endpoints[0].port();
    }

    for (size_t i = 0u; i < found_hosts.size(); ++i)
    {
        cql_host_t::ip_address_t host_address = found_hosts[i];

        cql_endpoint_t peer_endpoint(host_address, port_number);
        boost::shared_ptr<cql_host_t> host = clusters_metadata->get_host(peer_endpoint);
        if (!host) host = clusters_metadata->add_host(peer_endpoint);

        host->set_location_info(data_centers[i], racks[i]);

        if (partitioner != "") {
            for (std::map<std::string,bool>::iterator
                    one_token  = all_tokens[i].begin();
                    one_token != all_tokens[i].end(); ++one_token)
            {
                token_map[host_address][one_token->first] = true;
            }
        }
    }
    {
        // Now we will remove dead hosts.
        std::map<cql_host_t::ip_address_t, bool> hosts_addresses_map;
        for (size_t i = 0; i < found_hosts.size(); ++i) {
            hosts_addresses_map[found_hosts[i]] = true;
        }

        std::vector<boost::shared_ptr<cql_host_t> > clusters_hosts;
        clusters_metadata->get_hosts(clusters_hosts);

        for (std::vector<boost::shared_ptr<cql_host_t> >::iterator
                host  = clusters_hosts.begin();
                host != clusters_hosts.end(); ++host)
        {
            if (hosts_addresses_map.find((*host)->address()) == hosts_addresses_map.end()
                && (*host)->address() != _active_connection->endpoint().address())
            {
                clusters_metadata->remove_host((*host)->endpoint());
            }
        }
    }

    //TODO(JS) : clusters_metadata->rebuildTokenMap(...)

    if (_log_callback) {
        _log_callback(CQL_LOG_INFO, "NodeList and TokenMap successfully refreshed.");
    }
}

void
cql_control_connection_t::conn_cassandra_event(
    cql_connection_t& connection,
    cql_event_t* event)
{
    switch(event->event_type())
    {
        case CQL_EVENT_TYPE_TOPOLOGY:
            if (event->topology_change() == CQL_EVENT_TOPOLOGY_ADD_NODE) {
                setup_control_connection(true);
                cql_host_t::ip_address_t ip_address
                    = cql_host_t::ip_address_t::from_string(event->ip());
                _cluster.metadata()->add_host(cql_endpoint_t(ip_address, event->port())); // Just in case.
            }
            else if (event->topology_change() == CQL_EVENT_TOPOLOGY_REMOVE_NODE) {
                cql_host_t::ip_address_t ip_address
                    = cql_host_t::ip_address_t::from_string(event->ip());
                _cluster.metadata()->remove_host(cql_endpoint_t(ip_address, event->port()));
                
                bool refresh_only = false;
                if (_active_connection) {
                    refresh_only = !(_active_connection->endpoint().address() == ip_address);
                }

                setup_control_connection(refresh_only);
            }
            break;
            
        case CQL_EVENT_TYPE_STATUS:
            if (event->status_change() == CQL_EVENT_STATUS_UP) {
                cql_host_t::ip_address_t ip_address
                    = cql_host_t::ip_address_t::from_string(event->ip());
                cql_endpoint_t endpoint(ip_address, event->port());
                
                _cluster.metadata()->bring_up_host(endpoint);
                
                boost::shared_ptr<cql_host_state_changed_info_t> event_info =
                    cql_host_state_changed_info_t::make_instance(
                        cql_host_state_changed_info_t::NEW_HOST_STATE_UP,
                        endpoint);
                _cluster.metadata()->_host_state_changed(event_info);
            }
            else if (event->status_change() == CQL_EVENT_STATUS_DOWN) {
                cql_host_t::ip_address_t ip_address
                    = cql_host_t::ip_address_t::from_string(event->ip());
                cql_endpoint_t endpoint(ip_address, event->port());
                
                _cluster.metadata()->set_down_host(endpoint);

                boost::shared_ptr<cql_host_state_changed_info_t> event_info =
                    cql_host_state_changed_info_t::make_instance(
                        cql_host_state_changed_info_t::NEW_HOST_STATE_DOWN,
                        endpoint);
                _cluster.metadata()->_host_state_changed(event_info);
            }
            
            break;
        case CQL_EVENT_TYPE_SCHEMA:
            
            break;
        case CQL_EVENT_TYPE_UNKNOWN:
            break;
    }
    
    // We have the ownership of event.
    delete event;
}

bool
cql_control_connection_t::refresh_hosts()
{
    boost::mutex::scoped_lock lock(_mutex);
    try {
        if (_is_open) {
            refresh_node_list_and_token_map();
            return true;
        }
        else return false;
    }
    catch(cql_exception& ex) {
        if (_log_callback) {
            _log_callback(CQL_LOG_ERROR, ex.what());
        }
        return false;
    }
}

void
cql_control_connection_t::setup_control_connection(
    bool refresh_only)
{
    boost::mutex::scoped_lock lock(_mutex);

    if (_log_callback) {
        _log_callback(
            CQL_LOG_INFO,
            refresh_only ? "Refreshing control connection..."
            : "Setting up control connection...");
    }

    try {
        _timer.cancel();

        if (!refresh_only) {
            setup_event_listener();
        }
        refresh_node_list_and_token_map();

        _is_open = true;
        if (_log_callback) {
            _log_callback(
                CQL_LOG_INFO,
                refresh_only ? "Done refreshing control connection."
                : "Done setting up control connection.");
        }
    }
    catch (cql_exception& ex) {
        _is_open = false;

        _timer.expires_from_now(_reconnection_schedule->get_delay());
        _timer.async_wait(boost::bind(
            &cql_control_connection_t::reconnection_callback,
            this,
            boost::asio::placeholders::error));

        if (_log_callback) {
            _log_callback(CQL_LOG_ERROR, ex.what());
        }
    }
}

void
cql_control_connection_t::reconnection_callback(
    const boost::system::error_code& err)
{
    if (!err) {
        try {
            setup_control_connection();
        }
        catch (cql_exception& ex) {
            if (_log_callback) {
                _log_callback(CQL_LOG_ERROR, ex.what());
            }
        }
    }
}

} // namespace cql
