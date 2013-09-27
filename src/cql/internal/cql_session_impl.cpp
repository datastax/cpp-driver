/*
  Copyright (c) 2013 Matthew Stump

  This file is part of cassandra.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include <cassert>
#include <algorithm>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>

#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_session_impl.hpp"
#include "cql/exceptions/cql_exception.hpp"
#include "cql/exceptions/cql_no_host_available_exception.hpp"
#include "cql/cql_host.hpp"


cql::cql_session_impl_t::cql_session_impl_t(
    const cql_session_callback_info_t&      callbacks,
    boost::shared_ptr<cql_configuration_t>  configuration
    ) :
    _ready(false),
    _defunct(false),
    _client_callback(callbacks.client_callback()),
    _ready_callback(callbacks.ready_callback()),
    _defunct_callback(callbacks.defunct_callback()),
    _log_callback(callbacks.log_callback()),
    _reconnect_limit(0),
    _uuid(cql_uuid_t::create()),
    _configuration(configuration)
    
{ }

void 
cql::cql_session_impl_t::init(boost::asio::io_service& io_service) {
//    int port = _configuration->get_protocol_options().get_port();
//
//    // Add a client to the pool, this operation returns a future.
//    std::list<std::string> contact_points =
//        _configuration->get_protocol_options().get_contact_points();
//    
//    for(std::list<std::string>::iterator it = contact_points.begin(); 
//        it != contact_points.end(); ++it) 
//    {
//        boost::shared_future<cql_future_connection_t> connect_future = add_client(*it, port);
//        // Wait until the connection is complete, or has failed.
//        connect_future.wait();
//
//        if (connect_future.get().error.is_err()) {
//            throw cql_exception("cannot connect to host.");
//        }
//    }
    
    boost::shared_ptr<cql_query_plan_t> query_plan = _configuration->policies()
                   .load_balancing_policy()
                  ->new_query_plan(boost::shared_ptr<cql_query_t>());
    
    boost::shared_ptr<cql_host_t> host = query_plan->next_host_to_query();
    if(!host)
        throw cql_no_host_available_exception(
            "No host is available according to load balancing policy.");
    
    int stream_id;
    std::list<cql_endpoint_t> tried_hosts;
    
    boost::shared_ptr<cql_connection_t> conn =
            connect(query_plan, &stream_id, &tried_hosts);
    conn->release_stream_id(stream_id);
    
    _trashcan_timer = boost::shared_ptr<boost::asio::deadline_timer>(
        new boost::asio::deadline_timer(io_service));
}

boost::shared_future<cql::cql_future_connection_t>
cql::cql_session_impl_t::add_client(
    const std::string& server,
    unsigned int       port) {
    std::list<std::string> e;
    return add_client(server, port, NULL, e);
}

boost::shared_future<cql::cql_future_connection_t>
cql::cql_session_impl_t::add_client(
    const std::string&                      server,
    unsigned int                            port,
    cql_connection_t::cql_event_callback_t event_callback,
    const std::list<std::string>&           events) {
    std::map<std::string, std::string> credentials;
    return add_client(server, port, event_callback, events, credentials);
}


boost::shared_future<cql::cql_future_connection_t>
cql::cql_session_impl_t::add_client(
    const std::string&                        server,
    unsigned int                              port,
    cql_connection_t::cql_event_callback_t   event_callback,
    const std::list<std::string>&             events,
    const std::map<std::string, std::string>& credentials) 
{
    boost::shared_ptr<boost::promise<cql_future_connection_t> > promise(
        new boost::promise<cql_future_connection_t>());
        
    boost::shared_future<cql_future_connection_t> shared_future(promise->get_future());

    std::auto_ptr<cql_session_impl_t::client_container_t> client_container(
        new cql_session_impl_t::client_container_t(_client_callback()));

    client_container->client->events(event_callback, events);
    client_container->client->credentials(credentials);
    client_container->client->connect(server,
                                      port,
                                      boost::bind(&cql_session_impl_t::connect_callback, this, promise, ::_1),
                                      boost::bind(&cql_session_impl_t::connect_errback, this, promise, ::_1, ::_2));
    _clients.push_back(client_container.release());
    return shared_future;
}
     
void
cql::cql_session_impl_t::free_connections(
    connections_collection_t&       connections,
    const std::list<cql_uuid_t>&    connections_to_remove)
{
    for(std::list<cql_uuid_t>::const_iterator it = connections_to_remove.begin(); 
        it != connections_to_remove.end(); ++it)
    {
        cql_uuid_t conn_id = *it;
        boost::shared_ptr<cql_connection_t> conn;
        
        if(connections.try_get(conn_id, &conn)) {
            while(!connections.try_erase(conn_id))
                ;
            
            free_connection(conn);
        }
    }
}

void 
cql::cql_session_impl_t::free_connection(boost::shared_ptr<cql_connection_t> connection) {
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_session_impl_t::allocate_connection(const boost::asio::ip::address& address, cql_host_distance_enum distance) {
    return boost::shared_ptr<cql_connection_t>();
}

void
cql::cql_session_impl_t::trashcan_timer_timeout(
    const boost::system::error_code& error)
{
    if(!error) {
        trashcan_cleanup();
    }
}

void
cql::cql_session_impl_t::trashcan_cleanup() {
    for(connection_pool_t::iterator it = _trashcan.begin();
        it != _trashcan.end(); ++it)
    {
        boost::shared_ptr<connections_collection_t> connections = it->second;
        trashcan_remove_empty_connections
    }
}

void 
cql::cql_session_impl_t::trashcan_put(
        boost::shared_ptr<connections_collection_t>& connections,
        const cql_uuid_t&                           connection_id) 
{
    boost::shared_ptr<cql_connection_t> conn;
    
    if(connections->try_erase(connection_id, &conn)) {
        trashcan_put(conn);
    }
}

void
cql::cql_session_impl_t::trashcan_put(boost::shared_ptr<cql_connection_t>& connection) {
    
}

boost::shared_ptr<cql::cql_connection_t> 
cql::cql_session_impl_t::trashcan_recycle(const boost::asio::ip::address& address) {
    return boost::shared_ptr<cql_connection_t>();
}
    
boost::shared_ptr<cql::cql_session_impl_t::connections_collection_t>
cql::cql_session_impl_t::add_to_connection_pool(
    const boost::asio::ip::address& host_address) 
{
    boost::shared_ptr<connections_collection_t> result, empty_collection;
    
    while(!_connection_pool.try_get(host_address, &result)) {
        if(!empty_collection) {
            empty_collection = boost::shared_ptr<connections_collection_t>(
                new connections_collection_t());
        }
        
        _connection_pool.try_add(host_address, empty_collection);
    }
        
    return result;
}

void 
cql::cql_session_impl_t::try_remove_connection(
    boost::shared_ptr<connections_collection_t>& connections,
    const cql_uuid_t& connection_id) 
{
    // TODO: How we can guarantee that any other thread is not using
    // this connection object ?
    
    boost::shared_ptr<cql_connection_t> conn;
    
    if(connections->try_erase(connection_id, &conn)) {
        free_connection(conn);
    }
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_session_impl_t::try_find_free_stream(
    boost::shared_ptr<cql_host_t>               host, 
    boost::shared_ptr<connections_collection_t> connections,
    int*                                        stream_id)
{
    cql_pooling_options_t& pooling_options  = _configuration->pooling_options();
    cql_host_distance_enum distance         = get_host_distance(host);
    
    for(connections_collection_t::iterator kv = connections->begin(); 
        kv != connections->end(); ++kv)
    {  
        cql_uuid_t                          conn_id = kv->first;
        boost::shared_ptr<cql_connection_t> conn    = kv->second;

        if (!conn->is_healthy()) {
            try_remove_connection(conn_id);
        } 
        else if (!conn->is_busy(pooling_options.max_simultaneous_requests_per_connection_treshold(distance))) {
            *stream_id = conn->allocate_stream_id();
            if(*stream_id != (-1))
                return conn;
        } 
        else if (connections->size() > pooling_options.core_connections_per_host(distance)) {
            if (conn->is_free(pooling_options.min_simultaneous_requests_per_connection_treshold(distance))) {
                trashcan_put(connections, conn_id);
            }
        }
    }
    
    *stream_id = (-1);
    return boost::shared_ptr<cql_connection_t>();
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_session_impl_t::connect(
        boost::shared_ptr<cql::cql_query_plan_t>    query_plan, 
        int*                                        stream_id, 
        std::list<boost::asio::ip::address>*        tried_hosts) 
{
    assert(stream_id != NULL);
    assert(tried_hosts != NULL);

    while (boost::shared_ptr<cql_host_t> host = query_plan->next_host_to_query()) 
    {
        if (!host->is_considerably_up())
            continue;

        boost::asio::ip::address host_addr = host->address();
        tried_hosts->push_back( host_addr );

        boost::shared_ptr<connections_collection_t> connections 
            = add_to_connection_pool(host_addr);
        
        

        free_connections(connections, to_remove);

        boost::shared_ptr<cql_connection_t> client = trashcan_recycle(host_addr);
        
        if (client && !client->is_healthy()) {
            free_connection(client);
            client = boost::shared_ptr<cql_connection_t>();
        }
        
        if(!client) {
            client = allocate_connection(host_addr, distance);
        }
        
        if(!client)
            continue;

        
        connections.insert(std::make_pair(client->id(), client));
        *stream_id = client->allocate_stream_id();
        return client;
    }
    
    throw cql_exception("no host avaliable.");
}



cql::cql_stream_id_t
cql::cql_session_impl_t::query(
    const std::string&                   query,
    cql_consistency_enum                 consistency,
    cql_connection_t::cql_message_callback_t callback,
    cql_connection_t::cql_message_errback_t  errback) 
{
    boost::mutex::scoped_lock lock(_mutex);

    cql_connection_t* client = next_client();
    if (client) {
        return client->query(query, consistency, callback, errback);
    }
    
    return 0;
}

cql::cql_stream_id_t
cql::cql_session_impl_t::prepare(
    const std::string&                        query,
    cql_connection_t::cql_message_callback_t callback,
    cql_connection_t::cql_message_errback_t  errback) {
    boost::mutex::scoped_lock lock(_mutex);

    cql_connection_t* client = next_client();
    if (client) {
        return client->prepare(query, callback, errback);
    }
    return 0;
}

cql::cql_stream_id_t
cql::cql_session_impl_t::execute(
    cql_execute_t*                       message,
    cql_connection_t::cql_message_callback_t callback,
    cql_connection_t::cql_message_errback_t  errback) {
    boost::mutex::scoped_lock lock(_mutex);

    cql_connection_t* client = next_client();
    if (client) {
        return client->execute(message, callback, errback);
    }
    return 0;
}

boost::shared_future<cql::cql_future_result_t>
cql::cql_session_impl_t::query(
    const std::string&		 query,
    cql_consistency_enum     consistency) {
    boost::mutex::scoped_lock lock(_mutex);

    cql_connection_t* client = next_client();
    if (client) {
        return client->query(query, consistency);
    }

    boost::promise<cql_future_result_t> promise;
    boost::shared_future<cql_future_result_t> shared_future(promise.get_future());

    cql_future_result_t future_result;
    future_result.error.library = true;
    future_result.error.message = "could not obtain viable client from the pool.";
    promise.set_value(future_result);
    return shared_future;
}

boost::shared_future<cql::cql_future_result_t>
cql::cql_session_impl_t::prepare(
    const std::string& query) {
    boost::mutex::scoped_lock lock(_mutex);

    cql_connection_t* client = next_client();
    if (client) {
        return client->prepare(query);
    }

    boost::promise<cql_future_result_t> promise;
    boost::shared_future<cql_future_result_t> shared_future(promise.get_future());

    cql_future_result_t future_result;
    future_result.error.library = true;
    future_result.error.message = "could not obtain viable client from the pool.";
    promise.set_value(future_result);
    return shared_future;
}

boost::shared_future<cql::cql_future_result_t>
cql::cql_session_impl_t::execute(
    cql_execute_t* message) {
    boost::mutex::scoped_lock lock(_mutex);

    cql_connection_t* client = next_client();
    if (client) {
        return client->execute(message);
    }

    boost::promise<cql_future_result_t> promise;
    boost::shared_future<cql_future_result_t> shared_future(promise.get_future());

    cql_future_result_t future_result;
    future_result.error.library = true;
    future_result.error.message = "could not obtain viable client from the pool.";
    promise.set_value(future_result);
    return shared_future;
}

bool
cql::cql_session_impl_t::defunct() {
    return _defunct;
}

bool
cql::cql_session_impl_t::ready() {
    return _ready;
}

void
cql::cql_session_impl_t::close() {
    boost::mutex::scoped_lock lock(_mutex);

    BOOST_FOREACH(cql_session_impl_t::client_container_t& c, _clients) {
        c.client->close();
    }
}

size_t
cql::cql_session_impl_t::size() {
    return _clients.size();
}

bool
cql::cql_session_impl_t::empty() {
    return _clients.empty();
}

inline void
cql::cql_session_impl_t::log(
    cql_short_t level,
    const std::string& message) {
    if (_log_callback) {
        _log_callback(level, message);
    }
}

void
cql::cql_session_impl_t::connect_callback(
    boost::shared_ptr<boost::promise<cql_future_connection_t> > promise,
    cql_connection_t&                                               client) {
    _defunct = false;

    _ready = true;
    promise->set_value(cql_future_connection_t(&client));
    if (_ready_callback) {
        _ready_callback(this);
    }
}

void
cql::cql_session_impl_t::connect_errback(
    boost::shared_ptr<boost::promise<cql_future_connection_t> > promise,
    cql_connection_t&                                               client,
    const cql_error_t&                                               error) {
    boost::mutex::scoped_lock lock(_mutex);

    clients_collection_t::iterator pos = _clients.begin();
    for (; pos != _clients.end(); ++pos) {
        cql_session_impl_t::client_container_t& client_container = (*pos);

        if (client_container.client.get() == &client) {

            if (++client_container.errors > _reconnect_limit) {
                clients_collection_t::auto_type client_ptr = _clients.release(pos);
                log(CQL_LOG_ERROR, "client has reached error threshold, removing from pool");
                promise->set_value(cql_future_connection_t(&client, error));

                if (_connect_errback) {
                    _connect_errback(this, client, error);
                }

                if (_clients.empty()) {
                    log(CQL_LOG_ERROR, "no clients left in pool");
                    _ready = false;
                    _defunct = true;
                    if (_defunct_callback) {
                        _defunct_callback(this);
                    }
                }
            } else {
                log(CQL_LOG_INFO, "attempting to reconnect client");
                client_container.client->reconnect();
            }
            break;
        }
    }
}

cql::cql_connection_t*
cql::cql_session_impl_t::next_client() {
    if (_ready && !_defunct && !_clients.empty()) {
        clients_collection_t::auto_type client = _clients.pop_front();
        cql_connection_t* output = client->client.get();
        _clients.push_back(client.release());
        return output;
    }
    return NULL;
}

cql::cql_uuid_t
cql::cql_session_impl_t::id() const {
    return _uuid;
}