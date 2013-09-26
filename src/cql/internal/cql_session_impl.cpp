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

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>

#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_session_impl.hpp"
#include "cql/exceptions/cql_exception.hpp"
#include "cql/cql_host.hpp"

namespace cql {

cql_session_impl_t::cql_session_impl_t(
    const cql_session_callback_info_t& callbacks,
    boost::shared_ptr<cql_configuration_t>  configuration
    ) :
    _ready(false),
    _defunct(false),
    _client_callback(callbacks.client_callback()),
    _ready_callback(callbacks.ready_callback()),
    _defunct_callback(callbacks.defunct_callback()),
    _log_callback(callbacks.log_callback()),
    _reconnect_limit(0),
    _configuration(configuration) { }

void 
cql_session_impl_t::init() {
    int port = _configuration->get_protocol_options().get_port();

    // Add a client to the pool, this operation returns a future.
    std::list<std::string> contact_points =
        _configuration->get_protocol_options().get_contact_points();
    
    for(std::list<std::string>::iterator it = contact_points.begin(); 
        it != contact_points.end(); ++it) 
    {
        boost::shared_future<cql_future_connection_t> connect_future = add_client(*it, port);
        // Wait until the connection is complete, or has failed.
        connect_future.wait();

        if (connect_future.get().error.is_err()) {
            throw cql_exception("cannot connect to host.");
        }
    }
}

cql_session_impl_t::cql_session_impl_t(
    cql_session_t::cql_client_callback_t  client_callback,
    cql_session_t::cql_ready_callback_t   ready_callback,
    cql_session_t::cql_defunct_callback_t defunct_callback) :
    _ready(false),
    _defunct(false),
    _client_callback(client_callback),
    _ready_callback(ready_callback),
    _defunct_callback(defunct_callback),
    _log_callback(NULL),
    _reconnect_limit(0)
{}

cql_session_impl_t::cql_session_impl_t(
    cql_session_t::cql_client_callback_t  client_callback,
    cql_session_t::cql_ready_callback_t   ready_callback,
    cql_session_t::cql_defunct_callback_t defunct_callback,
    cql_session_t::cql_log_callback_t     log_callback) :
    _ready(false),
    _defunct(false),
    _client_callback(client_callback),
    _ready_callback(ready_callback),
    _defunct_callback(defunct_callback),
    _log_callback(log_callback),
    _reconnect_limit(0)
{}

cql_session_impl_t::cql_session_impl_t(
    cql_session_t::cql_client_callback_t  client_callback,
    cql_session_t::cql_ready_callback_t   ready_callback,
    cql_session_t::cql_defunct_callback_t defunct_callback,
    cql_session_t::cql_log_callback_t     log_callback,
    size_t                                         reconnect_limit) :
    _ready(false),
    _defunct(false),
    _client_callback(client_callback),
    _ready_callback(ready_callback),
    _defunct_callback(defunct_callback),
    _log_callback(log_callback),
    _reconnect_limit(reconnect_limit)
{}

boost::shared_future<cql_future_connection_t>
cql_session_impl_t::add_client(
    const std::string& server,
    unsigned int       port) {
    std::list<std::string> e;
    return add_client(server, port, NULL, e);
}

boost::shared_future<cql_future_connection_t>
cql_session_impl_t::add_client(
    const std::string&                      server,
    unsigned int                            port,
    cql_connection_t::cql_event_callback_t event_callback,
    const std::list<std::string>&           events) {
    std::map<std::string, std::string> credentials;
    return add_client(server, port, event_callback, events, credentials);
}


boost::shared_future<cql_future_connection_t>
cql_session_impl_t::add_client(
    const std::string&                        server,
    unsigned int                              port,
    cql_connection_t::cql_event_callback_t   event_callback,
    const std::list<std::string>&             events,
    const std::map<std::string, std::string>& credentials) 
{
    boost::mutex::scoped_lock lock(_mutex);

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

cql::cql_host_distance_enum
cql_session_impl_t::get_host_distance(boost::shared_ptr<cql::cql_host_t> host) {
    cql_host_t* p_host = host.get();
    
    cql_host_distance_enum distance = 
        _configuration
            ->get_policies()
            .get_load_balancing_policy()
            ->distance(*p_host);
    
    return distance;
}

     
void
cql_session_impl_t::free_connections(
    connections_collection_t& connections,
    const std::list<long>& connections_to_remove)
{
    for(std::list<long>::const_iterator it = connections_to_remove.begin(); 
        it != connections_to_remove.end(); ++it)
    {
        long connection_id = *it;
        
        free_connection(connections[connection_id]);
        connections.erase(connection_id);
    }
}

    
cql_session_impl_t::connections_collection_t&
cql_session_impl_t::add_to_connection_pool(
    const boost::asio::ip::address& host_address) 
{
    std::string address_string = host_address.to_string();
    connection_pool_t::iterator it = _connection_pool.find(address_string);
    
    if (it  == _connection_pool.end()) {
        _connection_pool.insert(
            std::make_pair(address_string, connections_collection_t()));
    }

    return _connection_pool[address_string];
}

boost::shared_ptr<cql_connection_t>
cql_session_impl_t::connect(
        cql_query_plan_t& query_plan, 
        int& stream_id, 
        std::list<std::string>& tried_hosts) 
{
    boost::mutex::scoped_lock lock_(_connection_pool_mtx);

    while (boost::shared_ptr<cql_host_t> host = query_plan.next_host_to_query()) 
    {
        if (!host->is_considerably_up())
            continue;

        boost::asio::ip::address host_addr = host->address();
        tried_hosts.push_back( host_addr.to_string() );

        connections_collection_t connections = add_to_connection_pool(host_addr);
        cql_host_distance_enum distance = get_host_distance(host);

        std::list<long> to_remove;

        for(connections_collection_t::iterator kv = connections.begin(); 
            kv != connections.end(); ++kv) 
        {
            boost::shared_ptr<cql_connection_t> client = kv->second;

            if (!client->is_healthy()) {
                to_remove.push_back(kv->first);
            } else if (!client->is_busy(_configuration->get_pooling_options().get_max_simultaneous_requests_per_connection_treshold(distance))) {
                stream_id = client->allocate_stream_id();
                return client;
            } else if (connections.size() > _configuration->get_pooling_options().get_core_connections_per_host(distance)) {
                if (client->is_free(_configuration->get_pooling_options().get_min_simultaneous_requests_per_connection_treshold(distance))) {
                    to_remove.push_back(kv->first);
                    trashcan_put(kv->second);
                }
            }
        }

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

        
        connections.insert(std::make_pair(client->get_id(), client));
        stream_id = client->allocate_stream_id();
        return client;
    }
    
    throw cql_exception("no host avaliable.");
}

boost::shared_ptr<cql_connection_t>
cql_session_impl_t::allocate_connection(const boost::asio::ip::address& address, cql_host_distance_enum distance) {
    return boost::shared_ptr<cql_connection_t>();
}

void 
cql_session_impl_t::trashcan_put(boost::shared_ptr<cql_connection_t> connection) {
}

boost::shared_ptr<cql_connection_t> 
cql_session_impl_t::trashcan_recycle(const boost::asio::ip::address& address) {
    return boost::shared_ptr<cql_connection_t>();
}

void 
cql_session_impl_t::free_connection(boost::shared_ptr<cql_connection_t> connection) {
}

cql_stream_id_t
cql_session_impl_t::query(
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

cql_stream_id_t
cql_session_impl_t::prepare(
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

cql_stream_id_t
cql_session_impl_t::execute(
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

boost::shared_future<cql_future_result_t>
cql_session_impl_t::query(
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

boost::shared_future<cql_future_result_t>
cql_session_impl_t::prepare(
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

boost::shared_future<cql_future_result_t>
cql_session_impl_t::execute(
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
cql_session_impl_t::defunct() {
    return _defunct;
}

bool
cql_session_impl_t::ready() {
    return _ready;
}

void
cql_session_impl_t::close() {
    boost::mutex::scoped_lock lock(_mutex);

    BOOST_FOREACH(cql_session_impl_t::client_container_t& c, _clients) {
        c.client->close();
    }
}

size_t
cql_session_impl_t::size() {
    return _clients.size();
}

bool
cql_session_impl_t::empty() {
    return _clients.empty();
}

inline void
cql_session_impl_t::log(
    cql_short_t level,
    const std::string& message) {
    if (_log_callback) {
        _log_callback(level, message);
    }
}

void
cql_session_impl_t::connect_callback(
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
cql_session_impl_t::connect_errback(
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

cql_connection_t*
cql_session_impl_t::next_client() {
    if (_ready && !_defunct && !_clients.empty()) {
        clients_collection_t::auto_type client = _clients.pop_front();
        cql_connection_t* output = client->client.get();
        _clients.push_back(client.release());
        return output;
    }
    return NULL;
}

}