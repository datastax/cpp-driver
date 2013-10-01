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

#include "cql/common_type_definitions.hpp"
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_session_impl.hpp"
#include "cql/exceptions/cql_exception.hpp"
#include "cql/exceptions/cql_no_host_available_exception.hpp"
#include "cql/exceptions/cql_too_many_connections_per_host_exception.hpp"
#include "cql/cql_host.hpp"
#include "cql/internal/cql_trashcan.hpp"


cql::cql_session_impl_t::cql_session_impl_t(
    const cql_session_callback_info_t&      callbacks,
    boost::shared_ptr<cql_configuration_t>  configuration
    ) :
    _client_callback(callbacks.client_callback()),
    _ready_callback(callbacks.ready_callback()),
    _defunct_callback(callbacks.defunct_callback()),
    _log_callback(callbacks.log_callback()),
    _uuid(cql_uuid_t::create()),
    _configuration(configuration)
{ }

void 
cql::cql_session_impl_t::init(boost::asio::io_service& io_service) 
{
    _trashcan = boost::shared_ptr<cql_trashcan_t>(
        new cql_trashcan_t(io_service, *this));
    
    boost::shared_ptr<cql_query_plan_t> query_plan = _configuration->policies()
                   .load_balancing_policy()
                  ->new_query_plan(boost::shared_ptr<cql_query_t>());
    
    cql_stream_t stream;
    std::list<cql_endpoint_t> tried_hosts;
    
    boost::shared_ptr<cql_connection_t> conn =
            connect(query_plan, &stream, &tried_hosts);
    conn->release_stream(stream);
}
     
void
cql::cql_session_impl_t::free_connections(
    cql_connections_collection_t&   connections,
    const std::list<cql_uuid_t>&    connections_to_remove)
{
    for(std::list<cql_uuid_t>::const_iterator it = connections_to_remove.begin(); 
        it != connections_to_remove.end(); ++it)
    {
        cql_uuid_t conn_id = *it;
        boost::shared_ptr<cql_connection_t> conn;
        
        do {
            if(!connections.try_get(conn_id, &conn))
                break;
        }
        while(!connections.try_erase(conn_id, &conn));
            
        free_connection(conn);
    }
}

void 
cql::cql_session_impl_t::free_connection(
        boost::shared_ptr<cql_connection_t> connection) 
{
    if(!connection)
        return;
    
    cql_endpoint_t connection_endpoint = connection->endpoint();
    connection->close();
    
    boost::shared_ptr<connection_counter_t> counter;
    if(_connection_counters.try_get(connection_endpoint, &counter)) {
        (*counter)--;
    }
}

long
cql::cql_session_impl_t::get_max_connections_number(
    const boost::shared_ptr<cql_host_t>& host)
{
    cql_host_distance_enum  distance = host->distance(_configuration->policies());
    
    long max_connections_per_host = _configuration
        ->pooling_options()
         .max_connection_per_host(distance);
    
    return max_connections_per_host;
}

bool
cql::cql_session_impl_t::increase_connection_counter(
    const boost::shared_ptr<cql_host_t>& host)
{
    cql_endpoint_t  endpoint        = host->endpoint();
    long            max_connections = get_max_connections_number(host);
    
    boost::shared_ptr<connection_counter_t> counter(new connection_counter_t(1));
    if(!_connection_counters.try_add(endpoint, counter)) {
        bool get_result = _connection_counters.try_get(endpoint, &counter);
        assert(get_result == true);
        
        (*counter)++;
        if((long)(*counter) > max_connections) {
            (*counter)--;
            return false;
        }
    }
    
    return true;
}


bool
cql::cql_session_impl_t::decrease_connection_counter(
    const boost::shared_ptr<cql_host_t>& host)
{
    boost::shared_ptr<connection_counter_t> counter;
    
    if(_connection_counters.try_get(host->endpoint(), &counter)) {
        (*counter)--;
        return true;
    }
    
    return false;
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_session_impl_t::allocate_connection(
        const boost::shared_ptr<cql_host_t>& host) 
{
    if(!increase_connection_counter(host))
        throw cql_too_many_connections_per_host_exception();
    
    boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise(
        new cql_promise_t<cql_future_connection_t>());
    
    boost::shared_ptr<cql_connection_t> connection(_client_callback());

    connection->set_credentials(_configuration->credentials());
    connection->connect(host->endpoint(),
                        boost::bind(&cql_session_impl_t::connect_callback, this, promise, ::_1),
                        boost::bind(&cql_session_impl_t::connect_errback, this, promise, ::_1, ::_2));
    
    
    boost::shared_future<cql_future_connection_t> shared_future = promise->shared_future();
    shared_future.wait();
    
    if(shared_future.get().error.is_err()) {
        decrease_connection_counter(host);
        throw cql_exception("cannot connect to host: " + host->endpoint().to_string());
    }
    
    return connection;
}
    
boost::shared_ptr<cql::cql_connections_collection_t>
cql::cql_session_impl_t::add_to_connection_pool(
    const cql_endpoint_t& host_address) 
{
    boost::shared_ptr<cql_connections_collection_t> result, empty_collection;
    
    while(!_connection_pool.try_get(host_address, &result)) {
        if(!empty_collection) {
            empty_collection = boost::shared_ptr<cql_connections_collection_t>(
                new cql_connections_collection_t());
        }
        
        _connection_pool.try_add(host_address, empty_collection);
    }
        
    return result;
}

void 
cql::cql_session_impl_t::try_remove_connection(
    boost::shared_ptr<cql_connections_collection_t>& connections,
    const cql_uuid_t&                                connection_id) 
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
    boost::shared_ptr<cql_host_t> const&             host, 
    boost::shared_ptr<cql_connections_collection_t>& connections,
    cql_stream_t*                                    stream)
{
    const cql_pooling_options_t&    pooling_options    = _configuration->pooling_options();
    cql_host_distance_enum          distance           = host->distance(_configuration->policies());
    
    for(cql_connections_collection_t::iterator kv = connections->begin(); 
        kv != connections->end(); ++kv)
    {  
        cql_uuid_t                          conn_id = kv->first;
        boost::shared_ptr<cql_connection_t> conn    = kv->second;

        if (!conn->is_healthy()) {
            try_remove_connection(connections, conn_id);
        } 
        else if (!conn->is_busy(pooling_options.max_simultaneous_requests_per_connection_treshold(distance))) {
            *stream = conn->acquire_stream();
            if(!stream->is_invalid())
                return conn;
        } 
        else if ((long)connections->size() > pooling_options.core_connections_per_host(distance)) {
            if (conn->is_free(pooling_options.min_simultaneous_requests_per_connection_treshold(distance))) {
                if(connections->try_erase(conn_id))
                    _trashcan->put(conn);
            }
        }
    }
    
    *stream = cql_stream_t();
    return boost::shared_ptr<cql_connection_t>();
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_session_impl_t::connect(
        boost::shared_ptr<cql::cql_query_plan_t>    query_plan, 
        cql_stream_t*                               stream, 
        std::list<cql_endpoint_t>*                  tried_hosts) 
{
    assert(stream != NULL);
    assert(tried_hosts != NULL);

    while (boost::shared_ptr<cql_host_t> host = query_plan->next_host_to_query()) 
    {
        if (!host->is_considerably_up())
            continue;

        const cql_endpoint_t host_address = host->endpoint();
        
        tried_hosts->push_back(host_address);

        boost::shared_ptr<cql_connections_collection_t> connections 
            = add_to_connection_pool(host_address);
        
        boost::shared_ptr<cql_connection_t> conn =
            try_find_free_stream(host, connections, stream);
        if(conn)
            return conn;
        
        conn = _trashcan->recycle(host_address);
        if (conn && !conn->is_healthy()) {
            free_connection(conn);
            conn = boost::shared_ptr<cql_connection_t>();
        }
        
        if(!conn) {
            if( !(conn = allocate_connection(host)) )
                continue;
        }
        
        *stream = conn->acquire_stream();
        connections->try_add(conn->id(), conn);
        return conn;
    }
    
    throw cql_exception("no host is available according to load balancing policy.");
}

cql::cql_stream_t
cql::cql_session_impl_t::execute_operation(
	const boost::shared_ptr<cql_query_t>&   	query,
	cql_connection_t::cql_message_callback_t	callback,
    cql_connection_t::cql_message_errback_t		errback,
	exec_query_method_t							method)
{
	cql_stream_t stream;
    boost::shared_ptr<cql_connection_t> conn = get_connection(query, &stream);

    if (conn) {
        query->set_stream(stream);
		return ((*conn).*method)(query, callback, errback);
    }
    
    return cql_stream_t();
}

cql::cql_stream_t
cql::cql_session_impl_t::query(
    const boost::shared_ptr<cql_query_t>&    query,
    cql_connection_t::cql_message_callback_t callback,
    cql_connection_t::cql_message_errback_t  errback) 
{
	return execute_operation(query, callback, errback, &cql_connection_t::query);
}

cql::cql_stream_t
cql::cql_session_impl_t::prepare(
    const boost::shared_ptr<cql_query_t>&    query,
    cql_connection_t::cql_message_callback_t callback,
    cql_connection_t::cql_message_errback_t  errback) 
{
	return execute_operation(query, callback, errback, &cql_connection_t::prepare);
}

cql::cql_stream_t
cql::cql_session_impl_t::execute(
    const boost::shared_ptr<cql_execute_t>&  message,
    cql_connection_t::cql_message_callback_t callback,
    cql_connection_t::cql_message_errback_t  errback) 
{
	cql_stream_t stream;
    boost::shared_ptr<cql_connection_t> conn = 
        get_connection(boost::shared_ptr<cql_query_t>(), &stream);

	assert(!"Not implemented");

    /*if (conn) {

        return conn->execute(message, callback, errback);
    }*/
    
    return cql_stream_t();
}

boost::shared_future<cql::cql_future_result_t>
cql::cql_session_impl_t::query(const boost::shared_ptr<cql_query_t>& query)
{
	cql_stream_t stream;
    boost::shared_ptr<cql_connection_t> conn = get_connection(query, &stream);

    if (conn) {
        query->set_stream(stream);
		return conn->query(query);
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
cql::cql_session_impl_t::prepare(const boost::shared_ptr<cql_query_t>& query) {
    cql_stream_t stream;
    boost::shared_ptr<cql_connection_t> conn = get_connection(query, &stream);

    if (conn) {
        query->set_stream(stream);
		return conn->query(query);
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
    const boost::shared_ptr<cql_execute_t>& message) 
{
	cql_stream_t stream;
    boost::shared_ptr<cql_connection_t> conn = get_connection(boost::shared_ptr<cql_query_t>(), &stream);
    
	assert(0);
	// TODO: Not implemented - change event to class and pass stream to execute

	if (conn) {
        return conn->execute(message);
    }

    boost::promise<cql_future_result_t> promise;
    boost::shared_future<cql_future_result_t> shared_future(promise.get_future());

    cql_future_result_t future_result;
    future_result.error.library = true;
    future_result.error.message = "could not obtain viable client from the pool.";
    promise.set_value(future_result);
    return shared_future;
}

void
cql::cql_session_impl_t::close() 
{
	_trashcan->remove_all();
    
	for(cql_connection_pool_t::iterator host_it = _connection_pool.begin();
		host_it != _connection_pool.end(); ++host_it)
	{
		boost::shared_ptr<cql_connections_collection_t> connections = host_it->second;

		for(cql_connections_collection_t::iterator conn_it = connections->begin();
			conn_it != connections->end(); ++conn_it)
		{
            free_connection(conn_it->second);
		}
	}
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
    boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise,
    cql_connection_t&                                               client) 
{
    promise->set_value(cql_future_connection_t(&client));
    if (_ready_callback) {
        _ready_callback(this);
    }
}

void
cql::cql_session_impl_t::connect_errback(
    boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise,
    cql_connection_t&                                           connection,
    const cql_error_t&                                          error) 
{
    // when connection breaks this will be called two times
    // one for async read, and one for asyn write requests
    
	promise->set_value(cql_future_connection_t(&connection, error));

    if (_connect_errback) {
        _connect_errback(this, connection, error);
    }
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_session_impl_t::get_connection(
        const boost::shared_ptr<cql_query_t>&       query, 
        cql_stream_t*                               stream) 
{
    boost::shared_ptr<cql_query_plan_t> query_plan = _configuration
                                                        ->policies()
                                                         .load_balancing_policy()
                                                        ->new_query_plan(query);
    
    std::list<cql_endpoint_t> tried_hosts;
    return connect(query_plan, stream, &tried_hosts);
}

cql::cql_uuid_t
cql::cql_session_impl_t::id() const {
    return _uuid;
}