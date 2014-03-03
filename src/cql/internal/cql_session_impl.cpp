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
#include "cql/exceptions/cql_connection_allocation_error.hpp"
#include "cql/cql_host.hpp"
#include "cql/internal/cql_trashcan.hpp"
#include "cql/internal/cql_socket.hpp"
#include "cql/internal/cql_connection_impl.hpp"


cql::cql_session_impl_t::cql_session_impl_t(
    const cql_session_callback_info_t&      callbacks,
    boost::shared_ptr<cql_configuration_t>  configuration) :
    _client_callback(callbacks.client_callback()),
    _ready_callback(callbacks.ready_callback()),
    _defunct_callback(callbacks.defunct_callback()),
    _log_callback(callbacks.log_callback()),
    _uuid(cql_uuid_t::create()),
    _configuration(configuration),
    _Iam_closed(false)
{}

cql::cql_session_impl_t::~cql_session_impl_t()
{
	close();
}

void
cql::cql_session_impl_t::init(
    boost::asio::io_service& io_service)
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
cql::cql_session_impl_t::free_connection(
    boost::shared_ptr<cql_connection_t> connection)
{
    if (!connection) {
        return;
    }
    connection->close();

	{
        boost::recursive_mutex::scoped_lock lock(_mutex);
		connections_counter_t::iterator it = _connection_counters.find(connection->endpoint());
		if (it != _connection_counters.end()) {
			(*it->second)--;
		}
	}
}

long
cql::cql_session_impl_t::get_max_connections_number(
    const boost::shared_ptr<cql_host_t>& host)
{
    cql_host_distance_enum distance = host->distance(_configuration->policies());

    long max_connections_per_host = _configuration
        ->pooling_options()
        .max_connection_per_host(distance);

    return max_connections_per_host;
}

void
cql::cql_session_impl_t::set_keyspace(const std::string& new_keyspace)
{
    boost::recursive_mutex::scoped_lock lock(_mutex);
    _keyspace_name = new_keyspace;

    for(connection_pool_t::iterator I  = _connection_pool.begin();
        I != _connection_pool.end(); ++I)
    {
        cql_connections_collection_t& conn_collection_ptr = *(I->second);
        for(cql_connections_collection_t::iterator
                J  = conn_collection_ptr.begin();
                J != conn_collection_ptr.end(); ++J)
        {
            J->second->set_keyspace(new_keyspace);
        }
    }
    
    for(connection_pool_t::iterator I  = _trashcan->_trashcan.begin();
        I != _trashcan->_trashcan.end(); ++I)
    {
        cql_connections_collection_t& conn_collection_ptr = *(I->second);
        for(cql_connections_collection_t::iterator
                J  = conn_collection_ptr.begin();
                J != conn_collection_ptr.end(); ++J)
        {
            J->second->set_keyspace(new_keyspace);
        }
    }
}

void
cql::cql_session_impl_t::set_prepare_statement(
    const std::vector<cql_byte_t>& query_id,
    const std::string& query_text)
{
    boost::recursive_mutex::scoped_lock lock(_mutex);
    _prepare_statements[query_id] = query_text;
    
    for(connection_pool_t::iterator I  = _connection_pool.begin();
        I != _connection_pool.end(); ++I)
    {
        cql_connections_collection_t& conn_collection_ptr = *(I->second);
        for(cql_connections_collection_t::iterator
            J  = conn_collection_ptr.begin();
            J != conn_collection_ptr.end(); ++J)
        {
            J->second->set_prepared_statement(query_id);
        }
    }
    
    for(connection_pool_t::iterator I  = _trashcan->_trashcan.begin();
        I != _trashcan->_trashcan.end(); ++I)
    {
        cql_connections_collection_t& conn_collection_ptr = *(I->second);
        for(cql_connections_collection_t::iterator
            J  = conn_collection_ptr.begin();
            J != conn_collection_ptr.end(); ++J)
        {
            J->second->set_prepared_statement(query_id);
        }
    }
}


bool
cql::cql_session_impl_t::increase_connection_counter(
    const boost::shared_ptr<cql_host_t>& host)
{
    boost::recursive_mutex::scoped_lock lock(_mutex);
    cql_endpoint_t            endpoint        = host->endpoint();
    long                      max_connections = get_max_connections_number(host);

    connections_counter_t::iterator it = _connection_counters.find(endpoint);
    if (it == _connection_counters.end()) {
        boost::shared_ptr<connection_counter_t> counter(new connection_counter_t(1));
        _connection_counters[endpoint] = counter;
    }
    else {
        (*it->second)++;
        if ((long)(*it->second) > max_connections) {
            (*it->second)--;
            return false;
        }
    }

    return true;
}


bool
cql::cql_session_impl_t::decrease_connection_counter(
    const boost::shared_ptr<cql_host_t>& host)
{
    boost::recursive_mutex::scoped_lock lock(_mutex);
    connections_counter_t::iterator it = _connection_counters.find(host->endpoint());

    if (it != _connection_counters.end()) {
        (*it->second)--;
        return true;
    }

    return false;
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_session_impl_t::allocate_connection(
    const boost::shared_ptr<cql_host_t>& host)
{
    if (!increase_connection_counter(host)) {
        throw cql_too_many_connections_per_host_exception();
    }

    boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise(
        new cql_promise_t<cql_future_connection_t>());

    boost::shared_ptr<cql_connection_t> connection(_client_callback());

    connection->set_credentials(_configuration->credentials());
    connection->connect(host->endpoint(),
                        boost::bind(&cql_session_impl_t::connect_callback, this, promise, ::_1),
                        boost::bind(&cql_session_impl_t::connect_errback, this, promise, ::_1, ::_2));
    connection->set_keyspace(_keyspace_name);

    boost::shared_future<cql_future_connection_t> shared_future = promise->shared_future();
    shared_future.wait();

    if (shared_future.get().error.is_err()) {
        decrease_connection_counter(host);
        throw cql_connection_allocation_error(
                ("Error when connecting to host: " + host->endpoint().to_string()).c_str());
        connection.reset();
    }

    return connection;
}

cql::cql_session_impl_t::cql_connections_collection_t*
cql::cql_session_impl_t::add_to_connection_pool(
    cql_endpoint_t& endpoint)
{
    boost::recursive_mutex::scoped_lock lock(_mutex);
    connection_pool_t::iterator it = _connection_pool.find(endpoint);
    if (it == _connection_pool.end()) {
        it = _connection_pool.insert(endpoint, new cql_connections_collection_t()).first;
    }

    return it->second;
}

cql::cql_session_impl_t::cql_connections_collection_t::iterator
cql::cql_session_impl_t::try_remove_connection(
    cql_connections_collection_t* const connections,
    const cql_uuid_t&                   connection_id)
{
    // TODO: How we can guarantee that any other thread is not using
    // this connection object ?

    cql_connections_collection_t::iterator tmp, iter = connections->find(connection_id);
    tmp = iter;
    if (iter != connections->end()) {
        ++tmp;
        free_connection(iter->second);
        connections->erase(iter);
    }
    return tmp;
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_session_impl_t::try_find_free_stream(
    boost::shared_ptr<cql_host_t> const&    host,
    cql_connections_collection_t* const     connections,
    cql_stream_t*                           stream)
{
    boost::recursive_mutex::scoped_lock lock(_mutex);
    
    const cql_pooling_options_t& pooling_options = _configuration->pooling_options();
    cql_host_distance_enum       distance        = host->distance(_configuration->policies());

    for (cql_connections_collection_t::iterator kv = connections->begin();
         kv != connections->end(); /* No increment */)
    {
        cql_uuid_t                          conn_id = kv->first;
        boost::shared_ptr<cql_connection_t> conn    = kv->second;

        if (!conn->is_healthy()) {
            kv = try_remove_connection(connections, conn_id);
            continue;
        }
        else if (!conn->is_busy(pooling_options.max_simultaneous_requests_per_connection_treshold(distance))) {
            *stream = conn->acquire_stream();
            if (!stream->is_invalid()) {
                return conn;
            }
        }
        else if ((long)connections->size() > pooling_options.core_connections_per_host(distance)) {
            if (conn->is_free(pooling_options.min_simultaneous_requests_per_connection_treshold(distance))) {
                _trashcan->put(conn);
                cql_connections_collection_t::iterator tmp = kv++;
                connections->erase(tmp);
                continue;
            }
        }
        ++kv;
    }

    *stream = cql_stream_t();
    return boost::shared_ptr<cql_connection_t>();
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_session_impl_t::connect(
    boost::shared_ptr<cql::cql_query_plan_t> query_plan,
    cql_stream_t*                            stream,
    std::list<cql_endpoint_t>*               tried_hosts)
{
    assert(stream != NULL);
    assert(tried_hosts != NULL);

    while (boost::shared_ptr<cql_host_t> host = query_plan->next_host_to_query()) {
        if (!host->is_considerably_up()) {
            continue;
        }

        cql_endpoint_t host_address = host->endpoint();
        tried_hosts->push_back(host_address);
        cql_connections_collection_t* connections = add_to_connection_pool(host_address);
        
        boost::shared_ptr<cql_connection_t> conn;
        conn = try_find_free_stream(host, connections, stream);
        if (conn) {
            // Connection must know the pointer to the containing session.
            // Otherwise it would be unable to propagate the result if
            // employed for, e.g. USE query.
            conn->set_session_ptr(this);
            return conn;
        }

        conn = _trashcan->recycle(host_address);
        if (conn && !conn->is_healthy()) {
            free_connection(conn);
            conn.reset();
        }

        if (!conn) {
            try {
                if (!(conn = allocate_connection(host))) {
                    continue;
                }
            }
            catch (cql_connection_allocation_error& e) {
                // This error is interpreted as host being dead.
                host->set_down();
                continue;
            }
            catch (cql_too_many_connections_per_host_exception& e) {
                // This happens; let's try with another host.
                continue;
            }
            host->bring_up();
        }

        if (conn) {
            *stream = conn->acquire_stream();
            (*connections)[conn->id()] = conn;
            conn->set_session_ptr(this);
        }
        return conn;
    }

    throw cql_no_host_available_exception("no host is available according to load balancing policy.");
}

cql::cql_stream_t
cql::cql_session_impl_t::execute_operation(
	const boost::shared_ptr<cql_query_t>&    query,
	cql_connection_t::cql_message_callback_t callback,
    cql_connection_t::cql_message_errback_t	 errback,
	exec_query_method_t						 method)
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
    return cql_stream_t();
}

/** Assigns all the prepared statements (ever created within the session)
    to the given connection. Possibly modifies the value pointed by `stream'.
*/
bool
cql::cql_session_impl_t::setup_prepared_statements(
    boost::shared_ptr<cql_connection_t> conn,
    cql_stream_t*                       stream)
{
    if (!conn) {
        return false;
    }
    
    std::vector<std::vector<cql_byte_t> > unprepared;
    conn->get_unprepared_statements(unprepared);

    bool is_success = true;
    
    for (size_t i = 0u; i < unprepared.size(); ++i) {
        if (_prepare_statements.find(unprepared[i]) == _prepare_statements.end()) {
            // This should not happen: the connection was told to prepare a
            // statement, but session knows nothing about it. We'll skip this statement.
            is_success = false;
            continue;
        }
    
        std::string prepare_query_text = _prepare_statements[unprepared[i]];
        boost::shared_ptr<cql_query_t> prepare_query(
            new cql_query_t(prepare_query_text));

        prepare_query->set_stream(*stream);

        boost::shared_future<cql::cql_future_result_t> future_result
            = conn->prepare(prepare_query);
            
        if (future_result.timed_wait(boost::posix_time::seconds(30))) { // TODO: set sensible (or none) time limit
            // The stream was released after receiving the body. Now we need to re-acquire it.
            *stream = conn->acquire_stream();
            
            if (future_result.get().error.is_err()) {
                is_success = false;
            }
            else {
                // It's a good idea to check whether returned query_id matches the requested one.
                // If they don't - it would be a sign of a severe failure.
                BOOST_ASSERT(future_result.get().result->query_id() == unprepared[i]);
            }
        }
        else {
            is_success = false;
        }
    }
    
    return is_success;
}

/** Assigns the keyspace name for the connection with the value used by session.
    Possibly modifies the value pointed by `stream'.
*/
bool
cql::cql_session_impl_t::setup_keyspace(
    boost::shared_ptr<cql_connection_t> conn,
    cql_stream_t*                       stream)
{
    if (!conn) {
        return false;
    }

    bool is_success = true;
    if (!(conn->is_keyspace_syncd())) {
        boost::shared_ptr<cql_query_t> use_my_keyspace(
            new cql_query_t("USE \""+_keyspace_name+"\";"));

        use_my_keyspace->set_stream(*stream);
        boost::shared_future<cql::cql_future_result_t> future_result
            = conn->query(use_my_keyspace);
        if (future_result.timed_wait(boost::posix_time::seconds(30))) { // TODO: set sensible (or none) time limit
            // The stream was released after receiving the body. Now we need to re-acquire it.
            *stream = conn->acquire_stream();
            
            if (future_result.get().error.is_err()) {
                is_success = false;
            }
        }
        else {
            is_success = false;
        }
    }
    return is_success;
}

boost::shared_future<cql::cql_future_result_t>
cql::cql_session_impl_t::query(
    const boost::shared_ptr<cql_query_t>& query)
{
	cql_stream_t stream;
    boost::shared_ptr<cql_connection_t> conn = get_connection(query, &stream);

    if (conn) {
        query->set_stream(stream);
		return conn->query(query);
    }

    boost::promise<cql_future_result_t>       promise;
    boost::shared_future<cql_future_result_t> shared_future(promise.get_future());

    cql_future_result_t future_result;
    future_result.error.library = true;
    future_result.error.message = "could not obtain viable client from the pool.";
    promise.set_value(future_result);

    return shared_future;
}

boost::shared_future<cql::cql_future_result_t>
cql::cql_session_impl_t::prepare(
    const boost::shared_ptr<cql_query_t>& query) {
    cql_stream_t stream;
    boost::shared_ptr<cql_connection_t> conn = get_connection(query, &stream);

    if (conn) {
        query->set_stream(stream);
		return conn->prepare(query);
    }

    boost::promise<cql_future_result_t>       promise;
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
    
	if (conn) {
        message->set_stream(stream);
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
    boost::recursive_mutex::scoped_lock lock(_mutex);
	if(_Iam_closed)
		return;

	for (connection_pool_t::iterator host_it = _connection_pool.begin();
         host_it != _connection_pool.end();
         ++host_it)
	{
		cql_connections_collection_t* connections = host_it->second;
		for (cql_connections_collection_t::iterator conn_it = connections->begin();
             conn_it != connections->end();
             ++conn_it)
		{
            free_connection(conn_it->second);
		}
        connections->clear();
	}
    _connection_pool.clear();

    if (_trashcan) {
        _trashcan = boost::shared_ptr<cql_trashcan_t>();
    }

    log(0, "size of session::_connection_poll is "
        + boost::lexical_cast<std::string>(_connection_pool.size()));

	_Iam_closed = true;
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
    cql_connection_t&                                          client)
{
    promise->set_value(cql_future_connection_t(&client));
    if (_ready_callback) {
        _ready_callback(this);
    }
}

void
cql::cql_session_impl_t::connect_errback(
    boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise,
    cql_connection_t&                                          connection,
    const cql_error_t&                                         error)
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
        const boost::shared_ptr<cql_query_t>& query,
        cql_stream_t*                         stream)
{
    boost::shared_ptr<cql_query_plan_t> query_plan = _configuration
        ->policies()
        .load_balancing_policy()
        ->new_query_plan(query);

    std::list<cql_endpoint_t> tried_hosts;
    boost::shared_ptr<cql::cql_connection_t> conn
        = connect(query_plan, stream, &tried_hosts);
    
    bool is_setup_keyspace_successful = false;
    bool is_setup_prepared_successful = false;
    
    if (conn) {
        // We cannot be sure if the new connection knows the
        // name of used keyspace (or the prepared statements).
        // Both must be set prior to usage.
        is_setup_keyspace_successful = setup_keyspace(conn, stream);
        is_setup_prepared_successful = setup_prepared_statements(conn, stream);
        
        
        // We also maintain a connection-wide dictionary (vector, really) that maps
        // from stream IDs to recent queries' strings. It is used to retrieve the
        // the recipes for prepared queries if needed by a connection.
        if (!(stream->is_invalid()) && query != NULL) {
            boost::shared_ptr<cql_connection_impl_t<cql_socket_t> > conn_impl
                = boost::static_pointer_cast<cql_connection_impl_t<cql_socket_t> >(conn);
            conn_impl->set_stream_id_vs_query_string(stream->stream_id(), query->query());
        }
    }
    
    if (!is_setup_keyspace_successful || !is_setup_prepared_successful) {
        // With unset keyspace name or without the full set of prepared
        // statements, the connection is unlikely to work. Therefore,
        // we pass null instead of a valid connection.
        conn.reset();
    }
        
    return conn;
}

cql::cql_uuid_t
cql::cql_session_impl_t::id() const {
    return _uuid;
}

#ifdef _DEBUG
void
cql::cql_session_impl_t::inject_random_connection_lowest_layer_shutdown()
{
	boost::recursive_mutex::scoped_lock lock(_mutex);

	size_t select_pool = (rand()*_connection_pool.size())/RAND_MAX;
    for(connection_pool_t::iterator I  = _connection_pool.begin();
        I != _connection_pool.end(); ++I)
    {
		if((select_pool--)==0)
		{
			size_t select_conn = (rand()*(*(I->second)).size())/RAND_MAX;
			cql_connections_collection_t& conn_collection_ptr = *(I->second);
			for(cql_connections_collection_t::iterator
					J  = conn_collection_ptr.begin();
					J != conn_collection_ptr.end(); ++J)
			{
				if((select_conn--)==0)
				{
					J->second->inject_lowest_layer_shutdown();
				}
			}
		}
    }
}
#endif
