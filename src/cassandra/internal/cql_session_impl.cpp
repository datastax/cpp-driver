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

#include "cassandra/internal/cql_defines.hpp"
#include "cassandra/internal/cql_session_impl.hpp"

cql::cql_client_pool_impl_t::cql_client_pool_impl_t(
    cql::cql_session_t::cql_client_callback_t  client_callback,
    cql::cql_session_t::cql_ready_callback_t   ready_callback,
    cql::cql_session_t::cql_defunct_callback_t defunct_callback) :
    _ready(false),
    _defunct(false),
    _client_callback(client_callback),
    _ready_callback(ready_callback),
    _defunct_callback(defunct_callback),
    _log_callback(NULL),
    _reconnect_limit(0)
{}

cql::cql_client_pool_impl_t::cql_client_pool_impl_t(
    cql::cql_session_t::cql_client_callback_t  client_callback,
    cql::cql_session_t::cql_ready_callback_t   ready_callback,
    cql::cql_session_t::cql_defunct_callback_t defunct_callback,
    cql::cql_session_t::cql_log_callback_t     log_callback) :
    _ready(false),
    _defunct(false),
    _client_callback(client_callback),
    _ready_callback(ready_callback),
    _defunct_callback(defunct_callback),
    _log_callback(log_callback),
    _reconnect_limit(0)
{}

cql::cql_client_pool_impl_t::cql_client_pool_impl_t(
    cql::cql_session_t::cql_client_callback_t  client_callback,
    cql::cql_session_t::cql_ready_callback_t   ready_callback,
    cql::cql_session_t::cql_defunct_callback_t defunct_callback,
    cql::cql_session_t::cql_log_callback_t     log_callback,
    size_t                                         reconnect_limit) :
    _ready(false),
    _defunct(false),
    _client_callback(client_callback),
    _ready_callback(ready_callback),
    _defunct_callback(defunct_callback),
    _log_callback(log_callback),
    _reconnect_limit(reconnect_limit)
{}

boost::shared_future<cql::cql_future_connection_t>
cql::cql_client_pool_impl_t::add_client(
    const std::string& server,
    unsigned int       port)
{
    std::list<std::string> e;
    return add_client(server, port, NULL, e);
}

boost::shared_future<cql::cql_future_connection_t>
cql::cql_client_pool_impl_t::add_client(
    const std::string&                      server,
    unsigned int                            port,
    cql::cql_client_t::cql_event_callback_t event_callback,
    const std::list<std::string>&           events)
{
    std::map<std::string, std::string> credentials;
    return add_client(server, port, event_callback, events, credentials);
}


boost::shared_future<cql::cql_future_connection_t>
cql::cql_client_pool_impl_t::add_client(
    const std::string&                        server,
    unsigned int                              port,
    cql::cql_client_t::cql_event_callback_t   event_callback,
    const std::list<std::string>&             events,
    const std::map<std::string, std::string>& credentials)
{
    boost::mutex::scoped_lock lock(_mutex);

    boost::shared_ptr<boost::promise<cql::cql_future_connection_t> > promise(new boost::promise<cql::cql_future_connection_t>());
    boost::shared_future<cql::cql_future_connection_t> shared_future(promise->get_future());

    std::auto_ptr<cql::cql_client_pool_impl_t::client_container_t> client_container(new cql::cql_client_pool_impl_t::client_container_t(_client_callback()));

    client_container->client->events(event_callback, events);
    client_container->client->credentials(credentials);
    client_container->client->connect(server,
                                      port,
                                      boost::bind(&cql_client_pool_impl_t::connect_callback, this, promise, ::_1),
                                      boost::bind(&cql_client_pool_impl_t::connect_errback, this, promise, ::_1, ::_2));
    _clients.push_back(client_container.release());
    return shared_future;
}

cql::cql_stream_id_t
cql::cql_client_pool_impl_t::query(
    const std::string&                        query,
    cql::cql_int_t                            consistency,
    cql::cql_client_t::cql_message_callback_t callback,
    cql::cql_client_t::cql_message_errback_t  errback)
{
    boost::mutex::scoped_lock lock(_mutex);

    cql_client_t* client = next_client();
    if (client) {
        return client->query(query, consistency, callback, errback);
    }
    return 0;
}

cql::cql_stream_id_t
cql::cql_client_pool_impl_t::prepare(
    const std::string&                        query,
    cql::cql_client_t::cql_message_callback_t callback,
    cql::cql_client_t::cql_message_errback_t  errback)
{
    boost::mutex::scoped_lock lock(_mutex);

    cql_client_t* client = next_client();
    if (client) {
        return client->prepare(query, callback, errback);
    }
    return 0;
}

cql::cql_stream_id_t
cql::cql_client_pool_impl_t::execute(
    cql::cql_execute_t*                       message,
    cql::cql_client_t::cql_message_callback_t callback,
    cql::cql_client_t::cql_message_errback_t  errback)
{
    boost::mutex::scoped_lock lock(_mutex);

    cql_client_t* client = next_client();
    if (client) {
        return client->execute(message, callback, errback);
    }
    return 0;
}

boost::shared_future<cql::cql_future_result_t>
cql::cql_client_pool_impl_t::query(
    const std::string& query,
    cql_int_t          consistency)
{
    boost::mutex::scoped_lock lock(_mutex);

    cql_client_t* client = next_client();
    if (client) {
        return client->query(query, consistency);
    }

    boost::promise<cql::cql_future_result_t> promise;
    boost::shared_future<cql::cql_future_result_t> shared_future(promise.get_future());

    cql::cql_future_result_t future_result;
    future_result.error.library = true;
    future_result.error.message = "could not obtain viable client from the pool";
    promise.set_value(future_result);
    return shared_future;
}

boost::shared_future<cql::cql_future_result_t>
cql::cql_client_pool_impl_t::prepare(
    const std::string& query)
{
    boost::mutex::scoped_lock lock(_mutex);

    cql_client_t* client = next_client();
    if (client) {
        return client->prepare(query);
    }

    boost::promise<cql::cql_future_result_t> promise;
    boost::shared_future<cql::cql_future_result_t> shared_future(promise.get_future());

    cql::cql_future_result_t future_result;
    future_result.error.library = true;
    future_result.error.message = "could not obtain viable client from the pool";
    promise.set_value(future_result);
    return shared_future;
}

boost::shared_future<cql::cql_future_result_t>
cql::cql_client_pool_impl_t::execute(
    cql::cql_execute_t* message)
{
    boost::mutex::scoped_lock lock(_mutex);

    cql_client_t* client = next_client();
    if (client) {
        return client->execute(message);
    }

    boost::promise<cql::cql_future_result_t> promise;
    boost::shared_future<cql::cql_future_result_t> shared_future(promise.get_future());

    cql::cql_future_result_t future_result;
    future_result.error.library = true;
    future_result.error.message = "could not obtain viable client from the pool";
    promise.set_value(future_result);
    return shared_future;
}

bool
cql::cql_client_pool_impl_t::defunct()
{
    return _defunct;
}

bool
cql::cql_client_pool_impl_t::ready()
{
    return _ready;
}

void
cql::cql_client_pool_impl_t::close()
{
    boost::mutex::scoped_lock lock(_mutex);

    BOOST_FOREACH(cql::cql_client_pool_impl_t::client_container_t& c, _clients) {
        c.client->close();
    }
}

size_t
cql::cql_client_pool_impl_t::size()
{
    return _clients.size();
}

bool
cql::cql_client_pool_impl_t::empty()
{
    return _clients.empty();
}

inline void
cql::cql_client_pool_impl_t::log(
    cql::cql_short_t level,
    const std::string& message)
{
    if (_log_callback) {
        _log_callback(level, message);
    }
}

void
cql::cql_client_pool_impl_t::connect_callback(
    boost::shared_ptr<boost::promise<cql::cql_future_connection_t> > promise,
    cql::cql_client_t&                                               client)
{
    _defunct = false;

    _ready = true;
    promise->set_value(cql::cql_future_connection_t(&client));
    if (_ready_callback) {
        _ready_callback(this);
    }
}

void
cql::cql_client_pool_impl_t::connect_errback(
    boost::shared_ptr<boost::promise<cql::cql_future_connection_t> > promise,
    cql::cql_client_t&                                               client,
    const cql_error_t&                                               error)
{
    boost::mutex::scoped_lock lock(_mutex);

    clients_collection_t::iterator pos = _clients.begin();
    for (; pos != _clients.end(); ++pos) {
        cql::cql_client_pool_impl_t::client_container_t& client_container = (*pos);

        if (client_container.client.get() == &client) {

            if (++client_container.errors > _reconnect_limit) {
                clients_collection_t::auto_type client_ptr = _clients.release(pos);
                log(CQL_LOG_ERROR, "client has reached error threshold, removing from pool");
                promise->set_value(cql::cql_future_connection_t(&client, error));

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
            }
            else {
                log(CQL_LOG_INFO, "attempting to reconnect client");
                client_container.client->reconnect();
            }
            break;
        }
    }
}

cql::cql_client_t*
cql::cql_client_pool_impl_t::next_client()
{
    if (_ready && !_defunct && !_clients.empty()) {
        clients_collection_t::auto_type client = _clients.pop_front();
        cql::cql_client_t* output = client->client.get();
        _clients.push_back(client.release());
        return output;
    }
    return NULL;
}
