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
#include <vector>
#include <iterator>
#include <boost/bind.hpp>

#include "cql/internal/cql_trashcan.hpp"
#include "cql/internal/cql_session_impl.hpp"

void
cql::cql_trashcan_t::put(
    const boost::shared_ptr<cql_connection_t>& connection)
{
    boost::mutex::scoped_lock lock(_mutex);
    cql_endpoint_t endpoint = connection->endpoint();

    connection_pool_t::iterator it = _trashcan.find(endpoint);
    if (it == _trashcan.end()) {
        it = _trashcan.insert(endpoint, new cql_connections_collection_t()).first;
    }

    (*it->second)[connection->id()] = connection;

    // TODO XXX WTF
    // _timer.expires_from_now(timer_expires_time());
    // _timer.async_wait(boost::bind(&cql_trashcan_t::timeout, this, _1));
}

boost::posix_time::time_duration
cql::cql_trashcan_t::timer_expires_time() const {
    return boost::posix_time::seconds(10);
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_trashcan_t::recycle(
    const cql_endpoint_t& endpoint)
{
    boost::mutex::scoped_lock lock(_mutex);

    connection_pool_t::iterator it = _trashcan.find(endpoint);
    if (it == _trashcan.end()) {
        return boost::shared_ptr<cql::cql_connection_t>();
    }

    cql_connections_collection_t* connections = it->second;
    for (cql_connections_collection_t::iterator it = connections->begin();
         it != connections->end();
         ++it)
    {
        boost::shared_ptr<cql::cql_connection_t> conn = it->second;
        connections->erase(it); //this is safe as long as we return in the next line
        return conn;
    }

    return boost::shared_ptr<cql::cql_connection_t>();
}

void
cql::cql_trashcan_t::timeout(const boost::system::error_code& error) {
    if (!error) {
        // cleanup();
    }
}

void
cql::cql_trashcan_t::remove_all() {
    boost::mutex::scoped_lock lock(_mutex);
    _timer.cancel();

    for (connection_pool_t::iterator host_it = _trashcan.begin();
         host_it != _trashcan.end(); ++host_it)
    {
        for (cql_connections_collection_t::iterator conn_it = host_it->second->begin();
             conn_it != host_it->second->end(); ++conn_it)
        {
            _session.free_connection(conn_it->second);
        }
		host_it->second->clear();
    }
    _trashcan.clear();
}
