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
#include <iterator>

#include "cql/internal/cql_hosts.hpp"


boost::shared_ptr<cql::cql_hosts_t>
cql::cql_hosts_t::create(
    const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy)
{
	if(!reconnection_policy) {
		throw std::invalid_argument("reconnection_policy cannot be null.");
    }

	return ::boost::shared_ptr<cql::cql_hosts_t>(new cql::cql_hosts_t(reconnection_policy));
}

void
cql::cql_hosts_t::get_hosts(
    std::vector<host_ptr_t>& empty_vector)
{
    boost::mutex::scoped_lock lock(_mutex);

    for (hosts_map_t::const_iterator it = _hosts.begin(); it != _hosts.end(); ++it) {
        empty_vector.push_back(it->second);
    }
}

bool
cql::cql_hosts_t::bring_up(
    const cql::cql_endpoint_t& endpoint)
{
    boost::mutex::scoped_lock lock(_mutex);

	boost::shared_ptr<cql_host_t> new_host = cql_host_t::create(endpoint, _reconnection_policy);
	_hosts[endpoint] = new_host;
    return new_host->bring_up();
}

bool
cql::cql_hosts_t::set_down(
    const cql::cql_endpoint_t& endpoint)
{
	boost::shared_ptr<cql_host_t> host;

	if (try_get(endpoint, &host)) {
        boost::mutex::scoped_lock lock(_mutex);
		return host->set_down();
    }
	else {
		return false;
    }
}

bool
cql::cql_hosts_t::try_remove(
    const cql::cql_endpoint_t& endpoint)
{
    boost::mutex::scoped_lock lock(_mutex);
	return _hosts.erase(endpoint);
}

void
cql::cql_hosts_t::get_endpoints(
    std::vector<cql::cql_endpoint_t>& empty_vector)
{
    boost::mutex::scoped_lock lock(_mutex);
    for (hosts_map_t::const_iterator it = _hosts.begin(); it != _hosts.end(); ++it) {
        empty_vector.push_back(it->first);
    }
}
