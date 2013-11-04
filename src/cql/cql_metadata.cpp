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
#include <exception>

#include "cql/internal/cql_util.hpp"
#include "cql/cql_metadata.hpp"
#include "cql/cql_host.hpp"
#include "cql/internal/cql_cluster_impl.hpp"
#include "cql/cql_builder.hpp"
#include "cql/internal/cql_hosts.hpp"
#include "cql/lockfree/boost_ip_address_traits.hpp"

void
cql::cql_metadata_t::get_hosts(std::vector<boost::shared_ptr<cql::cql_host_t> >& collection) const {
    _hosts->get_hosts(&collection);
}
        
boost::shared_ptr<cql::cql_host_t>
cql::cql_metadata_t::get_host(const cql_endpoint_t& endpoint) const {
    boost::shared_ptr<cql_host_t> host;
    
    if(_hosts->try_get(endpoint, &host))
        return host;
    
    return boost::shared_ptr<cql_host_t>();
}
        
void
cql::cql_metadata_t::get_endpoints(std::vector<cql::cql_endpoint_t>* collection) const {
    if(!collection)
        throw std::invalid_argument("collection cannot be null.");
    
    _hosts->get_endpoints(collection);
}

boost::shared_ptr<cql::cql_host_t>
cql::cql_metadata_t::add_host(const cql_endpoint_t& endpoint) 
{
    _hosts->bring_up(endpoint);
    return get_host(endpoint);
}

void
cql::cql_metadata_t::remove_host(const cql_endpoint_t& endpoint) {
    _hosts->try_remove(endpoint);
}

void
cql::cql_metadata_t::set_down_host(const cql_endpoint_t& endpoint) {
    _hosts->set_down(endpoint);
}
        
void
cql::cql_metadata_t::bring_up_host(const cql_endpoint_t& endpoint) {
    _hosts->bring_up(endpoint);
}

cql::cql_metadata_t::cql_metadata_t(
    boost::shared_ptr<cql_reconnection_policy_t> reconnection_policy)
    : _reconnection_policy(reconnection_policy),
      _hosts(cql_hosts_t::create(reconnection_policy)) 
{ }

void
cql::cql_metadata_t::add_hosts(
    const std::list<cql_endpoint_t>& endpoints) 
{ 
    std::for_each(endpoints.begin(), endpoints.end(), 
        boost::bind(&cql_metadata_t::add_host, this, _1));
}
