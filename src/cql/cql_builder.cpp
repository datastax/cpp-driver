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
#include <algorithm>
#include <boost/bind.hpp>	
	
#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"
#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_driver_internal_error.hpp"

cql::cql_builder_t&
cql::cql_builder_t::add_contact_point(
    const ::boost::asio::ip::address& address)
{
    _contact_points.push_back(
        cql_endpoint_t(address, DEFAULT_PORT));

    return *this;
}

cql::cql_builder_t&
cql::cql_builder_t::add_contact_point(
    const ::boost::asio::ip::address&   address,
    unsigned short                      port)
{
    _contact_points.push_back(
        cql_endpoint_t(address, port));

    return *this;
}

cql::cql_builder_t&
cql::cql_builder_t::add_contact_point(
    const cql_endpoint_t& endpoint)
{
    _contact_points.push_back(endpoint);

    return *this;
}

cql::cql_builder_t&
cql::cql_builder_t::add_contact_points(
    const std::list< ::boost::asio::ip::address>& addresses)
{
    cql_builder_t& (cql_builder_t::*add_contact)(const ::boost::asio::ip::address&)
        = &cql_builder_t::add_contact_point;

    std::for_each(addresses.begin(), addresses.end(),
        boost::bind(add_contact, this, _1));

    return *this;
}

cql::cql_builder_t&
cql::cql_builder_t::add_contact_points(
    const std::list< ::boost::asio::ip::address>& addresses,
    const unsigned short                          port)
{
    std::for_each(addresses.begin(), addresses.end(),
        boost::bind(&cql_builder_t::add_contact_point, this, _1, port));

    return *this;
}

cql::cql_builder_t&
cql::cql_builder_t::add_contact_points(
    const std::list<cql_endpoint_t>& addresses)
{
    cql_builder_t& (cql_builder_t::*add_contact)(const cql_endpoint_t&)
        = &cql_builder_t::add_contact_point;

    std::for_each(addresses.begin(), addresses.end(),
        boost::bind(add_contact, this, _1));

    return *this;
}

boost::shared_ptr<cql::cql_cluster_t>
cql::cql_builder_t::build()
{
    return cql_cluster_t::built_from(*this);
}

cql::cql_builder_t&
cql::cql_builder_t::with_credentials(
        const std::string& user_name,
        const std::string& password)
{
    cql_credentials_t credentials;

    credentials["username"] = user_name;
    credentials["password"] = password;

    _credentials = credentials;
    return *this;
}				
				
							
cql::cql_builder_t&
cql::cql_builder_t::with_load_balancing_policy(boost::shared_ptr<cql::cql_load_balancing_policy_t> load_balancing_policy)
{		
	if(load_balancing_policy == NULL)
		return *this;

	_load_balancing_policy = load_balancing_policy;	
		
	return *this;
}		
		

cql::cql_builder_t&
cql::cql_builder_t::with_reconnection_policy(boost::shared_ptr<cql::cql_reconnection_policy_t> reconnection_policy)
{	
	if(reconnection_policy == NULL)
		return *this;

	_reconnection_policy = reconnection_policy;	
	return *this;
}			


cql::cql_builder_t&
cql::cql_builder_t::with_retry_policy(boost::shared_ptr<cql::cql_retry_policy_t> retry_policy)
{	
	if(retry_policy == NULL)
		return *this;

	_retry_policy = retry_policy;	
	return *this;
}		

cql::cql_builder_t&
cql::cql_builder_t::with_compression(cql::cql_compression_enum compression)
{
    #ifdef CQL_NO_SNAPPY
    if (compression == CQL_COMPRESSION_SNAPPY) {
        throw cql_driver_internal_error_exception(
            "Requested snappy compression, which is unavailable. Recompile with snappy support.");
    }
    #endif
    
    _compression = compression;
	return *this;
}

