#include <algorithm>
#include <boost/bind.hpp>

#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"
#include "cql/internal/cql_util.hpp"


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
cql::cql_builder_t::build() {
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

