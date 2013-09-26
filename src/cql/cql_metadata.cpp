#include <cassert>
#include <algorithm>

#include "cql/internal/cql_util.hpp"
#include "cql/cql_metadata.hpp"
#include "cql/cql_host.hpp"
#include "cql/internal/cql_cluster_impl.hpp"
#include "cql/cql_builder.hpp"
#include "cql/internal/cql_hosts.hpp"

void
cql::cql_metadata_t::get_hosts(std::vector<boost::shared_ptr<cql::cql_host_t> >& collection) const {
    _hosts->get_hosts(&collection);
}
        
boost::shared_ptr<cql::cql_host_t>
cql::cql_metadata_t::get_host(const boost::asio::ip::address& ip_address) const {
    boost::shared_ptr<cql_host_t> host;
    
    if(_hosts->try_get(ip_address, &host))
        return host;
    
    return boost::shared_ptr<cql_host_t>();
}
        
void
cql::cql_metadata_t::get_host_addresses(std::vector<boost::asio::ip::address>& collection) const {
    _hosts->get_addresses(&collection);
}

boost::shared_ptr<cql::cql_host_t>
cql::cql_metadata_t::add_host(const boost::asio::ip::address& ip_address) {
    _hosts->add_if_not_exists_or_bring_up_if_down(ip_address);
    return get_host(ip_address);
}

void
cql::cql_metadata_t::remove_host(const boost::asio::ip::address& ip_address) {
    _hosts->remove_if_exists(ip_address);
}

void
cql::cql_metadata_t::set_down_host(const boost::asio::ip::address& ip_address) {
    _hosts->set_down_if_exists(ip_address);
}
        
void
cql::cql_metadata_t::bring_up_host(const boost::asio::ip::address& ip_address) {
    _hosts->add_if_not_exists_or_bring_up_if_down(ip_address);
}

cql::cql_metadata_t::cql_metadata_t(
    boost::shared_ptr<cql_reconnection_policy_t> reconnection_policy)
    : _reconnection_policy(reconnection_policy),
      _hosts(cql_hosts_t::create(reconnection_policy)) 
{ }

void
cql::cql_metadata_t::add_contact_points(const std::list<std::string>& contact_points) 
{
    std::vector<boost::asio::ip::address> addresses;
    
    for(std::list<std::string>::const_iterator it = contact_points.begin();
        it != contact_points.end(); ++it) 
    {
        boost::asio::ip::address address;
        if(!::cql::to_ipaddr(*it, &address))
            throw std::invalid_argument("IP address [" + *it + "] is invalid.");
            
        addresses.push_back(address);
    }
    
    std::for_each(addresses.begin(), addresses.end(), boost::bind(&cql_metadata_t::add_host, this, _1));
}