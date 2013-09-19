#include <exception>
#include <iterator>

#include "cql/internal/cql_hosts.hpp"


::boost::shared_ptr<cql::cql_hosts_t>
cql::cql_hosts_t::create( 
	const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy,
	const size_t expected_load)
{
	if(!reconnection_policy)
		throw std::invalid_argument("reconnection_policy cannot be null.");

	return ::boost::shared_ptr<cql::cql_hosts_t>(new 
		cql::cql_hosts_t(reconnection_policy, expected_load));
}

void
cql::cql_hosts_t::get_hosts(std::vector<host_ptr_t>* empty_vector) {
	if(!empty_vector)
		throw std::invalid_argument("empty_vector cannot be null.");

	_hosts.unsafe_get_values(std::back_inserter(*empty_vector));
}

bool 
cql::cql_hosts_t::add_if_not_exists_or_bring_up_if_down( 
	const cql::cql_hosts_t::ip_addr& ip_address )
{
	boost::shared_ptr<cql_host_t> new_host = 
		cql_host_t::create(ip_address, _reconnection_policy);
	
	if (_hosts.try_add(ip_address, new_host))
		return true;

	boost::shared_ptr<cql_host_t> host;
	if(_hosts.try_get(ip_address, &host))
		return host->bring_up_if_down();
	else
		return false;
}

bool 
cql::cql_hosts_t::set_down_if_exists( 
	const boost::asio::ip::address& ip_address )
{
	boost::shared_ptr<cql_host_t> host;

	if (_hosts.try_get(ip_address, &host))
		return host->set_down();
	else
		return false;
}

void 
cql::cql_hosts_t::remove_if_exists(
	const boost::asio::ip::address& ip_address )
{
	_hosts.try_erase(ip_address);
}

void
cql::cql_hosts_t::get_addresses(std::vector<cql::cql_hosts_t::ip_addr>* empty_vector) {
	if(!empty_vector)
		throw std::invalid_argument("empty_vector cannot be null.");

	_hosts.unsafe_get_keys(std::back_inserter(*empty_vector));
}
