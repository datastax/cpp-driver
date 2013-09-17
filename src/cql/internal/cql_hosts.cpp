#include <exception>

cql::cql_hosts_t 
cql::cql_hosts_t::create( 
	const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy )
{
	if(!reconnection_policy)
		throw std::invalid_argument("reconnection_policy cannot be null");

	return cql::cql_hosts_t(reconnection_policy);
}

bool 
cql::cql_hosts_t::add_if_not_exists_or_bring_up_if_down( 
	const boost::asio::ip::address ip_address )
{
	throw "not implemented";
}

bool 
cql::cql_hosts_t::set_down_if_exists( 
	const boost::asio::ip::address ip_address )
{
	throw "not implemented";
}

void 
cql::cql_hosts_t::remove_if_exists(
	const boost::asio::ip::address ip_address )
{
	throw "not implemented";
}

