#include <boost/date_time/gregorian/gregorian.hpp>
#include "cql/cql_host.hpp"

cql::cql_host_t::cql_host_t(
	const ip_address& address, 
	const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy)
	: _ip_address(address),
	  _datacenter("unknown"),
	  _rack("unknown"),
	  _is_up(false),
	  _next_up_time(date_time::NumSpecialValues.min_date_time),
	  _reconnection_policy(reconnection_policy),
	  _reconnection_schedule(reconnection_policy->new_schedule())
{ }

cql::cql_host_t
cql::cql_host_t::create(
	const ip_address& address, 
	const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy)
{
	if(address.is_unspecified())
		throw std::invalid_argument("unspecified IP address.");

	if(!reconnection_policy)
		throw std::invalid_argument("reconnection policy cannot be null.");

	return cql_host_t(address, reconnection_policy);
}

void 
cql::cql_host_t::set_location_info( 
	const std::string& datacenter, 
	const std::string& rack )
{
	_datacenter = datacenter;
	_rack = rack;
}

bool 
cql::cql_host_t::is_considerably_up() const
{
	if(is_up())
		return true;

	ptime utc_time = boost::posix_time::microsec_clock::universal_time();
	return (_next_up_time <= utc_time);
}

bool 
cql::cql_host_t::set_down()
{
	if (is_considerably_up()) {
		boost::posix_time::time_duration delay = 
					_reconnection_schedule->get_delay();

		ptime now = utc_now();

		_next_up_time = now + delay;
	}

	if (_is_up) {
		_is_up = false;
		return true;
	}

	return false;
}

boost::posix_time::ptime 
cql::cql_host_t::utc_now() const
{
	return boost::posix_time::microsec_clock::universal_time();
}

bool 
cql::cql_host_t::bring_up_if_down()
{
	_reconnection_schedule = _reconnection_policy->new_schedule();
	
	if (!_is_up) {
		_is_up = true;
		return true;
	}
	
	return false;
}
