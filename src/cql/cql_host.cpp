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
#include <boost/date_time/gregorian/gregorian.hpp>
#include "cql/cql_host.hpp"
#include "cql/internal/cql_util.hpp"

cql::cql_host_t::cql_host_t(
	const cql_endpoint_t& endpoint,
	const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy)
	: _endpoint(endpoint),
	  _datacenter("unknown"),
	  _rack("unknown"),
	  _is_up(false),
	  _next_up_time(boost::posix_time::min_date_time),
	  _reconnection_policy(reconnection_policy),
	  _reconnection_schedule(reconnection_policy->new_schedule())
{ }

::boost::shared_ptr<cql::cql_host_t>
cql::cql_host_t::create(
	const cql_endpoint_t&                               endpoint,
	const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy)
{
	// if (endpoint.is_unspecified()) {
	// 	throw std::invalid_argument("unspecified IP address.");
    // }

	if (!reconnection_policy) {
		throw std::invalid_argument("reconnection policy cannot be null.");
    }

	return ::boost::shared_ptr<cql::cql_host_t>(
        new cql_host_t(endpoint, reconnection_policy));
}

void
cql::cql_host_t::set_location_info(
	const std::string& datacenter,
	const std::string& rack )
{
    boost::unique_lock<boost::mutex> lock(_mutex);
	_datacenter = datacenter;
	_rack = rack;
}

bool
cql::cql_host_t::is_considerably_up() const
{
	if(is_up())
		return true;

	boost::posix_time::ptime utc_time = utc_now();
	return (_next_up_time <= utc_time);
}

bool
cql::cql_host_t::set_down()
{
	if (is_considerably_up()) {
		boost::posix_time::time_duration delay =
					_reconnection_schedule->get_delay();

		boost::posix_time::ptime now = utc_now();

		_next_up_time = now + delay;
	}

	if (_is_up) {
		_is_up = false;
		return true;
	}

	return false;
}

bool
cql::cql_host_t::bring_up()
{
	_reconnection_schedule = _reconnection_policy->new_schedule();

	if (!_is_up) {
		_is_up = true;
		return true;
	}

	return false;
}

cql::cql_host_distance_enum
cql::cql_host_t::distance(const cql_policies_t& policies) const {
    cql_host_distance_enum distance = policies
                                     .load_balancing_policy()
                                    ->distance(*this);
    return distance;
}
