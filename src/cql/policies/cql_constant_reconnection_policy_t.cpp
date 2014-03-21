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
#include <exception>
#include <limits>
#include <cmath>
		
#include "cql/policies/cql_constant_reconnection_policy_t.hpp"
		
boost::posix_time::time_duration
cql::cql_constant_reconnection_schedule_t::get_delay()		
{
	return _base_delay;
}		
		
			
cql::cql_constant_reconnection_policy_t::cql_constant_reconnection_policy_t(
        boost::posix_time::time_duration const & base_delay ) : 
		_base_delay(base_delay)
{		
    if(base_delay.is_negative())
        throw std::invalid_argument("base_delay cannot be negative.");
        
    if(base_delay.total_milliseconds() == 0)
        throw std::invalid_argument("base_delay must be at least 1 milisecond long.");
}


boost::shared_ptr<cql::cql_reconnection_schedule_t>
cql::cql_constant_reconnection_policy_t::new_schedule()
{
    return boost::shared_ptr<cql_reconnection_schedule_t>(
        new cql_constant_reconnection_schedule_t( base_delay() ));
}	
		
		