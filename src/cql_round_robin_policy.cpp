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
#include <cassert>

#include "cql/cql_host.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_metadata.hpp"
#include "cql/internal/cql_rand.hpp"

#include "cql/policies/cql_round_robin_policy.hpp"

cql::cql_round_robin_query_plan_t::cql_round_robin_query_plan_t(
	const cql_cluster_t* cluster,
	unsigned index)
{
    boost::mutex::scoped_lock lock(_mutex);
    cluster->metadata()->get_hosts(_hosts);
	_index = index;
	_current = 0;
}

boost::shared_ptr<cql::cql_host_t>
cql::cql_round_robin_query_plan_t::next_host_to_query()
{
    boost::mutex::scoped_lock lock(_mutex);
	while (_current < _hosts.size()) {
		unsigned host_to_try = (_index + _current) % _hosts.size();
		_current++;

		boost::shared_ptr<cql::cql_host_t> host = _hosts[host_to_try];
		if (host->is_considerably_up()) {
			return host;
        }
	}

	return boost::shared_ptr<cql::cql_host_t>();
}

void
cql::cql_round_robin_policy_t::init(cql_cluster_t* cluster)
{
    boost::mutex::scoped_lock lock(_mutex);
	_cluster = cluster;
	_index = (unsigned) cql_rand();
}

cql::cql_host_distance_enum
cql::cql_round_robin_policy_t::distance(
    const cql::cql_host_t& host)
{
	return cql::CQL_HOST_DISTANCE_LOCAL;
}

boost::shared_ptr<cql::cql_query_plan_t>
cql::cql_round_robin_policy_t::new_query_plan(
	const boost::shared_ptr<cql::cql_query_t>&)
{
    boost::mutex::scoped_lock lock(_mutex);
	_index++;

	return boost::shared_ptr<cql_query_plan_t>(
		new cql_round_robin_query_plan_t(_cluster, (unsigned)_index));
}
