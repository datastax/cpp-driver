#include <cassert>

#include "cql/policies/cql_round_robin_policy.hpp"
#include "cql/cql_host.hpp"

cql::cql_round_robin_query_plan_t::cql_round_robin_query_plan_t(
	const boost::shared_ptr<cql::cql_cluster_t>& cluster, 
	unsigned index)
{
	"TODO: Get hosts from cluster - metadata - not yet implemented";
	assert(0);

	_index = index;
	_current = 0;
}

boost::shared_ptr<cql::cql_host_t>
cql::cql_round_robin_query_plan_t::next_host_to_query() {

	while(_current < _hosts.size()) {
		unsigned host_to_try = (_index + _current) % _hosts.size();
		_current++;

		boost::shared_ptr<cql::cql_host_t> host = _hosts[host_to_try];
		if(host->is_considerably_up())
			return host;
	}

	return boost::shared_ptr<cql::cql_host_t>();
}

void
cql::cql_round_robin_policy_t::init(
	const boost::shared_ptr<cql_cluster_t>& cluster)
{
	_cluster = cluster;
	_index = (unsigned)cql_rand();
}

cql::cql_host_distance_enum 
cql::cql_round_robin_policy_t::distance(const cql::cql_host_t& host) {
	return cql::CQL_HOST_DISTANCE_LOCAL;
}

boost::shared_ptr<cql::cql_query_plan_t> 
cql::cql_round_robin_policy_t::new_query_plan(
	const boost::shared_ptr<cql::cql_query_t>& query)
{
	_index++;

	return boost::shared_ptr<cql_query_plan_t>(
		new cql_round_robin_query_plan_t(_cluster, (unsigned)_index));
}