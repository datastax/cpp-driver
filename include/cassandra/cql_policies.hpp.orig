#ifndef __CQL_POLICIES_H_INCLUDED__
#define __CQL_POLICIES_H_INCLUDED__

#include <stdint.h>
#include <boost/smart_ptr.hpp>
#include <boost/noncopyable.hpp>
#include "cassandra/cql_load_balancing_policy.h"

namespace cql {

	class cql_policies_t : boost::noncopyable
	{
	private:
		shared_ptr<cql_load_balancing_policy_t> _load_balancing_policy;
	public:
		const static cql_load_balancing_policy_t default_load_balancing_policy(new cql_round_robin_policy_t());

		cql_policies_t();
		cql_policies_t(boost::shared_ptr<cql_load_balancing_policy_t> load_balancing_policy):_load_balancing_policy(load_balancing_policy){}

		shared_ptr<cql_load_balancing_policy_t> get_load_balancing_policy()
		{
			return _load_balancing_policy;
		}

	}

} // namespace cql
#endif // __CQL_POLICIES_H_INCLUDED__
