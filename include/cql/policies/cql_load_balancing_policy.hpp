#ifndef __CQL_LOAD_BALANCING_POLICY_H_INCLUDED__
#define __CQL_LOAD_BALANCING_POLICY_H_INCLUDED__

#include <vector>
#include <boost/smart_ptr.hpp>

#include "cql/cql.hpp"

namespace cql {

class cql_cluster_t;
class cql_query_t;
class cql_host_t;

class cql_query_plan_t {
public:
	// Returns next host to query.
	// Returns NULL when all hosts were tried.
	virtual boost::shared_ptr<cql_host_t>
	next_host_to_query() = 0;

	virtual 
	~cql_query_plan_t() { }
};

// Interface class for implementing load balanacing policies.
class cql_load_balancing_policy_t {
public:
    virtual 
    ~cql_load_balancing_policy_t() {}

    virtual cql_host_distance_enum 
	distance(const cql_host_t& host) = 0;

	// Returns new query plan.
    virtual boost::shared_ptr<cql_query_plan_t> 
	new_query_plan(const boost::shared_ptr<cql_query_t>& query) = 0;
    
protected:
    friend class cql_policies_t;
    
    virtual void 
	init(cql_cluster_t* cluster) = 0;

};

} // namespace cql
#endif // __CQL_LOAD_BALANCING_POLICY_H_INCLUDED__
