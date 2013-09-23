#ifndef __CQL_LOAD_BALANCING_POLICY_H_INCLUDED__
#define __CQL_LOAD_BALANCING_POLICY_H_INCLUDED__

#include <vector>
#include <boost/smart_ptr.hpp>

#include "cql/cql_builder.hpp"
#include "cql/cql_host.hpp"

namespace cql {

class cql_cluster_t;

class cql_query_t { };


class cql_query_plan_t {
public:
	// Returns next host to query.
	boost::shared_ptr<cql_host_t>
	next_host_to_query();
};

// Interface class for implementing load balanacing policies.
class cql_load_balancing_policy_t {
public:
    virtual ~cql_load_balancing_policy_t() {}

	// Initializes policy object.
    virtual void 
	init(boost::shared_ptr<cql_cluster_t> cluster) = 0;


    virtual cql_host_distance_enum 
	distance(const cql_host_t* host) = 0;

	// Returns new query plan.
    virtual boost::shared_ptr<cql_query_plan_t> 
	new_query_plan(boost::shared_ptr<cql_query_t> query) = 0;
};

class cql_round_robin_policy_t: public cql_load_balancing_policy_t {
public:
    virtual void Initialize(boost::shared_ptr<cql_cluster_t> cluster) {
    }

    virtual cql_host_distance_enum distance(const cql_host_t* host) {
        return CQL_HOST_LOCAL;
    }

    class round_robin_query_plan_t:public cql_query_plan_t {
        virtual bool move_next() {
            return false;
        }
        virtual boost::shared_ptr<cql_host_t> current() {
            return boost::shared_ptr<cql_host_t>();
        }

    };

    virtual boost::shared_ptr<cql_query_plan_t> new_query_plan(boost::shared_ptr<cql_query_t> query) {
        return boost::shared_ptr<cql_query_plan_t>();
    }

};

} // namespace cql
#endif // __CQL_LOAD_BALANCING_POLICY_H_INCLUDED__
