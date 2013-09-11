#ifndef __CQL_LOAD_BALANCING_POLICY_H_INCLUDED__
#define __CQL_LOAD_BALANCING_POLICY_H_INCLUDED__

#include <boost/smart_ptr.hpp>
#include "cassandra/cql_builder.hpp"
namespace cql {
	
	class cql_cluster_t;

	enum cql_host_distance_t {
		CQL_HOST_REMOTE, 
		CQL_HOST_LOCAL
	};

	class cql_host_t
	{
	public:
		std::string get_address(){return "127.0.0.1";}
		bool is_considerably_up(){return false;}
	};

	class cql_query_t
	{
	};

	class cql_query_plan_t
	{
	public:
		virtual ~cql_query_plan_t(){}
		virtual bool move_next()=0;
		virtual boost::shared_ptr<cql_host_t> current()=0;
	};

	class cql_load_balancing_policy_t
	{
	public:
		virtual ~cql_load_balancing_policy_t(){}
        virtual void Initialize(boost::shared_ptr<cql_cluster_t> cluster) =0;
        virtual cql_host_distance_t distance(const cql_host_t* host) =0;
		virtual boost::shared_ptr<cql_query_plan_t> new_query_plan(boost::shared_ptr<cql_query_t> query) = 0;
	};

	class cql_round_robin_policy_t: public cql_load_balancing_policy_t
	{
	public:
        virtual void Initialize(boost::shared_ptr<cql_cluster_t> cluster)
		{
		}

        virtual cql_host_distance_t distance(const cql_host_t* host)
		{
			return CQL_HOST_LOCAL;
		}

		class round_robin_query_plan_t:public cql_query_plan_t
		{
			virtual bool move_next()
			{
				return false;
			}
			virtual boost::shared_ptr<cql_host_t> current()
			{
				return 0;
			}

		};

		virtual boost::shared_ptr<cql_query_plan_t> new_query_plan(boost::shared_ptr<cql_query_t> query)
		{
				return 0;
		}

	};

} // namespace cql
#endif // __CQL_LOAD_BALANCING_POLICY_H_INCLUDED__
