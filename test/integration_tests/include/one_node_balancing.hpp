#ifndef CQL_ONE_NODE_BALANCING_POLICY_HPP_
#define CQL_ONE_NODE_BALANCING_POLICY_HPP_
				
#include "cql/policies/cql_round_robin_policy.hpp"
	
namespace cql {
	class cql_host_t;
    class cql_cluster_t;
			
	class CQL_EXPORT cql_one_node_balancing_policy_t : public cql_load_balancing_policy_t	
	{			
	public:		
			
		cql_one_node_balancing_policy_t( std::string host_address ) :
			_host_address( host_address ) {}		// the IP of the node we connect to.
				
		virtual boost::shared_ptr<cql_query_plan_t>
		new_query_plan(
            const boost::shared_ptr<cql_query_t>& query);
							
		virtual void
		init(	
            cql_cluster_t* cluster);
				
	private:	
		boost::mutex	 _mutex2;
		std::string		 _host_address;			// the address IP of the node the query will go to	
		cql_cluster_t *  _cluster2;				// need to have its own version of this pointer		
	};				
					
				
	class CQL_EXPORT cql_one_node_query_plan_t : public cql_query_plan_t		
	{		
	public:	

		cql_one_node_query_plan_t( 
			const cql_cluster_t* cluster,
			unsigned             index,
			std::string			 hostAddress );   // the only one address the query will go to		
							
		virtual boost::shared_ptr<cql_host_t> next_host_to_query();			// Returns next host to query.
									
	private:			
		std::string									_hostAddress;		// the address IP of the node the query will go to
		std::vector<boost::shared_ptr<cql_host_t> >	_hosts;		
		boost::mutex								_mutex;
	};					
						
}					
	
#endif	