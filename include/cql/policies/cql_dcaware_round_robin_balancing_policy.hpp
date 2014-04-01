#ifndef CQL_DCAWARE_ROUND_ROBIN_BALANCING_POLICY_HPP_
#define CQL_DCAWARE_ROUND_ROBIN_BALANCING_POLICY_HPP_
									
#include "cql/policies/cql_round_robin_policy.hpp"
	
namespace cql {
	class cql_host_t;
    class cql_cluster_t;
		
	std::string DC( std::string const & dc, std::string localDc );		// conwert the data center name if the name is incorrect.		

	class CQL_EXPORT cql_dcaware_round_robin_balancing_policy_t : public cql_load_balancing_policy_t
	{				
	public:		
				
		cql_dcaware_round_robin_balancing_policy_t( std::string localDc ) :
			_usedHostsPerRemoteDc( 0 ),
			_index2( 0 ),
			_cluster2( NULL ),
			_localDc( localDc ) {}		

		cql_dcaware_round_robin_balancing_policy_t( std::string localDc, int usedHostsPerRemoteDc ) :
			_usedHostsPerRemoteDc( usedHostsPerRemoteDc ),
			_index2( 0 ),
			_cluster2( NULL ),	
			_localDc( localDc ) {}		
						
		virtual boost::shared_ptr<cql_query_plan_t>
		new_query_plan(
            const boost::shared_ptr<cql_query_t>& query);
							
		virtual void
		init(	
            cql_cluster_t* cluster);
				
		virtual cql::cql_host_distance_enum
			distance(const cql::cql_host_t& host);
									
	private:		

		std::string			_localDc;
		boost::mutex		_mutex2;
		cql_cluster_t *		_cluster2;				// need to have its own version of this pointer					
		int					_usedHostsPerRemoteDc;
		int					_index2;
	};				
					
			
		
	class CQL_EXPORT cql_dcaware_round_robin_query_plan_t : public cql_round_robin_query_plan_t
	{		
	public:	
				
		cql_dcaware_round_robin_query_plan_t( 
			const cql_cluster_t* cluster2,
			unsigned             index2,
			std::string			 localDc,			
			int					 usedHostsPerRemoteDc	
			);   // the only one address the query will go to		
							
		virtual boost::shared_ptr<cql_host_t> next_host_to_query();		// Returns next host to query.
											
	private:												
		std::vector<boost::shared_ptr<cql_host_t> > local_hosts;		// local hosts	
		std::vector<boost::shared_ptr<cql_host_t> > remote_hosts;		// remote hosts	
		boost::mutex					_mutex2;
		std::string						_localDc;		// the address IP of the node the query will go to
		const cql_cluster_t*			_cluster2;		// 	
		int 							_index2;		// 	
		std::map< std::string, long >	_dcQueryCount;	// how many times we queried each data center.	
		int								_usedHostsPerRemoteDc;	
	};						

}
		
#endif	// CQL_DCAWARE_ROUND_ROBIN_BALANCING_POLICY_HPP_