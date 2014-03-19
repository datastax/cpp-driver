#include "one_node_balancing.hpp"
#include "cql/cql_host.hpp"
#include "cql/cql_metadata.hpp"		
#include "cql/cql_cluster.hpp"


boost::shared_ptr<cql::cql_query_plan_t>
cql::cql_one_node_balancing_policy_t::new_query_plan(			 
	const boost::shared_ptr<cql::cql_query_t>&)
{				
    boost::mutex::scoped_lock lock(_mutex2 );			
	return boost::shared_ptr<cql_query_plan_t>( new cql_one_node_query_plan_t( _cluster2, 0, _host_address ) );
}		

void
cql::cql_one_node_balancing_policy_t::init(cql_cluster_t* cluster)		
{
    boost::mutex::scoped_lock lock(_mutex2);
	_cluster2 = cluster;		
}		
	
	
		
cql::cql_one_node_query_plan_t::cql_one_node_query_plan_t(	
			const cql_cluster_t* cluster,
			unsigned             index,
			std::string			 hostAddress
			) : 
	_hostAddress( hostAddress )
{		

	boost::mutex::scoped_lock lock(_mutex);
		
	if( cluster != NULL )
	{
		if( cluster->metadata().get() != NULL )
		{
		    cluster->metadata()->get_hosts(_hosts);
		}
	}
}		
				
		

boost::shared_ptr<cql::cql_host_t>
cql::cql_one_node_query_plan_t::next_host_to_query()		
{			
    boost::mutex::scoped_lock lock(_mutex);
			
	if( !_hosts.empty() )
	{
		for( int i = 0; i < _hosts.size(); ++i )
		{
			if( _hosts[ i ].get() != nullptr )
			{							   
				if( _hosts[ i ].get()->address().to_string() == _hostAddress )
				{
					if (_hosts[ i ].get()->is_considerably_up()) 
					{
						return _hosts[ i ];
					}	
				}		
			}			
		}				
						
		//// If the address does not fit, take the first address from the pool.
		boost::shared_ptr<cql::cql_host_t> host = _hosts[ 0 ];		
		if (host->is_considerably_up()) 
		{
			return host;
		}
	}		

	return boost::shared_ptr<cql::cql_host_t>();
}


