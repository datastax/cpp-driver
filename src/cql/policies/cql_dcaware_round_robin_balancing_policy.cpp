				
//#ifdef CQL_UNIT_TESTS				//// In unit test we must use a mock-object to simulate hundreds of virtual nodes.	
//	#include "../test/integration_tests/include/dcaware_test_metadata_mock_object.hpp"
//#endif		
			
#include "cql/policies/cql_dcaware_round_robin_balancing_policy.hpp"
#include "cql/cql_host.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_metadata.hpp"				
	
std::string cql::DC( std::string const & dc, std::string localDc )		// conwert the data center name if the name is incorrect.	
{
	if( dc == "" || dc == "unknown" )			// if there is something incorrect with the name of the datacenter, convert it to the local data center name.
		return localDc;
		
	return dc;
}


boost::shared_ptr<cql::cql_query_plan_t>
cql::cql_dcaware_round_robin_balancing_policy_t::new_query_plan(			 
	const boost::shared_ptr<cql::cql_query_t>&)
{						
    boost::mutex::scoped_lock lock( _mutex2 );			
	return boost::shared_ptr<cql_query_plan_t>( new cql_dcaware_round_robin_query_plan_t( _cluster2, ++_index2, _localDc, _usedHostsPerRemoteDc ) );
}				
				
							
cql::cql_host_distance_enum
	cql::cql_dcaware_round_robin_balancing_policy_t::distance(const cql::cql_host_t& host)
{	
	if( host.datacenter().empty() )
		return cql::CQL_HOST_DISTANCE_LOCAL;

	if( DC( host.datacenter(), _localDc ) == _localDc )
		return cql::CQL_HOST_DISTANCE_LOCAL;
		
	std::vector<boost::shared_ptr<cql_host_t> > collection;
	_cluster2->metadata()->get_hosts( collection );
	int const host_size = collection.size();

	int ix = 0;

	for( int i = 0; i < host_size; ++i )
    {
		if( collection[ i ]->address() == host.address() )
		{
			if ( ix < _usedHostsPerRemoteDc)
				return cql::CQL_HOST_DISTANCE_REMOTE;
			else
				return cql::CQL_HOST_DISTANCE_IGNORE;
		}		
		else if( collection[ i ]->datacenter() == host.datacenter() )
		{						
			ix++;			
        }					
	}
	return cql::CQL_HOST_DISTANCE_IGNORE;
}		
		
	

void			
cql::cql_dcaware_round_robin_balancing_policy_t::init(cql_cluster_t* cluster2)			// j.kasprzak - zmiany.	
{		
    boost::mutex::scoped_lock lock(_mutex2);
	_cluster2 = cluster2;		
				
	if( _cluster2->metadata().get() != NULL )					//// if we here know the number of nodes, 
	{															//// set at the beginning the starting node as random.	
		std::vector<boost::shared_ptr<cql_host_t> > collection; //// It should not start always with the same node.		
		_cluster2->metadata()->get_hosts( collection );			
		int const host_size = collection.size();
			
		if( host_size > 0 )
		{
			_index2 = rand() % host_size;	
		}	
	}	
	else
	{		
		_index2 = rand() % 1000;		// set a random starting point. It should start differently every start.	
	}
}		
		
										
cql::cql_dcaware_round_robin_query_plan_t::cql_dcaware_round_robin_query_plan_t(	
			const cql_cluster_t* cluster2,
			unsigned             index2,
			std::string			 localDc,
			int					 usedHostsPerRemoteDc	
			) :		
	cql_round_robin_query_plan_t( cluster2, index2 ),
	_localDc( localDc ),	
	_cluster2( cluster2),	
	_index2( index2 ),		
	_usedHostsPerRemoteDc( usedHostsPerRemoteDc )
{						
}						
				
		
												
boost::shared_ptr<cql::cql_host_t>
cql::cql_dcaware_round_robin_query_plan_t::next_host_to_query()		
{			
    boost::mutex::scoped_lock lock( _mutex2 );
		
	if( _cluster2->metadata().get() == NULL )						//// Check if the metadata pointer is NULL. 
		return boost::shared_ptr<cql::cql_host_t>();				//// 	

	std::vector<boost::shared_ptr<cql_host_t> > collection_hosts;	//// all hosts. 
	_cluster2->metadata()->get_hosts( collection_hosts );

	if( collection_hosts.empty() )
		return boost::shared_ptr<cql::cql_host_t>();				//// there are no hosts available for this cluster.	 	

	std::vector<boost::shared_ptr<cql_host_t> > local_hosts;		//// local hosts
	std::vector<boost::shared_ptr<cql_host_t> > remote_hosts;		//// remote hosts
					
	for( int i = 0; i < collection_hosts.size(); ++i )				//// filter which divides hosts into two vectors: local and remote	
	{				
		if( !collection_hosts[ i ]->is_considerably_up() )
			continue;
				
		if( DC( collection_hosts[ i ]->datacenter(), _localDc ) == _localDc )
		{	
			local_hosts.push_back( collection_hosts[ i ] );
		}
		else
		{
			remote_hosts.push_back( collection_hosts[ i ] );
		}
	}

	++_index2;		// try the next host from the vector of hosts if we look in the local hosts. It must be a round robin algorithm.	

	int const size_of_local( local_hosts.size() );
	if( !local_hosts.empty() )
	{		
		return local_hosts[ _index2 % size_of_local ];		// take the next host from  the local pool	
	}							
								
	if( remote_hosts.empty() )	
		return boost::shared_ptr<cql::cql_host_t>();
				
	if( _usedHostsPerRemoteDc == 0 )
		return boost::shared_ptr<cql::cql_host_t>();
			
	int const remote_size( remote_hosts.size() );
									
	bool const IS_RANDOM_SELECTION( true );			//// USE IMPROVED ALGORITHM WITH RANDOM SELECTION OF NODES AND DATACENTERS.
								
	if( IS_RANDOM_SELECTION )	
	{
		_index2 = rand() % remote_size;				//// for every query to a remote data center take a random taken node.	
						
		for( int j = 0; j < 10; ++j )				//// make ten tries of totaly random index.		
		{			
			std::string const data_center_name = DC( remote_hosts[ _index2 ]->datacenter(), _localDc );	

			std::map< std::string, long >::iterator p = _dcQueryCount.find( data_center_name );
			
			if( p == _dcQueryCount.end() )			//// it is the first query to a host from this datacenter.
			{
				_dcQueryCount.insert( std::make_pair( data_center_name, 1 ) );				
				return remote_hosts[ _index2 ];	
			}				
			else
			{
				if( p->second < _usedHostsPerRemoteDc )
				{
					++( p->second );				// increment the number of usage hosts from this data center.	
					return remote_hosts[ _index2 ];		
				}
			}			
			_index2 = rand() % remote_size;				//// for every query to a remote data center take a random taken node.	
		}		
	}
	
			
	//// After 10 unsuccessful random indexes, make the linear search through the sorted array.	
	//// It gives the lowest address for the next available dataceter much more frequently than the high IP address.	
	for( int i = 0; i < remote_size; ++i )		// check each node from romote data centers.	
	{			
		int index3 = ( _index2 + i ) % remote_size;		//// loop through the remote hosts.		
		std::string const data_center_name = DC( remote_hosts[ index3 ]->datacenter(), _localDc );	
				
		std::map< std::string, long >::iterator p = _dcQueryCount.find( data_center_name );
			
		if( p == _dcQueryCount.end() )			//// it is the first query to a host from this datacenter.
		{
			_dcQueryCount.insert( std::make_pair( data_center_name, 1 ) );				
			_index2 += i;
			return remote_hosts[ index3 ];	
		}				
		else
		{
			if( p->second < _usedHostsPerRemoteDc )
			{
				++( p->second );				// increment the number of usage hosts from this data center.	
				_index2 += i;
				return remote_hosts[ index3 ];		
			}			
		}				
	}					
										
	return boost::shared_ptr<cql::cql_host_t>();		//// return NULL. Not found a host.	
}				
				
				