/* 
 * File:   common_type_definitions.hpp
 * Author: mc
 *
 * Created on September 27, 2013, 3:09 PM
 */

#ifndef COMMON_TYPE_DEFINITIONS_HPP_
#define	COMMON_TYPE_DEFINITIONS_HPP_

#include "cql/cql_endpoint.hpp"
#include "cql/lockfree/cql_lockfree_hash_map.hpp"

namespace cql {
    
typedef
    cql::cql_lockfree_hash_map_t<
        cql_uuid_t, 
        boost::shared_ptr<cql_connection_t> >
    cql_connections_collection_t;  

typedef
    cql::cql_lockfree_hash_map_t<
        cql_endpoint_t, 
        boost::shared_ptr<cql_connections_collection_t> >
    cql_connection_pool_t;
}

#endif	/* COMMON_TYPE_DEFINITIONS_HPP_ */

