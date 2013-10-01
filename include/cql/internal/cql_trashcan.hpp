/* 
 * File:   cql_trashcan.hpp
 * Author: mc
 *
 * Created on September 27, 2013, 3:03 PM
 */

#ifndef CQL_TRASHCAN_HPP_
#define	CQL_TRASHCAN_HPP_

#include <boost/noncopyable.hpp>
#include <boost/asio.hpp>

#include "cql/lockfree/cql_lockfree_hash_map.hpp"
#include "cql/common_type_definitions.hpp"
#include "cql/cql_endpoint.hpp"
#include "cql/cql_connection.hpp"

namespace cql {
    class cql_session_impl_t;
    
    class cql_trashcan_t: boost::noncopyable 
    {
    public:
        cql_trashcan_t(
            boost::asio::io_service& timer_service,
            cql_session_impl_t&     session)
            : _timer(timer_service),
              _session(session) { }
                
        void
        put(const boost::shared_ptr<cql_connection_t>& connection);
        
        boost::shared_ptr<cql_connection_t>
        recycle(const cql_endpoint_t& address);
        
        void
        remove_all();
            
    private:
        void
        cleanup();
        
        void
        cleanup(
            const cql_uuid_t& connection_id,
            const boost::shared_ptr<cql_connections_collection_t>& connections);
        
        void
        timeout(const boost::system::error_code& error);
        
        boost::posix_time::time_duration
        timer_expires_time() const;
        
        boost::asio::deadline_timer     _timer;
        cql_connection_pool_t           _trashcan;
        cql_session_impl_t&             _session;
    };
}


#endif	/* CQL_TRASHCAN_HPP_ */

