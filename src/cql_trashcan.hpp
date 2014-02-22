/*
 * File:   cql_trashcan.hpp
 * Author: mc
 *
 * Created on September 27, 2013, 3:03 PM
 */

#ifndef CQL_TRASHCAN_HPP_
#define	CQL_TRASHCAN_HPP_

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/thread/mutex.hpp>

#include "cql/common_type_definitions.hpp"

#include "cql/cql_endpoint.hpp"
#include "cql/cql_connection.hpp"

namespace cql {
    class cql_session_impl_t;

    class cql_trashcan_t :
        boost::noncopyable
    {
    public:
        cql_trashcan_t(
            boost::asio::io_service& timer_service,
            cql_session_impl_t&      session) :
            _timer(timer_service),
            _session(session)
        {}

        void
        put(
            const boost::shared_ptr<cql_connection_t>& connection);

        boost::shared_ptr<cql_connection_t>
        recycle(
            const cql_endpoint_t& address);

        void
        remove_all();

        friend class cql_session_impl_t;
        
    private:
        typedef std::map<cql_uuid_t, boost::shared_ptr<cql_connection_t> > cql_connections_collection_t;

        void
        timeout(
            const boost::system::error_code& error);

        boost::posix_time::time_duration
        timer_expires_time() const;

        typedef boost::ptr_map<cql_endpoint_t, cql_connections_collection_t> connection_pool_t;

        boost::mutex                _mutex;
        boost::asio::deadline_timer _timer;
        connection_pool_t           _trashcan;
        cql_session_impl_t&         _session;
    };
}


#endif	/* CQL_TRASHCAN_HPP_ */
