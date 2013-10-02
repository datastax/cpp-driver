#include <vector>
#include <iterator>
#include <boost/bind.hpp>

#include "cql/internal/cql_trashcan.hpp"
#include "cql/internal/cql_session_impl.hpp"

cql::cql_trashcan_t::~cql_trashcan_t() {
    std::vector<cql_connections_collection_t*> pointers;
    _trashcan.get_values(std::back_inserter(pointers));
    
    for(std::vector<cql_connections_collection_t*>::iterator it = pointers.begin();
        it != pointers.end(); ++it)
    {
        delete *it;
    }
}

void
cql::cql_trashcan_t::put(const boost::shared_ptr<cql_connection_t>& connection) {
    cql_endpoint_t endpoint = connection->endpoint();
    
    cql_connections_collection_t* conn   = NULL;
    cql_connections_collection_t* result = NULL;
    
    while(!_trashcan.try_get(endpoint, &result)) {
        if(!conn) {
            conn = new cql_connections_collection_t();
        }
        
        _trashcan.try_add(endpoint, conn);
    }
    
    if(conn && (result != conn))
        delete conn;
    
    if(result->try_add(connection->id(), connection)) {
        _timer.expires_from_now(timer_expires_time());
        _timer.async_wait(boost::bind(&cql_trashcan_t::timeout, this, _1));
    }
}

boost::posix_time::time_duration
cql::cql_trashcan_t::timer_expires_time() const {
    return boost::posix_time::seconds(10);
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_trashcan_t::recycle(const cql_endpoint_t& endpoint) 
{
    cql_connections_collection_t* connectons;
    
    if(!_trashcan.try_get(endpoint, &connectons))
        return boost::shared_ptr<cql::cql_connection_t>();
    
    boost::shared_ptr<cql_connection_t> conn;
    
    for(cql_connections_collection_t::iterator it = connectons->begin();
        it != connectons->end(); ++it)
    {
        if(connectons->try_erase(it->first, &conn))
            return conn;
    }
    
    return boost::shared_ptr<cql::cql_connection_t>();
}

void
cql::cql_trashcan_t::cleanup(
    const cql_uuid_t& connection_id, 
    cql_connections_collection_t* const connections)
{
    boost::shared_ptr<cql_connection_t> conn;
    
    if(connections->try_erase(connection_id, &conn)) {
        if(conn->is_empty()) {
            _session.free_connection(conn);
        }
        else {
            connections->try_add(connection_id, conn);
        }
    }
}

void
cql::cql_trashcan_t::cleanup() {
    cql_connections_collection_t* connections;
    
    for(cql_connection_pool_t::iterator host_it = _trashcan.begin();
        host_it != _trashcan.end(); ++host_it)
    {
        connections = host_it->second;
        
        for(cql_connections_collection_t::iterator conn_it = connections->begin();
            conn_it != connections->end(); ++conn_it)
        {
            cleanup(/* connection id: */   conn_it->first, 
                    /* all connections: */ connections);
        }
    }
}

void
cql::cql_trashcan_t::timeout(const boost::system::error_code& error) {
    if(!error)
        cleanup();
}

void
cql::cql_trashcan_t::remove_all() {
    _timer.cancel();
     
    for(cql_connection_pool_t::iterator host_it = _trashcan.begin();
        host_it != _trashcan.end(); ++host_it)
    {
        cql_connections_collection_t* connections = host_it->second;
        
        for(cql_connections_collection_t::iterator conn_it = connections->begin();
            conn_it != connections->end(); ++conn_it)
        {
            _session.free_connection(conn_it->second);
        }
    }
}