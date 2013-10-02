/* 
 * File:   cql_cluster_impl.hpp
 * Author: mc
 *
 * Created on September 25, 2013, 12:16 PM
 */

#ifndef CQL_CLUSTER_IMPL_HPP_
#define	CQL_CLUSTER_IMPL_HPP_

#include <boost/shared_ptr.hpp>

#include "cql/cql_connection_factory.hpp"
#include "cql/cql_connection.hpp"
#include "cql/cql_session.hpp"
#include "cql/internal/cql_session_impl.hpp"
#include "cql/cql_builder.hpp"
#include "cql/cql_metadata.hpp"
#include "cql/lockfree/cql_lockfree_hash_map.hpp"
#include "cql/cql_uuid.hpp"

namespace cql {

// This is a non-SSL client factory
struct client_functor_t {
public:
    
    client_functor_t(boost::asio::io_service&              service,
                     cql::cql_connection_t::cql_log_callback_t log_callback) :
        _io_service(service),
        _log_callback(log_callback)
    {}

    inline boost::shared_ptr<cql::cql_connection_t>
    operator()() {
        // called every time the pool needs to initiate a new connection to a host
        return cql::cql_connection_factory_t::create_connection(_io_service, _log_callback);
    }

private:
    boost::asio::io_service&              _io_service;
    cql::cql_connection_t::cql_log_callback_t _log_callback;
};

// This is an SSL client factory
struct client_ssl_functor_t {
public:

    client_ssl_functor_t(boost::asio::io_service&              service,
                         boost::asio::ssl::context&            context,
                         cql::cql_connection_t::cql_log_callback_t log_callback) :
        _io_service(service),
        _ssl_ctx(context),
        _log_callback(log_callback)
    {}

    inline boost::shared_ptr<cql::cql_connection_t>
    operator()() {
        // called every time the pool needs to initiate a new connection to a host
        return cql::cql_connection_factory_t::create_connection(_io_service, _ssl_ctx, _log_callback);
    }

private:
    boost::asio::io_service&              _io_service;
    boost::asio::ssl::context&            _ssl_ctx;
    cql::cql_connection_t::cql_log_callback_t _log_callback;
};

class cql_cluster_impl_t: public cql_cluster_t {
private:
    // Main function of thread on which we call io_service::run
    static void
    asio_thread_main(boost::asio::io_service* io_service) {
        cql::cql_thread_infrastructure_t gc;
        io_service->run();
    }
    
public:
    cql_cluster_impl_t(
            const std::list<cql_endpoint_t>&        endpoints, 
			boost::shared_ptr<cql_configuration_t>  configuration)
        : _contact_points(endpoints)
		, _configuration(configuration)
        , work(new boost::asio::io_service::work(_io_service))
        , thread(boost::bind(&cql_cluster_impl_t::asio_thread_main, &_io_service))
    { 
            _configuration->init(this);
            const cql_policies_t& policies = _configuration->policies();
            
            _metadata = boost::shared_ptr<cql_metadata_t>(new cql_metadata_t(
                    policies.reconnection_policy()
                ));
            
            _metadata->add_hosts(endpoints);
    }
        
     virtual 
     ~cql_cluster_impl_t() { }
        
    virtual boost::shared_ptr<cql::cql_session_t>
    connect() {
        return connect("");
    }
        
    virtual boost::shared_ptr<cql::cql_session_t> 
    connect(const std::string& keyspace) {
        // decide which client factory we want, SSL or non-SSL.  This is a hack, if you pass any commandline arg to the
        // binary it will use the SSL factory, non-SSL by default

        cql::cql_session_t::cql_client_callback_t client_factory;
        boost::asio::ssl::context* ssl_context = 
				_configuration->protocol_options()
							   .ssl_context()
							   .get();
							   
        cql::cql_connection_t::cql_log_callback_t log_callback = 
				_configuration->client_options()
							   .log_callback();

        if (ssl_context != 0) {
            client_factory = client_ssl_functor_t(
				_io_service, 
				const_cast<boost::asio::ssl::context&>(*ssl_context), 
				log_callback);
        } else {
            client_factory = client_functor_t(_io_service, log_callback);
        }

        // Construct the session
        cql_session_callback_info_t session_callbacks;
        session_callbacks.set_client_callback(client_factory);
        session_callbacks.set_log_callback(log_callback);
        
        boost::shared_ptr<cql_session_impl_t> session(
            new cql_session_impl_t(session_callbacks, _configuration));

        session->init(_io_service);
        
        // HACK: Comment this out to get libcds gc bug
       // bool session_add = _connected_sessions.try_add(session->id(), session);
       // assert(session_add);
        
        return session;
    }

    virtual void 
    shutdown(int timeout_ms) {
        close_sessions();
                
        if(work.get()!=NULL) {
            work.reset();
            thread.join();
        }
    }
    
    virtual boost::shared_ptr<cql_metadata_t> 
    metadata() const {
        return _metadata;
    }
    
    friend class cql_metadata_t;
    
private:
    
    typedef
        cql_lockfree_hash_map_t<cql_uuid_t, boost::shared_ptr<cql_session_t> >  
        connected_sessions_t;
    
    void
    close_sessions() {
        for(connected_sessions_t::iterator it = _connected_sessions.begin();
            it != _connected_sessions.end(); ++it)
        {
            boost::shared_ptr<cql_session_t> session;
            if(_connected_sessions.try_erase(it->first, &session))
                session->close();
        }
    }
    
    const std::list<cql_endpoint_t> 		_contact_points;
    boost::shared_ptr<cql_configuration_t> 	_configuration;

    // Initialize the IO service, this allows us to perform network operations asyncronously
    boost::asio::io_service _io_service;

    // Typically async operations are performed in the thread performing the request, because we want synchronous behavior
    // we're going to spawn a thread whose sole purpose is to perform network communication, and we'll use this thread to
    // initiate and check the status of requests.
    //
    // Also, typically the boost::asio::io_service::run will exit as soon as it's work is done, which we want to prevent
    // because it's in it's own thread.  Using boost::asio::io_service::work prevents the thread from exiting.
    boost::scoped_ptr<boost::asio::io_service::work> work;
    boost::thread thread;

    boost::shared_ptr<cql_metadata_t>     _metadata;
    connected_sessions_t                  _connected_sessions;
};

}

#endif	/* CQL_CLUSTER_IMPL_HPP_ */

