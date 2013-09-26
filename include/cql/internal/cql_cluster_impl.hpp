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

class cql_cluster_pimpl_t {
private:
    // Main function of thread on which we call io_service::run
    static void
    asio_thread_main(boost::asio::io_service* io_service) {
        cql::cql_thread_infrastructure_t gc;
        io_service->run();
    }
    
public:
    cql_cluster_pimpl_t(
			const std::list<std::string>& contact_points, 
			boost::shared_ptr<cql_configuration_t> configuration)
        :_contact_points(contact_points), 
		 _configuration(configuration),
         work(new boost::asio::io_service::work(_io_service)),
         thread(boost::bind(&cql_cluster_pimpl_t::asio_thread_main, &_io_service))
    { 
            cql_policies_t& policies = configuration->get_policies();
            
            _metadata = boost::shared_ptr<cql_metadata_t>(new cql_metadata_t(
                    policies.get_reconnection_policy()
                ));
            
            _metadata->add_contact_points(contact_points);
    }
        
    boost::shared_ptr<cql::cql_session_t> 
    connect(const std::string& keyspace) {
        // decide which client factory we want, SSL or non-SSL.  This is a hack, if you pass any commandline arg to the
        // binary it will use the SSL factory, non-SSL by default

        cql::cql_session_t::cql_client_callback_t client_factory;
        boost::asio::ssl::context* ssl_context = 
				_configuration->get_protocol_options()
							   .get_ssl_context()
							   .get();
							   
        cql::cql_connection_t::cql_log_callback_t log_callback = 
				_configuration->get_client_options()
							   .get_log_callback();

        if (ssl_context != 0) {
            client_factory = client_ssl_functor_t(
				_io_service, 
				const_cast<boost::asio::ssl::context&>(*ssl_context), 
				log_callback);
        } else {
            client_factory = client_functor_t(_io_service, log_callback);
        }

        // Construct the session
        boost::shared_ptr<cql_session_impl_t> session(
            new cql_session_impl_t(client_factory, _configuration));

        session->init(_io_service);
        
        while(!_connected_sessions.try_add(session->session_uuid(), session))
            ;
        
        return session;
    }

    void 
    shutdown(int timeout_ms) {
        if(work.get()!=NULL) {
            work.reset();
            thread.join();
        }
    }
    
    boost::shared_ptr<cql_metadata_t> 
    metadata() const {
        return _metadata;
    }
    
    friend class cql_metadata_t;
    
private:
    const std::list<std::string> 			_contact_points;
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

    boost::shared_ptr<cql_metadata_t>                                           _metadata;
    cql_lockfree_hash_map_t<cql_uuid_t, boost::shared_ptr<cql_session_t> >      _connected_sessions;
};

}

#endif	/* CQL_CLUSTER_IMPL_HPP_ */

