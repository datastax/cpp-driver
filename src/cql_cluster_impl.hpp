/*
 * File:   cql_cluster_impl.hpp
 * Author: mc
 *
 * Created on September 25, 2013, 12:16 PM
 */

#ifndef CQL_CLUSTER_IMPL_HPP_
#define	CQL_CLUSTER_IMPL_HPP_

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "cql/cql_builder.hpp"
#include "cql/cql_connection.hpp"
#include "cql/internal/cql_connection_factory.hpp"
#include "cql/cql_metadata.hpp"
#include "cql/cql_session.hpp"
#include "cql/cql_uuid.hpp"

#include "cql/internal/cql_control_connection.hpp"
#include "cql/internal/cql_session_impl.hpp"

namespace cql {
    
// This is a non-SSL client factory
    struct client_functor_t
{
public:

    client_functor_t(
        boost::asio::io_service&                  service,
        cql::cql_connection_t::cql_log_callback_t log_callback) :
        _io_service(service),
        _log_callback(log_callback)
    {}
    
    inline boost::shared_ptr<cql::cql_connection_t>
    operator()()
    {
        // called every time the pool needs to initiate a new connection to a host
        boost::shared_ptr<cql::cql_connection_t> ret_val
            = cql::cql_connection_factory_t::create_connection(_io_service, _log_callback);
        return ret_val;
    }

private:
    boost::asio::io_service&                  _io_service;
    cql::cql_connection_t::cql_log_callback_t _log_callback;
};

// This is an SSL client factory
    struct client_ssl_functor_t
{
    public:

    client_ssl_functor_t(boost::asio::io_service&                  service,
                         boost::asio::ssl::context&                context,
                         cql::cql_connection_t::cql_log_callback_t log_callback) :
        _io_service(service),
        _ssl_ctx(context),
        _log_callback(log_callback)
    {}

    inline boost::shared_ptr<cql::cql_connection_t>
    operator()()
    {
        // called every time the pool needs to initiate a new connection to a host
        boost::shared_ptr<cql::cql_connection_t> ret_val
            = cql::cql_connection_factory_t::create_connection(_io_service, _ssl_ctx, _log_callback);
        return ret_val;
    }

private:
    boost::asio::io_service&                  _io_service;
    boost::asio::ssl::context&                _ssl_ctx;
    cql::cql_connection_t::cql_log_callback_t _log_callback;
};

class cql_cluster_impl_t :
        public cql_cluster_t
{
private:
    // Main function of thread on which we call io_service::run
    static void
    asio_thread_main(
                     boost::shared_ptr<boost::asio::io_service> io_service)
    {
        io_service->run();
    }

	bool _Iam_shotdown;

public:
    cql_cluster_impl_t(
        const std::list<cql_endpoint_t>&        endpoints,
        boost::shared_ptr<cql_configuration_t>  configuration) :
		_Iam_shotdown(false),
        _io_service(configuration->io_service()),
        _contact_points(endpoints),
        _configuration(configuration),
        _work(new boost::asio::io_service::work(*configuration->io_service()))
    {
		for(int i=0;i<configuration->client_options().thread_pool_size();i++)
			_threads.push_back(boost::shared_ptr<boost::thread>(new boost::thread(boost::bind(&cql_cluster_impl_t::asio_thread_main, _io_service))));

		_configuration->init(this);
        const cql_policies_t& policies = _configuration->policies();

        _metadata = boost::shared_ptr<cql_metadata_t>(new cql_metadata_t(policies.reconnection_policy()));

        _metadata->add_hosts(endpoints);

        _control_connection = boost::shared_ptr<cql_control_connection_t>(
            new cql_control_connection_t(
                *this,
                *_io_service,
                configuration
                ));
        _control_connection->init();
    }

    virtual
    ~cql_cluster_impl_t()
    {
		shutdown(60*1000);
	}

    virtual boost::shared_ptr<cql::cql_session_t>
    connect()
    {
        return connect("");
    }

    virtual boost::shared_ptr<cql::cql_session_t>
    connect(
        const std::string& keyspace)
    {
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
				*_io_service,
				const_cast<boost::asio::ssl::context&>(*ssl_context),
				log_callback);
        }
        else {
            client_factory = client_functor_t(*_io_service, log_callback);
        }
        
        // Construct the session
        cql_session_callback_info_t session_callbacks;
        session_callbacks.set_client_callback(client_factory);
        session_callbacks.set_log_callback(log_callback);

        boost::shared_ptr<cql_session_impl_t> session(
            new cql_session_impl_t(session_callbacks, _configuration));

        session->init(*_io_service);
        session->set_keyspace(keyspace);

        return session;
    }

    virtual void
    shutdown(int timeout_ms) {
		if(_Iam_shotdown)
			return;
        close_sessions();
        _control_connection->shutdown();

        if (_work.get() != NULL) {
            _work.reset();
			for(std::vector<boost::shared_ptr<boost::thread> >::iterator pos = _threads.begin() ;pos!=_threads.end();++pos)
				(*pos)->join();
        }
		_Iam_shotdown = true;
    }

    virtual boost::shared_ptr<cql_metadata_t>
    metadata() const {
        return _metadata;
    }

    friend class cql_metadata_t;

private:

    typedef std::map<cql_uuid_t, boost::shared_ptr<cql_session_t> > connected_sessions_t;

    void
    close_sessions()
    {
        boost::mutex::scoped_lock lock(_mutex);
        for (connected_sessions_t::iterator it = _connected_sessions.begin();
             it != _connected_sessions.end(); ++it)
        {
            it->second->close();
        }
		_connected_sessions.clear();
	}

    boost::shared_ptr<boost::asio::io_service> _io_service;
    const std::list<cql_endpoint_t> 	       _contact_points;
    boost::shared_ptr<cql_configuration_t>     _configuration;

    // Typically async operations are performed in the thread performing the request, because we want synchronous behavior
    // we're going to spawn a thread whose sole purpose is to perform network communication, and we'll use this thread to
    // initiate and check the status of requests.
    //
    // Also, typically the boost::asio::io_service::run will exit as soon as it's work is done, which we want to prevent
    // because it's in it's own thread.  Using boost::asio::io_service::work prevents the thread from exiting.
    boost::mutex                                     _mutex;
    boost::scoped_ptr<boost::asio::io_service::work> _work;
	std::vector<boost::shared_ptr<boost::thread> >	 _threads;
    boost::shared_ptr<cql_metadata_t>                _metadata;
    connected_sessions_t                             _connected_sessions;
    boost::shared_ptr<cql_control_connection_t>      _control_connection;
};

}

#endif	/* CQL_CLUSTER_IMPL_HPP_ */
