/*
  Copyright (c) 2013 Matthew Stump

  This file is part of cassandra.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include <boost/bind.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>

#include "cassandra/internal/cql_session_impl.hpp"

#include "cassandra/cql_client.hpp"
#include "cassandra/cql_client_factory.hpp"

#include "cassandra/cql_cluster.hpp"
#include "cassandra/cql_builder.hpp"


boost::shared_ptr<cql::cql_cluster_t> cql::cql_cluster_t::built_from(const cql_initializer_t& initializer)
{
	return boost::shared_ptr<cql::cql_cluster_t>(new cql::cql_cluster_t(initializer.get_contact_points(), initializer.get_configuration()));
}

cql::cql_builder_t cql::cql_cluster_t::builder()
{
	return cql::cql_builder_t();
}

boost::shared_ptr<cql::cql_session_t> cql::cql_cluster_t::connect(boost::asio::io_service& io_service)
{
	return connect(io_service, "");
}



// This is a non-SSL client factory
struct client_functor_t
{

public:

    client_functor_t(boost::asio::io_service&              service,
                     cql::cql_client_t::cql_log_callback_t log_callback) :
        _io_service(service),
        _log_callback(log_callback)
    {}

    cql::cql_client_t*
    operator()()
    {
        // called every time the pool needs to initiate a new connection to a host
        return cql::cql_client_factory_t::create_cql_client_t(_io_service, _log_callback);
    }

private:
    boost::asio::io_service&              _io_service;
    cql::cql_client_t::cql_log_callback_t _log_callback;
};


// This is an SSL client factory
struct client_ssl_functor_t
{

public:

    client_ssl_functor_t(boost::asio::io_service&              service,
                         boost::asio::ssl::context&            context,
                         cql::cql_client_t::cql_log_callback_t log_callback) :
        _io_service(service),
        _ssl_ctx(context),
        _log_callback(log_callback)
    {}

    cql::cql_client_t*
    operator()()
    {
        // called every time the pool needs to initiate a new connection to a host
        return cql::cql_client_factory_t::create_cql_client_t(_io_service, _ssl_ctx, _log_callback);
    }

private:
    boost::asio::io_service&              _io_service;
    boost::asio::ssl::context&            _ssl_ctx;
    cql::cql_client_t::cql_log_callback_t _log_callback;
};

boost::shared_ptr<cql::cql_session_t> cql::cql_cluster_t::connect(boost::asio::io_service& io_service, const std::string& keyspace)
{
		// decide which client factory we want, SSL or non-SSL.  This is a hack, if you pass any commandline arg to the
        // binary it will use the SSL factory, non-SSL by default

        cql::cql_session_t::cql_client_callback_t client_factory;
		boost::asio::ssl::context* ssl_context = _configuration->get_protocol_options().get_ssl_context().get();
		cql::cql_client_t::cql_log_callback_t log_callback = _configuration->get_client_options().get_log_callback();

		if (ssl_context!=0) {
            client_factory = client_ssl_functor_t(io_service, const_cast<boost::asio::ssl::context&>(*ssl_context), log_callback);
        }
        else {
            client_factory = client_functor_t(io_service, log_callback);
        }

        // Construct the pool
        boost::shared_ptr<cql::cql_session_t> session(cql::cql_cluster_t::create_client_pool_t(client_factory, NULL, NULL));

		int port = _configuration->get_protocol_options().get_port();

		bool wasError = false;

        // Add a client to the pool, this operation returns a future.
		std::list<std::string> contact_points =_configuration->get_protocol_options().get_contact_points();
		for(std::list<std::string>::iterator it = contact_points.begin();it!=contact_points.end();++it)
		{
			boost::shared_future<cql::cql_future_connection_t> connect_future = session->add_client(*it, port);
			// Wait until the connection is complete, or has failed.
			connect_future.wait();

			// Check whether or not the connection was successful.
			std::cout << "connect successfull? ";
			if (connect_future.get().error.is_err()) {
				wasError = true;
				break;
			}
		}
        // Check whether or not the connection was successful.
        std::cout << "connect successfull? ";
        if (!wasError) {
			return session;
		}
		else{
			return 0;
		}
}

void cql::cql_cluster_t::shutdown(int timeout_ms)
{
}


//////////////////




cql::cql_session_t*
cql::cql_cluster_t::create_client_pool_t(cql::cql_session_t::cql_client_callback_t  client_callback,
                                                     cql::cql_session_t::cql_ready_callback_t   ready_callback,
                                                     cql::cql_session_t::cql_defunct_callback_t defunct_callback)
{
    return new cql::cql_client_pool_impl_t(client_callback, ready_callback, defunct_callback);
}

cql::cql_session_t*
cql::cql_cluster_t::create_client_pool_t(cql::cql_session_t::cql_client_callback_t  client_callback,
                                                     cql::cql_session_t::cql_ready_callback_t   ready_callback,
                                                     cql::cql_session_t::cql_defunct_callback_t defunct_callback,
                                                     cql::cql_session_t::cql_log_callback_t     log_callback)
{
    return new cql::cql_client_pool_impl_t(client_callback, ready_callback, defunct_callback, log_callback);
}

cql::cql_session_t*
cql::cql_cluster_t::create_client_pool_t(cql::cql_session_t::cql_client_callback_t  client_callback,
                                                     cql::cql_session_t::cql_ready_callback_t   ready_callback,
                                                     cql::cql_session_t::cql_defunct_callback_t defunct_callback,
                                                     cql::cql_session_t::cql_log_callback_t     log_callback,
                                                     size_t                                         reconnect_limit)
{
    return new cql::cql_client_pool_impl_t(client_callback, ready_callback, defunct_callback, log_callback, reconnect_limit);
}
