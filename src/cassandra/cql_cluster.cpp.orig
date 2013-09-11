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

namespace cql
{

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

	class cql_cluster_pimpl_t
	{
	private:
		const std::list<std::string> _contact_points;
		boost::shared_ptr<cql_configuration_t> _configuration;

	    // Initialize the IO service, this allows us to perform network operations asyncronously
        boost::asio::io_service io_service;

        // Typically async operations are performed in the thread performing the request, because we want synchronous behavior
        // we're going to spawn a thread whose sole purpose is to perform network communication, and we'll use this thread to
        // initiate and check the status of requests.
        //
        // Also, typically the boost::asio::io_service::run will exit as soon as it's work is done, which we want to prevent
        // because it's in it's own thread.  Using boost::asio::io_service::work prevents the thread from exiting.
        boost::scoped_ptr<boost::asio::io_service::work> work;
        boost::thread thread;

	public:
		cql_cluster_pimpl_t(const std::list<std::string>& contact_points, boost::shared_ptr<cql::cql_configuration_t> configuration)
			:_contact_points(contact_points), _configuration(configuration),
				work(new boost::asio::io_service::work(io_service)),
				thread(boost::bind(static_cast<size_t(boost::asio::io_service::*)()>(&boost::asio::io_service::run), &io_service))
		{}

		boost::shared_ptr<cql::cql_session_t> connect(const std::string& keyspace)
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

			// Construct the session
			boost::shared_ptr<cql::cql_session_impl_t> session(
					new cql::cql_session_impl_t(client_factory, _configuration));

			session->init();

			return session;
		}

		void shutdown(int timeout_ms)
		{
			if(work.get()!=NULL)
			{
				work.reset();
				thread.join();
			}
		}
	};
}

boost::shared_ptr<cql::cql_cluster_t> cql::cql_cluster_t::built_from(const cql_initializer_t& initializer)
{
	return boost::shared_ptr<cql::cql_cluster_t>(new cql::cql_cluster_t(new cql::cql_cluster_pimpl_t(initializer.get_contact_points(), initializer.get_configuration())));
}

boost::shared_ptr<cql::cql_builder_t> cql::cql_cluster_t::builder()
{
	return boost::shared_ptr<cql::cql_builder_t>(new cql::cql_builder_t());
}

boost::shared_ptr<cql::cql_session_t> cql::cql_cluster_t::connect()
{
	return connect("");
}

boost::shared_ptr<cql::cql_session_t> cql::cql_cluster_t::connect(const std::string& keyspace)
{
	return _pimpl->connect(keyspace);
}

void cql::cql_cluster_t::shutdown(int timeout_ms)
{
	_pimpl->shutdown(timeout_ms);
}

cql::cql_cluster_t::~cql_cluster_t()
{
	shutdown();
}
