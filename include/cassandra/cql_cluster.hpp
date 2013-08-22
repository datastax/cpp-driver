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

#ifndef CQL_CLIENT_POOL_FACTORY_H_
#define CQL_CLIENT_POOL_FACTORY_H_

#include <boost/shared_ptr.hpp>
#include "cassandra/cql_session.hpp"
#include "cassandra/cql_builder.hpp"

namespace cql {

    class cql_cluster_t {
	private:
		const std::list<std::string> _contact_points;
		boost::shared_ptr<cql_configuration_t> _configuration;
		cql_cluster_t(const std::list<std::string>& contact_points, boost::shared_ptr<cql_configuration_t> configuration)
			:_contact_points(contact_points), _configuration(configuration){}

	public:
		static boost::shared_ptr<cql_cluster_t> built_from(const cql_initializer_t& initializer);
		static cql_builder_t builder();

		boost::shared_ptr<cql_session_t> connect(boost::asio::io_service& io_service);
		boost::shared_ptr<cql_session_t> connect(boost::asio::io_service& io_service, const std::string& keyspace);
		void shutdown(int timeout_ms=-1);

		~cql_cluster_t()
		{
			shutdown();
		}


    private:

        static cql_session_t*
        create_client_pool_t(cql::cql_session_t::cql_client_callback_t  client_callback,
                             cql::cql_session_t::cql_ready_callback_t   ready_callback,
                             cql::cql_session_t::cql_defunct_callback_t defunct_callback);

        static cql_session_t*
        create_client_pool_t(cql::cql_session_t::cql_client_callback_t  client_callback,
                             cql::cql_session_t::cql_ready_callback_t   ready_callback,
                             cql::cql_session_t::cql_defunct_callback_t defunct_callback,
                             cql::cql_session_t::cql_log_callback_t     log_callback);

        static cql_session_t*
        create_client_pool_t(cql::cql_session_t::cql_client_callback_t  client_callback,
                             cql::cql_session_t::cql_ready_callback_t   ready_callback,
                             cql::cql_session_t::cql_defunct_callback_t defunct_callback,
                             cql::cql_session_t::cql_log_callback_t     log_callback,
                             size_t                                         reconnect_limit);

    };
} 

#endif // CQL_CLIENT_POOL_FACTORY_H_
