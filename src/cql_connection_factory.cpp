/*
 *      Copyright (C) 2013 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "cql/internal/cql_connection_impl.hpp"
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_socket.hpp"
#include "cql/internal/cql_socket_ssl.hpp"

#include "cql/internal/cql_connection_factory.hpp"

typedef cql::cql_connection_impl_t<cql::cql_socket_t>       connection_t;
typedef cql::cql_connection_impl_t<cql::cql_socket_ssl_t>   ssl_connection_t;

boost::shared_ptr<cql::cql_connection_t>
cql::cql_connection_factory_t::create_connection(boost::asio::io_service& io_service) 
{
    return boost::shared_ptr<cql::cql_connection_t>(
            new connection_t(io_service, new cql::cql_socket_t(io_service)));
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_connection_factory_t::create_connection(
        boost::asio::io_service& io_service,
        boost::asio::ssl::context& context) 
{
    return boost::shared_ptr<cql::cql_connection_t>(
        new ssl_connection_t(io_service, new cql::cql_socket_ssl_t(io_service, context)));
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_connection_factory_t::create_connection(
        boost::asio::io_service& io_service,
        cql::cql_connection_t::cql_log_callback_t log_callback) 
{
    return boost::shared_ptr<cql::cql_connection_t>(
        new connection_t(io_service, new cql::cql_socket_t(io_service), log_callback));
}

boost::shared_ptr<cql::cql_connection_t>
cql::cql_connection_factory_t::create_connection(
        boost::asio::io_service& io_service,
        boost::asio::ssl::context& context,
        cql::cql_connection_t::cql_log_callback_t log_callback) 
{
    return boost::shared_ptr<cql::cql_connection_t>(
        new ssl_connection_t(
                         io_service,
                         new cql::cql_socket_ssl_t(io_service, context),
                         log_callback));
}
