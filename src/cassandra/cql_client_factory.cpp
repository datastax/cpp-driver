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

#include <memory>
#include "cassandra/internal/cql_client_impl.hpp"
#include "cassandra/internal/cql_defines.hpp"
#include "cassandra/internal/cql_socket.hpp"
#include "cassandra/internal/cql_socket_ssl.hpp"

#include "cassandra/cql_client_factory.hpp"

typedef cql::cql_client_impl_t<cql::cql_socket_t> client_t;
typedef cql::cql_client_impl_t<cql::cql_socket_ssl_t> client_ssl_t;

cql::cql_client_t*
cql::cql_client_factory_t::create_cql_client_t(boost::asio::io_service& io_service) {
    return new client_t(io_service,
                        new cql::cql_socket_t(io_service));
}

cql::cql_client_t*
cql::cql_client_factory_t::create_cql_client_t(boost::asio::io_service& io_service,
        boost::asio::ssl::context& context) {
    return new client_ssl_t(io_service,
                            new cql::cql_socket_ssl_t(io_service, context));
}

cql::cql_client_t*
cql::cql_client_factory_t::create_cql_client_t(boost::asio::io_service& io_service,
        cql::cql_client_t::cql_log_callback_t log_callback) {
    std::auto_ptr<client_t> client(
        new client_t(io_service,
                     new cql::cql_socket_t(io_service),
                     log_callback));
    return client.release();
}

cql::cql_client_t*
cql::cql_client_factory_t::create_cql_client_t(boost::asio::io_service& io_service,
        boost::asio::ssl::context& context,
        cql::cql_client_t::cql_log_callback_t log_callback) {
    std::auto_ptr<client_ssl_t> client(
        new client_ssl_t(io_service,
                         new cql::cql_socket_ssl_t(io_service, context),
                         log_callback));
    return client.release();
}
