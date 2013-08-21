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

#include "cassandra/internal/cql_socket_ssl.hpp"

cql::cql_socket_ssl_t::cql_socket_ssl_t(boost::asio::io_service& io_service,
                                        boost::asio::ssl::context& ctx) :
    _socket(new cql::cql_socket_ssl_t::socket_t(io_service, ctx)),
    _ctx(ctx)
{
}

boost::asio::io_service&
cql::cql_socket_ssl_t::get_io_service()
{
    return _socket->get_io_service();
}

bool
cql::cql_socket_ssl_t::requires_handshake()
{
    return true;
}

boost::asio::ip::tcp::socket&
cql::cql_socket_ssl_t::lowest_layer()
{
    return _socket->next_layer();
}

void
cql::cql_socket_ssl_t::reset()
{
    _socket.reset(new cql::cql_socket_ssl_t::socket_t(_socket->get_io_service(), _ctx));
}
