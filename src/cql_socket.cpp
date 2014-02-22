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
#include "cql/internal/cql_socket.hpp"

using namespace std;
namespace asio = ::boost::asio;

cql::cql_socket_t::cql_socket_t(asio::io_service& io_service) :
    _socket(new asio::ip::tcp::socket(io_service)) {
}

asio::io_service&
cql::cql_socket_t::get_io_service() {
    return _socket->get_io_service();
}

bool
cql::cql_socket_t::requires_handshake() {
    return false;
}

asio::ip::tcp::socket&
cql::cql_socket_t::lowest_layer() {
    return *_socket;
}

void
cql::cql_socket_t::reset() {
    _socket.reset(new asio::ip::tcp::socket(_socket->get_io_service()));
}
