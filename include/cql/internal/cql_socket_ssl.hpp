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

#ifndef CQL_SOCKET_SSL_H_
#define CQL_SOCKET_SSL_H_

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

namespace cql {

class cql_socket_ssl_t :
        boost::noncopyable {
public:

    cql_socket_ssl_t(boost::shared_ptr<boost::asio::io_service> io_service,
                     boost::asio::ssl::context&                 ctx);

    boost::asio::io_service&
    get_io_service();

    template<typename ConstBufferSequence, typename WriteHandler>
    void
    async_write_some(const ConstBufferSequence& buffers,
                     WriteHandler handler) {
        _socket->async_write_some(buffers, handler);
    }

    template<typename MutableBufferSequence, typename ReadHandler>
    void
    async_read_some(const MutableBufferSequence& buffers,
                    ReadHandler handler) {
        _socket->async_read_some(buffers, handler);
    }

    template<typename HandshakeHandler>
    void
    async_handshake(HandshakeHandler handler) {
        _socket->async_handshake(boost::asio::ssl::stream_base::client, handler);
    }

    bool
    requires_handshake();

    boost::asio::ip::tcp::socket&
    lowest_layer();

    void
    reset();

private:
    typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket> socket_t;
    std::auto_ptr<socket_t> _socket;
    boost::asio::ssl::context& _ctx;
};
} // namespace cql

#endif // CQL_SOCKET_SSL_H_
