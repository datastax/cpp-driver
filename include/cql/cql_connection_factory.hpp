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

#ifndef CQL_CLIENT_FACTORY_H_
#define CQL_CLIENT_FACTORY_H_

#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include "cql/cql_connection.hpp"

namespace cql {
class cql_connection_factory_t {
public:
    /** Instantiate a new cql_connection_t. Client is not capable of SSL.

        @param service the boost IO service used for all network IO
    */
    static boost::shared_ptr<cql_connection_t>
    create_connection(boost::asio::io_service& service);

    /** Instantiate a new cql_connection_t. The client will attempt an SSL handshake on connect.

        @param service the boost IO service used for all network IO
        @param context the boost SSL context, dictates cert validation behavior and SSL version
    */
    static boost::shared_ptr<cql_connection_t>
    create_connection(boost::asio::io_service& service,
                        boost::asio::ssl::context& context);

    /** Instantiate a new cql_connection_t. Client is not capable of SSL.

        @param service the boost IO service used for all network IO
        @param log_callback a callback which will be triggered for all internally generated log messages
    */
    static boost::shared_ptr<cql_connection_t>
    create_connection(boost::asio::io_service& service,
                        cql::cql_connection_t::cql_log_callback_t log_callback);

    /** Instantiate a new cql_connection_t. The client will attempt an SSL handshake on connect.

        @param service the boost IO service used for all network IO
        @param context the boost SSL context, dictates cert validation behavior and SSL version
        @param log_callback a callback which will be triggered for all internally generated log messages
    */
    static boost::shared_ptr<cql_connection_t>
    create_connection(boost::asio::io_service& service,
                        boost::asio::ssl::context& context,
                        cql::cql_connection_t::cql_log_callback_t log_callback);

};
} // namespace cql

#endif // CQL_CLIENT_FACTORY_H_
