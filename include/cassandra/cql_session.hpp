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

#ifndef CQL_CLIENT_POOL_H_
#define CQL_CLIENT_POOL_H_

#include <list>
#include <map>
#include <string>

#include <boost/function.hpp>
#include <boost/thread/future.hpp>
#include "cassandra/cql_future_connection.hpp"
#include "cassandra/cql_client.hpp"
#include "cassandra/cql.hpp"

namespace cql {

    // Forward declarations
    class cql_client_t;
    class cql_event_t;
    class cql_result_t;
    class cql_execute_t;
    struct cql_error_t;

    class cql_session_t
    {

    public:
        typedef boost::function<cql::cql_client_t*()>                                                  cql_client_callback_t;
        typedef boost::function<void(cql_session_t*)>                                              cql_ready_callback_t;
        typedef boost::function<void(cql_session_t*)>                                              cql_defunct_callback_t;
        typedef boost::function<void(cql_session_t*, cql::cql_client_t&, const cql::cql_error_t&)> cql_connection_errback_t;
        typedef boost::function<void(const cql::cql_short_t, const std::string&)>                      cql_log_callback_t;

        virtual
        ~cql_session_t(){};

        virtual boost::shared_future<cql::cql_future_connection_t>
        add_client(
            const std::string& server,
            unsigned int       port) = 0;

        virtual boost::shared_future<cql::cql_future_connection_t>
        add_client(
            const std::string&                      server,
            unsigned int                            port,
            cql::cql_client_t::cql_event_callback_t event_callback,
            const std::list<std::string>&           events) = 0;

        virtual boost::shared_future<cql::cql_future_connection_t>
        add_client(
            const std::string&                        server,
            unsigned int                              port,
            cql::cql_client_t::cql_event_callback_t   event_callback,
            const std::list<std::string>&             events,
            const std::map<std::string, std::string>& credentials) = 0;

        virtual cql::cql_stream_id_t
        query(
            const std::string&                        query,
            cql_int_t                                 consistency,
            cql::cql_client_t::cql_message_callback_t callback,
            cql::cql_client_t::cql_message_errback_t  errback) = 0;

        virtual cql::cql_stream_id_t
        prepare(
            const std::string&                        query,
            cql::cql_client_t::cql_message_callback_t callback,
            cql::cql_client_t::cql_message_errback_t  errback) = 0;

        virtual cql::cql_stream_id_t
        execute(
            cql::cql_execute_t*                       message,
            cql::cql_client_t::cql_message_callback_t callback,
            cql::cql_client_t::cql_message_errback_t  errback) = 0;

        virtual boost::shared_future<cql::cql_future_result_t>
        query(
            const std::string& query,
            cql_int_t          consistency) = 0;

        virtual boost::shared_future<cql::cql_future_result_t>
        prepare(
            const std::string& query) = 0;

        virtual boost::shared_future<cql::cql_future_result_t>
        execute(
            cql::cql_execute_t* message) = 0;

        virtual bool
        defunct() = 0;

        virtual bool
        ready() = 0;

        virtual void
        close() = 0;

        virtual size_t
        size() = 0;

        virtual bool
        empty() = 0;
    };

} // namespace cql

#endif // CQL_CLIENT_POOL_H_
