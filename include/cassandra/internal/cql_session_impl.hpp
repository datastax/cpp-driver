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

#ifndef CQL_CLIENT_POOL_IMPL_H_
#define CQL_CLIENT_POOL_IMPL_H_

#include <istream>
#include <list>
#include <map>
#include <ostream>
#include <string>

#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_deque.hpp>

#include "cassandra/cql.hpp"
#include "cassandra/cql_client.hpp"
#include "cassandra/cql_session.hpp"

namespace cql {

    // Forward declarations
    class cql_event_t;
    class cql_result_t;
    class cql_execute_t;
    struct cql_error_t;

    class cql_client_pool_impl_t :
        public cql_session_t,
        boost::noncopyable
    {

    public:
        cql_client_pool_impl_t(
            cql::cql_session_t::cql_client_callback_t  client_callback,
            cql::cql_session_t::cql_ready_callback_t   ready_callback,
            cql::cql_session_t::cql_defunct_callback_t defunct_callback);

        cql_client_pool_impl_t(
            cql::cql_session_t::cql_client_callback_t  client_callback,
            cql::cql_session_t::cql_ready_callback_t   ready_callback,
            cql::cql_session_t::cql_defunct_callback_t defunct_callback,
            cql::cql_session_t::cql_log_callback_t     log_callback);

        cql_client_pool_impl_t(
            cql::cql_session_t::cql_client_callback_t  client_callback,
            cql::cql_session_t::cql_ready_callback_t   ready_callback,
            cql::cql_session_t::cql_defunct_callback_t defunct_callback,
            cql::cql_session_t::cql_log_callback_t     log_callback,
            size_t                                         reconnect_limit);

        virtual
        ~cql_client_pool_impl_t(){};

        boost::shared_future<cql::cql_future_connection_t>
        add_client(
            const std::string& server,
            unsigned int       port);

        boost::shared_future<cql::cql_future_connection_t>
        add_client(
            const std::string&                      server,
            unsigned int                            port,
            cql::cql_client_t::cql_event_callback_t event_callback,
            const std::list<std::string>&           events);

        boost::shared_future<cql::cql_future_connection_t>
        add_client(
            const std::string&                        server,
            unsigned int                              port,
            cql::cql_client_t::cql_event_callback_t   event_callback,
            const std::list<std::string>&             events,
            const std::map<std::string, std::string>& credentials);

        cql::cql_stream_id_t
        query(
            const std::string&                        query,
            cql_int_t                                 consistency,
            cql::cql_client_t::cql_message_callback_t callback,
            cql::cql_client_t::cql_message_errback_t  errback);

        cql::cql_stream_id_t
        prepare(
            const std::string&                        query,
            cql::cql_client_t::cql_message_callback_t callback,
            cql::cql_client_t::cql_message_errback_t  errback);

        cql::cql_stream_id_t
        execute(
            cql::cql_execute_t*                       message,
            cql::cql_client_t::cql_message_callback_t callback,
            cql::cql_client_t::cql_message_errback_t  errback);

        boost::shared_future<cql::cql_future_result_t>
        query(
            const std::string& query,
            cql_int_t          consistency);

        boost::shared_future<cql::cql_future_result_t>
        prepare(
            const std::string& query);

        boost::shared_future<cql::cql_future_result_t>
        execute(
            cql::cql_execute_t* message);

        bool
        defunct();

        bool
        ready();

        void
        close();

        size_t
        size();

        bool
        empty();

    private:

        struct client_container_t
        {
            client_container_t(
                cql::cql_client_t* c) :
                client(c),
                errors(0)
            {}

            std::auto_ptr<cql::cql_client_t> client;
            size_t errors;
        };

        inline void
        log(
            cql::cql_short_t level,
            const std::string& message);

        void
        connect_callback(
            boost::shared_ptr<boost::promise<cql::cql_future_connection_t> > promise,
            cql::cql_client_t& client);

        void
        connect_errback(
            boost::shared_ptr<boost::promise<cql::cql_future_connection_t> > promise,
            cql::cql_client_t& client,
            const cql_error_t& error);

        void
        connect_future_callback(
            boost::shared_ptr<boost::promise<cql::cql_future_connection_t> > promise,
            cql::cql_client_t&                                               client);

        void
        connect_future_errback(
            boost::shared_ptr<boost::promise<cql::cql_future_connection_t> > promise,
            cql::cql_client_t&                                               client,
            const cql_error_t&                     error);

        cql::cql_client_t*
        next_client();

        typedef boost::ptr_deque<client_container_t>     clients_collection_t;
        clients_collection_t                             _clients;
        boost::mutex                                     _mutex;
        bool                                             _ready;
        bool                                             _defunct;
        cql::cql_session_t::cql_client_callback_t    _client_callback;
        cql::cql_session_t::cql_ready_callback_t     _ready_callback;
        cql::cql_session_t::cql_defunct_callback_t   _defunct_callback;
        cql::cql_session_t::cql_log_callback_t       _log_callback;
        cql::cql_session_t::cql_connection_errback_t _connect_errback;
        size_t                                           _reconnect_limit;
    };

} // namespace cql

#endif // CQL_CLIENT_POOL_IMPL_H_
