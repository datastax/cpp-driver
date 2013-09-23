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

#ifndef CQL_SESSION_H_
#define CQL_SESSION_H_

#include <list>
#include <map>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <boost/thread/future.hpp>

#include "cql/cql_future_connection.hpp"
#include "cql/cql_client.hpp"
#include "cql/cql.hpp"

namespace cql {

// Forward declarations
class cql_client_t;
class cql_event_t;
class cql_result_t;
class cql_execute_t;
struct cql_error_t;

class cql_session_t {

public:
    typedef 
        boost::function<boost::shared_ptr<cql::cql_client_t>()>             
        cql_client_callback_t;
    
    typedef 
        boost::function<void(cql_session_t *)>             
        cql_ready_callback_t;
    
    typedef 
        boost::function<void(cql_session_t *)>             
        cql_defunct_callback_t;
    
    typedef 
        boost::function<void(cql_session_t *, 
                             cql::cql_client_t&, 
                             const cql::cql_error_t&)>                      
        cql_connection_errback_t;
    
    typedef 
        boost::function<void(const cql::cql_short_t, const std::string&)>                      
        cql_log_callback_t;

    virtual
    ~cql_session_t() {};

    virtual cql::cql_stream_id_t
    query(
        const std::string&                        query,
        cql::cql_consistency_enum                 consistency,
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
        cql::cql_consistency_enum consistency) = 0;

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

#endif // CQL_SESSION_H_
