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

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <boost/thread/future.hpp>

#include "cql/cql_future_connection.hpp"
#include "cql/cql_connection.hpp"
#include "cql/cql.hpp"
#include "cql/cql_uuid.hpp"
#include "cql/cql_stream.hpp"
#include "cql/cql_query.hpp"

namespace cql {

// Forward declarations
class cql_connection_t;
class cql_event_t;
class cql_result_t;
class cql_execute_t;
struct cql_error_t;

class cql_session_t {

public:
    typedef 
        boost::function<boost::shared_ptr<cql_connection_t>()>             
        cql_client_callback_t;
    
    typedef 
        boost::function<void(cql_session_t *)>             
        cql_ready_callback_t;
    
    typedef 
        boost::function<void(cql_session_t *)>             
        cql_defunct_callback_t;
    
    typedef 
        boost::function<void(cql_session_t *, 
                             cql_connection_t&, 
                             const cql_error_t&)>                      
        cql_connection_errback_t;
    
    typedef 
        boost::function<void(const cql_short_t, const std::string&)>                      
        cql_log_callback_t;

    virtual
    ~cql_session_t() { }

    virtual cql_stream_t
    query(
        const boost::shared_ptr<cql_query_t>&      query,
        cql_connection_t::cql_message_callback_t   callback,
        cql_connection_t::cql_message_errback_t    errback) = 0;

    virtual cql_stream_t
    prepare(
        const boost::shared_ptr<cql_query_t>&      query,
        cql_connection_t::cql_message_callback_t   callback,
        cql_connection_t::cql_message_errback_t    errback) = 0;

    virtual cql_stream_t
    execute(
        const boost::shared_ptr<cql_execute_t>&    message,
        cql_connection_t::cql_message_callback_t   callback,
        cql_connection_t::cql_message_errback_t    errback) = 0;

    virtual boost::shared_future<cql_future_result_t>
    query(const boost::shared_ptr<cql_query_t>& query) = 0;

    virtual boost::shared_future<cql_future_result_t>
    prepare(const boost::shared_ptr<cql_query_t>& query) = 0;

    virtual boost::shared_future<cql_future_result_t>
    execute(const boost::shared_ptr<cql_execute_t>& message) = 0;

    virtual void
    set_keyspace(const std::string& new_keyspace) = 0;

    virtual void
    close() = 0;
    
    // Returns unique session identifier
    virtual cql_uuid_t
    id() const = 0;

#ifdef _DEBUG
	virtual void 
	inject_random_connection_lowest_layer_shutdown() = 0;
#endif
};

} // namespace cql

#endif // CQL_SESSION_H_
