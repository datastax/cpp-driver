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

#ifndef CQL_CLIENT_H_
#define CQL_CLIENT_H_

#include <istream>
#include <list>
#include <map>
#include <ostream>
#include <string>

#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/future.hpp>
#include <boost/thread/mutex.hpp>

#include "cql/cql.hpp"
#include "cql/cql_future_connection.hpp"
#include "cql/cql_future_result.hpp"
#include "cql/cql_uuid.hpp"
#include "cql/cql_endpoint.hpp"
#include "cql/cql_stream.hpp"
#include "cql/cql_query.hpp"

namespace cql {

// Forward declarations
class cql_event_t;
class cql_result_t;
class cql_execute_t;
class cql_session_t;
struct cql_error_t;

class cql_connection_t :
        boost::noncopyable {
public:
    typedef
        boost::function<void(const cql::cql_short_t, const std::string&)>
        cql_log_callback_t;

    typedef
        boost::function<void(cql_connection_t&)>
        cql_connection_callback_t;

    typedef
        boost::function<void(cql_connection_t&, const cql_error_t&)>
        cql_connection_errback_t;

    typedef
        boost::function<void(cql_connection_t&, cql_event_t*)>
        cql_event_callback_t;

    typedef
        boost::function<void(cql_connection_t&, const cql::cql_stream_t&, cql::cql_result_t*)>
        cql_message_callback_t;

    typedef
        boost::function<void(cql_connection_t&, const cql::cql_stream_t&, const cql_error_t&)>
        cql_message_errback_t;

    typedef
        std::map<std::string, std::string>
        cql_credentials_t;

    virtual
    ~cql_connection_t() { }

    virtual bool
    is_healthy() const = 0;

    virtual bool
    is_busy(int) const = 0;

    virtual bool
    is_free(int) const = 0;

    virtual bool
    is_empty() const = 0;

    virtual cql_stream_t
    acquire_stream()  = 0;

    virtual void
    release_stream(cql_stream_t& stream) = 0;

    virtual cql_uuid_t
    id() const = 0;

    virtual void
    set_session_ptr(cql_session_t* session_ptr) = 0;
            
    /**
       Connect to the server at the specified address and port.

       @param ip address and port
     */
    virtual boost::shared_future<cql::cql_future_connection_t>
    connect(const cql_endpoint_t& address) = 0;

    /**
       Connect to the server at the specified address and port.

       @param ip address and port
       @param callback Callback function which will be called when connection is complete and ready for use.
       @param errback Errback which will be called for transport errors encountered at any point in the lifecycle of the client.
     */
    virtual void
    connect(
        const cql_endpoint_t&       address,
        cql_connection_callback_t   callback,
        cql_connection_errback_t    errback) = 0;

    /**
       Perform an ad-hoc query using the specified string and consistency.

       @param query query string.
       @param consistency desired consistency.
       @param callback callback for successful query results.
       @param errback callback for un-successful query results. Only triggered for application level errors, transport level errors trigger the connect errback.
       @return stream ID of the message. Results can be returned out of order, and the stream ID is used to correlate the results with the message.
     */
    virtual cql::cql_stream_t
    query(
        const boost::shared_ptr<cql_query_t>&   query,
        cql_message_callback_t                  callback,
        cql_message_errback_t                   errback) = 0;

    /**
       Perform an ad-hoc query using the specified string and consistency.

       @param query query string.
       @param consistency desired consistency.
       @return populated when results or error is obtained
     */
    virtual boost::shared_future<cql::cql_future_result_t>
    query(const boost::shared_ptr<cql_query_t>& query) = 0;

    /**
       Prepare a CQL statement.

       When the callback is triggered it will be passed a cql_message_result_t
       which contains the ID of the prepared statement.
       This ID is used when composing execution messages.

       @param query query string.
       @param callback callback for successful query results.
       @param errback callback for un-successful query results. Only triggered for application level errors, transport level errors trigger the connect errback.
       @return stream ID of the message. Results can be returned out of order, and the stream ID is used to correlate the results with the message.
     */
    virtual cql::cql_stream_t
    prepare(
		const boost::shared_ptr<cql_query_t>&	    query,
		cql_message_callback_t                      callback,
        cql_message_errback_t                       errback) = 0;

    /**
       Prepare a CQL statement.

       When the callback is triggered it will be passed a cql_message_result_t which contains the ID of the prepared statement. This ID is used when composing execution messages.

       @param query query string.
       @return populated when results or error is obtained
     */
    virtual boost::shared_future<cql::cql_future_result_t>
    prepare(const boost::shared_ptr<cql_query_t>& query) = 0;

    /**
       Execute a prepared CQL statement.

       @param message execute message
       @param callback callback for successful query results
       @param errback callback for un-successful query results. Only triggered for application level errors, transport level errors trigger the connect errback.
       @return stream ID of the message. Results can be returned out of order, and the stream ID is used to correlate the results with the message.
     */
    virtual cql::cql_stream_t
    execute(
        const boost::shared_ptr<cql::cql_execute_t>&    message,
        cql_message_callback_t                          callback,
        cql_message_errback_t                           errback) = 0;

    /**
       Execute a prepared CQL statement.

       @param message execute message
       @return populated when results or error is obtained
     */
    virtual boost::shared_future<cql::cql_future_result_t>
    execute(const boost::shared_ptr<cql::cql_execute_t>& message) = 0;

    /**
       If the connection errback is triggered, this function is used to determine whether the underlying transport is still valid or if a reconnect is neccessary.
     */
    virtual bool
    defunct() const = 0;

    /**
       Is the connection ready for queries.
     */
    virtual bool
    ready() const = 0;

    /**
       Force the connection to close.
     */
    virtual void
    close() = 0;

    /**
       IP endpoint.
     */
    virtual const cql_endpoint_t&
    endpoint() const = 0;

    /**
       List of registered events.
     */
    virtual const std::list<std::string>&
    events() const = 0;

    /**
       Set list of registered events and callback
     */
    virtual void
    set_events(
        cql::cql_connection_t::cql_event_callback_t event_callback,
        const std::list<std::string>&               events) = 0;

    /**
       Callback triggered for registered events.
     */
    virtual cql_event_callback_t
    event_callback() const = 0;

    /**
       Credentials used if prompted for authentication
     */
    virtual const cql_credentials_t&
    credentials() const = 0;

    /**
       Credentials used if prompted for authentication
     */
    virtual void
    set_credentials(const cql_credentials_t& credentials) = 0;

    /** 
        Informs the connection that it will have to use certain prepared statement (identified
        by the ID in argument). Lazily triggers "PREPARE" query prior to any future activity.
        Recommended only for advanced users.
    */
    virtual void
    set_prepared_statement(const std::vector<cql_byte_t>& id) = 0;
    
    /** 
        Modifies the argument, namely it appends the IDs of "PREPARE" statements that have
        been set by set_prepared_statement, but not yet been sent to the DB.
        Complexity: O(n), n being the number of statements *registered for preparation* so far.
        Recommended only for advanced users.
    */
    virtual void
    get_unprepared_statements(
        std::vector<std::vector<cql_byte_t> > &output) const = 0;

    /**
        The connection may be forced (externally) to use a different keyspace than the one
        obtained with recent "USE" query. This method answers whether it is necessary to
        sync both keyspace names by firing an extra "USE" statement.
        Recommended only for advanced users.
    */
    virtual bool
    is_keyspace_syncd() const = 0;
            
    /**
       Sets keyspace to be used by this connection. 
       Triggers "USE" query prior to any next query if necessary.
    */
    virtual void
    set_keyspace(const std::string& new_keyspace_name) = 0;
            
    /**
       Force reconnect
    */
    virtual void
    reconnect() = 0;

#ifdef _DEBUG
	virtual void 
	inject_lowest_layer_shutdown() = 0;
#endif
};

} // namespace cql

#endif // CQL_CLIENT_H_
