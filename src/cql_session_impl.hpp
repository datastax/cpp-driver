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

#ifndef CQL_SESSION_IMPL_H_
#define CQL_SESSION_IMPL_H_

#include <istream>
#include <list>
#include <map>
#include <ostream>
#include <string>

#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_deque.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "cql/common_type_definitions.hpp"
#include "cql/cql.hpp"
#include "cql/cql_builder.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_connection.hpp"
#include "cql/cql_endpoint.hpp"
#include "cql/cql_session.hpp"
#include "cql/policies/cql_load_balancing_policy.hpp"

#include "cql/internal/cql_promise.hpp"
#include "cql/internal/cql_trashcan.hpp"

namespace cql {

// Forward declarations
class cql_event_t;
class cql_result_t;
class cql_execute_t;
struct cql_error_t;

class cql_session_callback_info_t
{
public:
    explicit
    cql_session_callback_info_t(
        const cql_session_t::cql_client_callback_t  client  = 0,
        const cql_session_t::cql_ready_callback_t   ready   = 0,
        const cql_session_t::cql_defunct_callback_t defunct = 0,
        const cql_session_t::cql_log_callback_t     log     = 0)
            : _client_callback(client),
              _ready_callback(ready),
              _defunct_callback(defunct),
              _log_callback(log) { }

    inline void
    set_client_callback(
        const cql_session_t::cql_client_callback_t& client_callback)
    {
        _client_callback = client_callback;
    }

    inline void
    set_ready_callback(
        const cql_session_t::cql_ready_callback_t& ready_callback)
    {
        _ready_callback = ready_callback;
    }

    inline void
    set_defunct_callback(
        const cql_session_t::cql_defunct_callback_t& defunct_callback)
    {
        _defunct_callback = defunct_callback;
    }

    inline void
    set_log_callback(
        const cql_session_t::cql_log_callback_t& log_callback)
    {
        _log_callback = log_callback;
    }

private:
    cql_session_t::cql_client_callback_t
    client_callback() const
    {
        return _client_callback;
    }

    cql_session_t::cql_ready_callback_t
    ready_callback() const
    {
        return _ready_callback;
    }

    cql_session_t::cql_defunct_callback_t
    defunct_callback() const
    {
        return _defunct_callback;
    }

    cql_session_t::cql_log_callback_t
    log_callback() const
    {
        return _log_callback;
    }

    friend class cql_session_impl_t;

private:

    cql_session_t::cql_client_callback_t   _client_callback;
    cql_session_t::cql_ready_callback_t    _ready_callback;
    cql_session_t::cql_defunct_callback_t  _defunct_callback;
    cql_session_t::cql_log_callback_t      _log_callback;
};

class cql_session_impl_t :
    public cql_session_t,
    boost::noncopyable
{

public:
    cql_session_impl_t(
        const cql_session_callback_info_t&      callbacks,
        boost::shared_ptr<cql_configuration_t>  configuration);

    void
    init(boost::asio::io_service& io_service);

    boost::shared_ptr<cql_connection_t>
	connect(
        boost::shared_ptr<cql_query_plan_t>    query_plan,
        cql_stream_t*                          stream,
        std::list<cql_endpoint_t>*             tried_hosts);

    virtual cql_uuid_t
    id() const;


    virtual
	~cql_session_impl_t();

    void
    set_keyspace(const std::string& new_keyspace);
    
    void
    set_prepare_statement(
                          const std::vector<cql_byte_t>& query_id,
                          const std::string& query_text);

#ifdef _DEBUG
	void 
	inject_random_connection_lowest_layer_shutdown();
#endif

private:
    
    typedef std::map<cql_uuid_t, boost::shared_ptr<cql_connection_t> > cql_connections_collection_t;
    typedef
        std::map<std::vector<cql_byte_t>, std::string>
        cql_id_query_map_t;

    struct client_container_t
    {
        client_container_t(
            const boost::shared_ptr<cql_connection_t>& c) :
            connection(c),
            errors(0)
        {}

        boost::shared_ptr<cql_connection_t> connection;
        size_t errors;
    };

    typedef
    boost::ptr_deque<client_container_t>
    clients_collection_t;

    typedef long connection_counter_t;

    typedef
    std::map<
        cql_endpoint_t,
        boost::shared_ptr<connection_counter_t> >
    connections_counter_t;

    boost::shared_ptr<cql_connection_t>
	allocate_connection(
        const boost::shared_ptr<cql_host_t>& host);

	void
	free_connection(
        boost::shared_ptr<cql_connection_t> connection);

	typedef
		cql_stream_t(cql::cql_connection_t::* exec_query_method_t )(
            const boost::shared_ptr<cql_query_t>&	 query,
            cql_connection_t::cql_message_callback_t callback,
            cql_connection_t::cql_message_errback_t  errback);

	cql_stream_t
	execute_operation(
		const boost::shared_ptr<cql_query_t>&	 query,
		cql_connection_t::cql_message_callback_t callback,
        cql_connection_t::cql_message_errback_t	 errback,
		exec_query_method_t						 method);

    cql_stream_t
    query(
        const boost::shared_ptr<cql_query_t>&    query,
        cql_connection_t::cql_message_callback_t callback,
        cql_connection_t::cql_message_errback_t  errback);

    cql_stream_t
    prepare(
        const boost::shared_ptr<cql_query_t>&    query,
        cql_connection_t::cql_message_callback_t callback,
        cql_connection_t::cql_message_errback_t  errback);

    cql_stream_t
    execute(
        const boost::shared_ptr<cql_execute_t>&  message,
        cql_connection_t::cql_message_callback_t callback,
        cql_connection_t::cql_message_errback_t  errback);

    bool
    setup_prepared_statements(
        boost::shared_ptr<cql_connection_t> conn,
        cql_stream_t*                       stream);
    
    bool
    setup_keyspace(
        boost::shared_ptr<cql_connection_t> conn,
        cql_stream_t*                       stream);

    boost::shared_future<cql_future_result_t>
    query(
        const boost::shared_ptr<cql_query_t>& query);

    boost::shared_future<cql_future_result_t>
    prepare(
        const boost::shared_ptr<cql_query_t>& query);

    boost::shared_future<cql_future_result_t>
    execute(
        const boost::shared_ptr<cql_execute_t>& message);

    void
    close();

    inline void
    log(
        cql_short_t        level,
        const std::string& message);

    void
    connect_callback(
        boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise,
        cql_connection_t& client);

    void
    connect_errback(
        boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise,
        cql_connection_t&                                          client,
        const cql_error_t&                                         error);

    void
    connect_future_callback(
        boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise,
        cql_connection_t&                                          client);

    void
    connect_future_errback(
        boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise,
        cql_connection_t&                                          client,
        const cql_error_t&                                         error);

    boost::shared_ptr<cql_connection_t>
    get_connection(
        const boost::shared_ptr<cql_query_t>& query,
        cql_stream_t*                         stream);

    cql_host_distance_enum
    get_host_distance(
        boost::shared_ptr<cql_host_t> host);

    void
    free_connections(
        cql_connections_collection_t& connections,
        const std::list<cql_uuid_t>&  connections_to_remove);

    cql_connections_collection_t*
    add_to_connection_pool(
        cql_endpoint_t& host_address);

    /* Note: the method returns an iterator to the next-after-removed-one element of `connections'.
       Or connections->end() if no such element is present. */
    cql_connections_collection_t::iterator
    try_remove_connection(
        cql_connections_collection_t* const connections,
        const cql_uuid_t&                   connection_id);

    boost::shared_ptr<cql_connection_t>
    try_find_free_stream(
        boost::shared_ptr<cql_host_t> const& host,
        cql_connections_collection_t* const  connections,
        cql_stream_t*                        stream);

    bool
    increase_connection_counter(
        const boost::shared_ptr<cql_host_t>& host);

    bool
    decrease_connection_counter(
        const boost::shared_ptr<cql_host_t>& host);

    long
    get_max_connections_number(
        const boost::shared_ptr<cql_host_t>& host);

    friend class cql_trashcan_t;
    typedef boost::ptr_map<cql_endpoint_t, cql_connections_collection_t> connection_pool_t;

    boost::recursive_mutex                  _mutex;
    cql_session_t::cql_client_callback_t    _client_callback;
    cql_session_t::cql_ready_callback_t     _ready_callback;
    cql_session_t::cql_defunct_callback_t   _defunct_callback;
    cql_session_t::cql_log_callback_t       _log_callback;
    cql_session_t::cql_connection_errback_t _connect_errback;
    cql_uuid_t                              _uuid;
    boost::shared_ptr<cql_configuration_t>  _configuration;
    connection_pool_t                       _connection_pool;
    boost::shared_ptr<cql_trashcan_t>       _trashcan;
    connections_counter_t                   _connection_counters;
    
    std::string                             _keyspace_name;

    cql_id_query_map_t                      _prepare_statements;    

	bool _Iam_closed;
    
};

} // namespace cql

#endif // CQL_SESSION_IMPL_H_
