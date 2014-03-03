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

#ifndef CQL_CLIENT_IMPL_H_
#define CQL_CLIENT_IMPL_H_

// Required to use stdint.h
#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#include <iomanip>
#include <iostream>
#include <istream>
#include <ostream>
#include <stdint.h>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#if BOOST_VERSION >= 104800
#include <boost/asio/connect.hpp>
#endif
#include <boost/asio/ssl.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/future.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/asio/strand.hpp>

#include "cql/cql.hpp"
#include "cql/cql_connection.hpp"
#include "cql/cql_error.hpp"
#include "cql/cql_execute.hpp"
#include "cql/cql_future_connection.hpp"
#include "cql/cql_future_result.hpp"
#include "cql/internal/cql_message.hpp"
#include "cql/internal/cql_defines.hpp"
#include "cql/internal/cql_callback_storage.hpp"
#include "cql/internal/cql_header_impl.hpp"
#include "cql/internal/cql_message_event_impl.hpp"
#include "cql/internal/cql_message_execute_impl.hpp"
#include "cql/internal/cql_message_prepare_impl.hpp"
#include "cql/internal/cql_message_query_impl.hpp"
#include "cql/internal/cql_message_result_impl.hpp"
#include "cql/internal/cql_message_credentials_impl.hpp"
#include "cql/internal/cql_message_error_impl.hpp"
#include "cql/internal/cql_message_options_impl.hpp"
#include "cql/internal/cql_message_ready_impl.hpp"
#include "cql/internal/cql_message_register_impl.hpp"
#include "cql/internal/cql_message_startup_impl.hpp"
#include "cql/internal/cql_message_supported_impl.hpp"
#include "cql/internal/cql_serialization.hpp"
#include "cql/cql_session.hpp"
#include "cql/cql_uuid.hpp"
#include "cql/cql_stream.hpp"
#include "cql/cql_host.hpp"
#include "cql/internal/cql_util.hpp"
#include "cql/internal/cql_promise.hpp"
#include "cql/internal/cql_session_impl.hpp"

namespace cql {
    
// The following class is a collection of the prepared statements
struct cql_prepare_statements_t
{
    cql_prepare_statements_t() : _is_syncd(true) {}
    
    typedef
        std::vector<cql_byte_t>
        cql_query_id_t;
    
    void
    set(
        const cql_query_id_t& query_id)
    {
        boost::mutex::scoped_lock lock(_mutex);
        
        if (_collection.find(query_id) != _collection.end()) {
            return;
        }
        _collection[query_id] = false;
        _is_syncd = false;
    }
    
    void
    get_unprepared_statements(
        std::vector<cql_query_id_t> &output) const
    {
        if (_is_syncd) {
            return;
        }
            
        bool none_returned = true;
        
        boost::mutex::scoped_lock lock(_mutex);
        
        for(prepare_statements_collection_t::const_iterator
                I  = _collection.begin();
                I != _collection.end(); ++I)
        {
            if (I->second == false) {
                output.push_back(I->first);
                none_returned = false;
            }
        }
        _is_syncd = none_returned;
    }
    
    bool
    enable(
        const cql_query_id_t& query_id)
    {
        boost::mutex::scoped_lock lock(_mutex);
        
        if (_collection.find(query_id) == _collection.end()) {
            return false;
        }
        _collection[query_id] = true;
        return true;
    }

private:

    typedef
        std::map<cql_query_id_t, bool>
        prepare_statements_collection_t;
    
    prepare_statements_collection_t   _collection;
    mutable volatile bool             _is_syncd;
    mutable boost::mutex              _mutex;
};
    
template <typename TSocket>
class cql_connection_impl_t : public cql::cql_connection_t
{
public:
    static const int NUMBER_OF_STREAMS = 128;
    // stream with is 0 is dedicated to events messages and
    // connection management.
    static const int NUMBER_OF_USER_STREAMS = 127;

	//helper boolean value holder
	class boolkeeper
	{
	public:
		boolkeeper():value(false){}
		bool value;
		boost::mutex mutex;
	};

public:
    
    typedef
        std::list<cql::cql_message_buffer_t>
        request_buffer_t;

    typedef
        std::pair<cql_message_callback_t, cql_message_errback_t>
        callback_pair_t;

    typedef
        cql::cql_callback_storage_t<callback_pair_t>
        callback_storage_t;

    typedef
        boost::function<void(const boost::system::error_code&, std::size_t)>
        write_callback_t;

    cql_connection_impl_t(
        boost::asio::io_service&                    io_service,
        TSocket*                                    transport,
        cql::cql_connection_t::cql_log_callback_t   log_callback = 0) :
        _io_service(io_service),
        _strand(io_service),
        _resolver(io_service),
        _transport(transport),
        _callback_storage(NUMBER_OF_STREAMS),
        _number_of_free_stream_ids(NUMBER_OF_USER_STREAMS),
        _connect_callback(0),
        _connect_errback(0),
        _log_callback(log_callback),
        _events_registered(false),
        _event_callback(0),
        _defunct(false),
        _ready(false),
        _closing(false),
        _reserved_stream(_callback_storage.acquire_stream()),
        _uuid(cql_uuid_t::create()),
        _is_disposed(new boolkeeper),
        _stream_id_vs_query_string(NUMBER_OF_STREAMS, ""),
        _session_ptr(NULL)
    {}

	virtual ~cql_connection_impl_t()
	{
		boost::mutex::scoped_lock lock(_is_disposed->mutex);
		// lets set the disposed flag (the shared counter will prevent it from being destroyed)
		_is_disposed->value=true;
	}

    boost::shared_future<cql::cql_future_connection_t>
    connect(const cql_endpoint_t& endpoint)
    {
        boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise(
            new cql_promise_t<cql_future_connection_t>());

        connect(
            endpoint,
            boost::bind(&cql_connection_impl_t::_connection_future_callback, this, promise, ::_1),
            boost::bind(&cql_connection_impl_t::_connection_future_errback, this, promise, ::_1, ::_2));

        return promise->shared_future();
    }

    void
    connect(
        const cql_endpoint_t&       endpoint,
        cql_connection_callback_t   callback,
        cql_connection_errback_t    errback)
    {
        boost::mutex::scoped_lock lock(_mutex);
        _endpoint = endpoint;
        _connect_callback = callback;
        _connect_errback = errback;
        resolve();
    }

    virtual cql_uuid_t
    id() const
    {
        return _uuid;
    }

    void
    set_session_ptr(cql_session_t* session_ptr)
    {
        _session_ptr = static_cast<cql_session_impl_t*>(session_ptr);
    }
    
    boost::shared_future<cql::cql_future_result_t>
    query(
        const boost::shared_ptr<cql_query_t>& query_)
    {
        boost::shared_ptr<cql_promise_t<cql_future_result_t> > promise(
            new cql_promise_t<cql_future_result_t>());

		query(query_,
              boost::bind(&cql_connection_impl_t::_statement_future_callback, this, promise, ::_1, ::_2, ::_3),
              boost::bind(&cql_connection_impl_t::_statement_future_errback, this, promise, ::_1, ::_2, ::_3));

        return promise->shared_future();
    }

    boost::shared_future<cql::cql_future_result_t>
    prepare(
        const boost::shared_ptr<cql_query_t>& query)
	{
        boost::shared_ptr<cql_promise_t<cql_future_result_t> > promise(
            new cql_promise_t<cql_future_result_t>());

        prepare(query,
                boost::bind(&cql_connection_impl_t::_statement_future_callback, this, promise, ::_1, ::_2, ::_3),
                boost::bind(&cql_connection_impl_t::_statement_future_errback, this, promise, ::_1, ::_2, ::_3));

        return promise->shared_future();
    }

    boost::shared_future<cql::cql_future_result_t>
    execute(
        const boost::shared_ptr<cql::cql_execute_t>& message)
	{
        boost::shared_ptr<cql_promise_t<cql_future_result_t> > promise(
            new cql_promise_t<cql_future_result_t>());

        execute(message,
                boost::bind(&cql_connection_impl_t::_statement_future_callback, this, promise, ::_1, ::_2, ::_3),
                boost::bind(&cql_connection_impl_t::_statement_future_errback, this, promise, ::_1, ::_2, ::_3));

        return promise->shared_future();
    }

    cql::cql_stream_t
    query(
        const boost::shared_ptr<cql_query_t>&         query,
        cql::cql_connection_t::cql_message_callback_t callback,
        cql::cql_connection_t::cql_message_errback_t  errback)
    {
		cql_stream_t stream = query->stream();

        if (stream.is_invalid()) {
            errback(*this, stream, create_stream_id_error());
            return stream;
		}

		_callback_storage.set_callbacks(stream, callback_pair_t(callback, errback));

        cql::cql_message_query_impl_t messageQuery(query);

		create_request(
            &messageQuery,
			boost::bind(&cql_connection_impl_t::write_handle,
			this,
			boost::asio::placeholders::error,
			boost::asio::placeholders::bytes_transferred),
			stream);

		return stream;
    }

    cql::cql_stream_t
    prepare(
        const boost::shared_ptr<cql_query_t>&         query,
        cql::cql_connection_t::cql_message_callback_t callback,
        cql::cql_connection_t::cql_message_errback_t  errback)
    {
		cql_stream_t stream = query->stream();

        if (stream.is_invalid()) {
            errback(*this, stream, create_stream_id_error());
            return stream;
        }
        _stream_id_vs_query_string[stream.stream_id()] = query->query();

        _callback_storage.set_callbacks(stream, callback_pair_t(callback, errback));

        cql::cql_message_prepare_impl_t messageQuery(query);

        create_request(
            &messageQuery,
            boost::bind(&cql_connection_impl_t::write_handle,
                        this,
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred),
            stream);

        return stream;
    }

    cql::cql_stream_t
    execute(
        const boost::shared_ptr<cql::cql_execute_t>&  message,
        cql::cql_connection_t::cql_message_callback_t callback,
        cql::cql_connection_t::cql_message_errback_t  errback)
    {

        cql_stream_t stream = message->stream();

        if (stream.is_invalid()) {
            errback(*this, stream, create_stream_id_error());
            return stream;
        }

        _callback_storage.set_callbacks(stream, callback_pair_t(callback, errback));
        create_request(
            message->impl(),
            boost::bind(&cql_connection_impl_t::write_handle,
                        this,
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred),
            stream);

       return stream;
    }

    bool
    defunct() const
    {
        return _defunct;
    }

    bool
    ready() const
    {
        return _ready;
    }

    void
    close()
    {
        boost::mutex::scoped_lock lock(_mutex);
        
        if (_closing) {
            return;
        }
        
        _closing = true;
        log(CQL_LOG_INFO, "closing connection");

        const cql_error_t error
            = cql_error_t::transport_error(boost::system::errc::connection_aborted,
                                           "The connection was closed.");
        
        // Now we will set the promises on all callbacks to some error state.
        // Otherwise, if disposed, all futures would throw `broken_promise' exception.
        for (int i = 0; i < NUMBER_OF_STREAMS; ++i) {
            const cql_stream_t consecutive_stream
                = cql_stream_t::from_stream_id(cql_stream_id_t(i));
            
            if (_callback_storage.has_callbacks(consecutive_stream)) {
                callback_pair_t callback_and_errback
                    = _callback_storage.get_callbacks(consecutive_stream);
                cql_message_errback_t errback
                    = callback_and_errback.second;
                
                if (errback) {
                    errback(*this, consecutive_stream, error);
                }
            }
        }
        
        boost::system::error_code ec;
        _transport->lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
        _transport->lowest_layer().close();
    }

    virtual const cql_endpoint_t&
    endpoint() const
    {
        return _endpoint;
    }

    void
    set_events(
        cql::cql_connection_t::cql_event_callback_t event_callback,
        const std::list<std::string>&               events)
    {
        _event_callback = event_callback;
        _events = events;
    }
    
    void
    events_register()
    {

        cql::cql_message_register_impl_t messageRegister;
        messageRegister.events(_events);
        
        // We need to reset _connect_callback here. Otherwise, registering an event
        // may fire cql_session_impl_t::connect_callback(...) which is the default
        // value of _connect_callback. That can result in havoc, since bound variable
        // `promise' may no longer exist. Anyway, this callback was not that crucial.
        _connect_callback = 0;

        create_request(&messageRegister,
                       boost::bind(&cql_connection_impl_t::write_handle,
                                   this,
                                   boost::asio::placeholders::error,
                                   boost::asio::placeholders::bytes_transferred),
                       _reserved_stream);
        
        _events_registered = true;
    }

    const std::list<std::string>&
    events() const
    {
        return _events;
    }

    cql::cql_connection_t::cql_event_callback_t
    event_callback() const
    {
        return _event_callback;
    }

    const cql_credentials_t&
    credentials() const
    {
        return _credentials;
    }

    void
    set_credentials(
        const cql_connection_t::cql_credentials_t& credentials)
    {
        _credentials = credentials;
    }
    
    bool
    is_keyspace_syncd() const
    {
        return _selected_keyspace_name == _current_keyspace_name
                    || _selected_keyspace_name == "";
    }
    
    void
    set_keyspace(const std::string& new_keyspace_name)
    {
        _selected_keyspace_name = new_keyspace_name;
    }
    
    void
    set_prepared_statement(const std::vector<cql_byte_t>& id)
    {
        _prepare_statements.set(id);
    }
   
    void
    get_unprepared_statements(
        std::vector<std::vector<cql_byte_t> > &output) const
    {
        _prepare_statements.get_unprepared_statements(output);
    }

    void
    reconnect()
    {
        _closing = false;
        _events_registered = false;
        _ready = false;
        _defunct = false;
        resolve();
    }
    
    void
    set_stream_id_vs_query_string(
        cql_byte_t stream_id,
        const std::string& query_string)
    {
        _stream_id_vs_query_string[stream_id] = query_string;
    }

#ifdef _DEBUG
	void 
	inject_lowest_layer_shutdown()
	{
        boost::system::error_code ec;
        _transport->lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
	}
#endif

private:
    inline cql::cql_error_t
    create_stream_id_error()
    {
        cql::cql_error_t error;
        error.library = true;
        error.message = "Too many streams. The maximum value of parallel requests is 127 (1 is reserved by this library)";
        return error;
    }

    inline void
    log(
        cql::cql_short_t level,
        const std::string& message)
    {
        if (_log_callback) {
            _log_callback(level, message);
        }
    }

    void
    _connection_future_callback(
        boost::shared_ptr<cql_promise_t<cql_future_connection_t> > promise,
        cql_connection_t&)
    {
        promise->set_value(cql::cql_future_connection_t(this));
    }

    void
    _connection_future_errback(
        boost::shared_ptr<cql_promise_t<cql_future_connection_t> >       promise,
        cql_connection_t&,
        const cql_error_t&                                               error)
    {
        promise->set_value(cql::cql_future_connection_t(this, error));
    }

    void
    _statement_future_callback(
        boost::shared_ptr<cql_promise_t<cql_future_result_t> > promise,
        cql_connection_t&,
        const cql::cql_stream_t&                                     stream,
        cql::cql_result_t*                                           result_ptr)
    {
        promise->set_value(cql::cql_future_result_t(this, stream, result_ptr));
    }

    void
    _statement_future_errback(
        boost::shared_ptr<cql_promise_t<cql_future_result_t> >   promise,
        cql_connection_t&,
        const cql::cql_stream_t&                                     stream,
        const cql_error_t&                                           error)
    {
        promise->set_value(cql::cql_future_result_t(this, stream, error));
    }

    void
    resolve()
    {
        log(CQL_LOG_DEBUG, "resolving remote host: " + _endpoint.to_string());
        
        
        _resolver.async_resolve(
            _endpoint.resolver_query(),
            boost::bind(&cql_connection_impl_t::resolve_handle,
                        this,
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::iterator));
        
        /*
        // Equivalent synchronous call (for debug purposes; avoids spawning internal asio threads)
        boost::system::error_code err;
        boost::asio::ip::tcp::resolver::iterator endpoint_iterator = _resolver.resolve(_endpoint.resolver_query(), err);
        resolve_handle(err, endpoint_iterator);
        */
    }

    void
    resolve_handle(
        const boost::system::error_code&         err,
        boost::asio::ip::tcp::resolver::iterator endpoint_iterator)
    {
        if (!err) {
            log(CQL_LOG_DEBUG, "resolved remote host, attempting to connect");
#if BOOST_VERSION >= 104800
            boost::asio::async_connect(
                _transport->lowest_layer(),
                endpoint_iterator,
                boost::bind(&cql_connection_impl_t::connect_handle,
                            this,
                            boost::asio::placeholders::error));
#else
            _transport->lowest_layer().async_connect(
                *endpoint_iterator,
                boost::bind(&cql_connection_impl_t::connect_handle,
                            this,
                            boost::asio::placeholders::error));
#endif
        }
        else {
            log(CQL_LOG_CRITICAL, "error resolving remote host " + err.message());
            check_transport_err(err);
        }
    }

    void
    connect_handle(
        const boost::system::error_code& err)
    {
        if (!err) {
            log(CQL_LOG_DEBUG, "connection successful to remote host");
            if (_transport->requires_handshake()) {
                _transport->async_handshake(
                    boost::bind(&cql_connection_impl_t::handshake_handle,
                                this,
                                boost::asio::placeholders::error));
            }
            else {
                options_write();
            }
        }
        else {
            log(CQL_LOG_CRITICAL, "error connecting to remote host " + err.message());
            check_transport_err(err);
        }
    }

    void
    handshake_handle(
        const boost::system::error_code& err) {
        if (!err) {
            log(CQL_LOG_DEBUG, "successful ssl handshake with remote host");
            options_write();
        }
        else {
            log(CQL_LOG_CRITICAL, "error performing ssl handshake " + err.message());
            check_transport_err(err);
        }
    }

    // if stream cannot be acquired returns invalid stream
    virtual cql::cql_stream_t
    acquire_stream()
    {
        cql_stream_t stream = _callback_storage.acquire_stream();
        return stream;
    }

    // releases stream, parameter will be set  to invalid stream.
    virtual void
    release_stream(cql_stream_t& stream)
    {
        _callback_storage.release_stream(stream);
    }

	virtual bool
    is_healthy() const
    {
        return _ready && !_defunct && !_closing && !_is_disposed->value;
    }

	virtual bool
    is_busy(int max) const
    {
		return (NUMBER_OF_STREAMS - _number_of_free_stream_ids) >= max;
    }

	virtual bool
    is_free(int min) const
    {
		return (NUMBER_OF_STREAMS - _number_of_free_stream_ids) <= min;
    }

    virtual bool
    is_empty() const
    {
        return (NUMBER_OF_USER_STREAMS == _number_of_free_stream_ids);
    }

    void
    create_request(
        cql::cql_message_t* message,
        write_callback_t    callback,
        cql_stream_t&       stream)
    {
        cql::cql_error_t err;
        message->prepare(&err);

        cql::cql_header_impl_t header(CQL_VERSION_1_REQUEST,
                                      CQL_FLAG_NOFLAG,
                                      stream,
                                      message->opcode(),
                                      message->size());
        header.prepare(&err);

        log(CQL_LOG_DEBUG, "sending message: " + header.str() + " " + message->str());

        std::vector<boost::asio::const_buffer> buf;

        buf.push_back(boost::asio::buffer(header.buffer()->data(), header.size()));

        if (header.length() != 0) {
            buf.push_back(boost::asio::buffer(message->buffer()->data(), message->size()));
        }

        boost::asio::async_write(*_transport, buf, callback);
    }

    void
    write_handle(
        const boost::system::error_code& err,
        std::size_t                      num_bytes)
    {
        if (!err) {
            log(CQL_LOG_DEBUG, "wrote to socket " + boost::lexical_cast<std::string>(num_bytes) + " bytes");
        }
        else {
            log(CQL_LOG_ERROR, "error writing to socket " + err.message());
            check_transport_err(err);
        }
    }

    void
    header_read()
    {
        boost::asio::async_read(*_transport,
                                boost::asio::buffer(_response_header.buffer()->data(), _response_header.size()),
#if BOOST_VERSION >= 104800
                                boost::asio::transfer_exactly(sizeof(cql::cql_header_impl_t)),
#else
                                boost::asio::transfer_all(),
#endif
                                _strand.wrap(boost::bind(&cql_connection_impl_t<TSocket>::header_read_handle, this, _is_disposed, boost::asio::placeholders::error)));
    }

    void
    header_read_handle(
		boost::shared_ptr<boolkeeper> is_disposed,
        const boost::system::error_code& err)
    {
        {
            // if the connection was already disposed we return here immediately
            boost::mutex::scoped_lock lock(is_disposed->mutex);
	        if (is_disposed->value) {
			    return;
            }
        }
    
		if (!err) {
			cql::cql_error_t decode_err;
			if (_response_header.consume(&decode_err)) {
				log(CQL_LOG_DEBUG, "received header for message " + _response_header.str());
				body_read(_response_header);
			}
			else {
				log(CQL_LOG_ERROR, "error decoding header " + _response_header.str());
			}
		}
		else if (err.value() == boost::system::errc::operation_canceled) {
			/* do nothing */
		}
		else if (err == boost::asio::error::eof) {
            // Endopint was closed. The connection is set to defunct state and should not throw.
			_defunct = true;
            //is_disposed->value = true;
            log(CQL_LOG_ERROR, "error reading header " + err.message());
		}
		else {
			log(CQL_LOG_ERROR, "error reading header " + err.message());
			check_transport_err(err);
		}
	}

    void
    body_read(const cql::cql_header_impl_t& header) {

        switch (header.opcode()) {

        case CQL_OPCODE_ERROR:
            _response_message.reset(new cql::cql_message_error_impl_t(header.length()));
            break;

        case CQL_OPCODE_RESULT:
            _response_message.reset(new cql::cql_message_result_impl_t(header.length()));
            break;

        case CQL_OPCODE_SUPPORTED:
            _response_message.reset(new cql::cql_message_supported_impl_t(header.length()));
            break;

        case CQL_OPCODE_READY:
            _response_message.reset(new cql::cql_message_ready_impl_t(header.length()));
            break;

        case CQL_OPCODE_EVENT:
            _response_message.reset(new cql::cql_message_event_impl_t(header.length()));
            break;

        default:
            // need some bucket to put the data so we can get past the unknown
            // body in the stream it will be discarded by the body_read_handle
            _response_message.reset(new cql::cql_message_result_impl_t(header.length()));
            break;
        }

        boost::asio::async_read(*_transport,
                                boost::asio::buffer(header.length()==0 ? 0 : _response_message->buffer()->data(), _response_message->size()),
#if BOOST_VERSION >= 104800
                                boost::asio::transfer_exactly(header.length()),
#else
                                boost::asio::transfer_all(),
#endif
                                _strand.wrap(boost::bind(&cql_connection_impl_t<TSocket>::body_read_handle, this, header,_is_disposed, boost::asio::placeholders::error)));
    }
    
    
    /* The purpose of this method is to propagate the results of USE and PREPARE
       queries across the session. */
    void
    preprocess_result_message(cql::cql_message_result_impl_t* response_message)
    {
        switch(response_message->result_type()) {
                
            case CQL_RESULT_SET_KEYSPACE: {
                {
                    boost::mutex::scoped_lock lock(_mutex);
                    response_message->get_keyspace_name(_current_keyspace_name);
                }
                if (_session_ptr) {
                    _session_ptr->set_keyspace(_current_keyspace_name);
                }
            }
            break;
                
            case CQL_RESULT_PREPARED: {
                const std::vector<cql_byte_t>& query_id = response_message->query_id();
                set_prepared_statement(query_id);
                _prepare_statements.enable(query_id);
                if (_session_ptr) {
                    cql_stream_id_t stream_id = _response_header.stream().stream_id();
                    
                    _session_ptr->set_prepare_statement(
                        query_id,
                        _stream_id_vs_query_string[stream_id]);
                }
            }
            break;
                
            default: {}
            break;
        }
    }

    void
    body_read_handle(
        const cql::cql_header_impl_t& header,
		boost::shared_ptr<boolkeeper> is_disposed,
        const boost::system::error_code& err)
    {
		boost::mutex::scoped_lock lock(is_disposed->mutex);
		// if the connection was already disposed we return here immediatelly
		if(is_disposed->value)
			return;

        log(CQL_LOG_DEBUG, "received body for message " + header.str());

        if (err) {
            if (err == boost::asio::error::eof) {
                // Endopint was closed. The connection is set to defunct state and should not throw.
                _defunct = true;
                is_disposed->value = true;
                log(CQL_LOG_ERROR, "error reading body " + err.message());
                return;
            }
            
            log(CQL_LOG_ERROR, "error reading body " + err.message());
            check_transport_err(err);

            header_read(); // loop
            return;
        }

        cql::cql_error_t consume_error;
        if (!_response_message->consume(&consume_error)) {
            log(CQL_LOG_ERROR, "error deserializing result message " + consume_error.message);

            header_read();
            return;
        }

        switch (header.opcode()) {
        case CQL_OPCODE_RESULT:
        {
            log(CQL_LOG_DEBUG, "received result message " + header.str());

            cql_stream_t stream = header.stream();

            if (!_callback_storage.has_callbacks(stream)) {
                log(CQL_LOG_INFO, "no callback found for message " + header.str());
            }
            else {
                callback_pair_t callback_pair = _callback_storage.get_callbacks(stream);

                cql::cql_message_result_impl_t* response_message =
                    dynamic_cast<cql::cql_message_result_impl_t*>(_response_message.release());

                preprocess_result_message(response_message);
                release_stream(stream);
                callback_pair.first(*this, header.stream(), response_message);
            }
            break;
        }

        case CQL_OPCODE_EVENT:
            log(CQL_LOG_DEBUG, "received event message");
            if (_event_callback) {
                cql_message_event_impl_t* event
                    = dynamic_cast<cql::cql_message_event_impl_t*>(_response_message.release());

                if ((event->topology_change() == CQL_EVENT_TOPOLOGY_REMOVE_NODE
                        || event->status_change() == CQL_EVENT_STATUS_DOWN)
                        && cql_host_t::ip_address_t::from_string(event->ip()) == _endpoint.address()) {
                    // The event says that the endpoint for this connection is dead.
                    _is_disposed->value = true;
                    close();
                }
                
                _io_service.post(boost::bind(_event_callback,
                                             boost::ref(*this),
                                             event));
            }
            break;

        case CQL_OPCODE_ERROR:
        {
            cql_stream_t stream = header.stream();

            if (!_callback_storage.has_callbacks(stream)) {
                log(CQL_LOG_INFO, "no callback found for message " + header.str() + " " + _response_message->str());
            }
            else {
                callback_pair_t callback_pair = _callback_storage.get_callbacks(stream);
                release_stream(stream);

                cql::cql_message_error_impl_t* m =
                    dynamic_cast<cql::cql_message_error_impl_t*>(_response_message.get());

                cql::cql_error_t cql_error =
                    cql::cql_error_t::cassandra_error(m->code(), m->message());

                callback_pair.second(*this, stream, cql_error);
            }
            break;
        }

        case CQL_OPCODE_READY:
            log(CQL_LOG_DEBUG, "received ready message");
            _ready = true;
            if (_connect_callback) {
                // let the caller know that the connection is ready
                _connect_callback(*this);
            }
            break;

        case CQL_OPCODE_SUPPORTED:
            log(CQL_LOG_DEBUG, "received supported message " + _response_message->str());
            startup_write();
            break;

        case CQL_OPCODE_AUTHENTICATE:
            credentials_write();
            break;

        default:
            log(CQL_LOG_ERROR, "unhandled opcode " + header.str());
            break;
        }

        header_read(); // loop
    }

    void
    options_write()
    {
        cql::cql_message_options_impl_t messageOption;
		create_request(
            &messageOption,
            (boost::function<void (const boost::system::error_code &, std::size_t)>)boost::bind(
                &cql_connection_impl_t::write_handle,
                this,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred),
            _reserved_stream);

        // start listening
        header_read();
    }

    void
    startup_write()
    {
        cql::cql_message_startup_impl_t m;
        m.version(CQL_VERSION_IMPL);
        create_request(
            &m,
            boost::bind(&cql_connection_impl_t::write_handle,
                        this,
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred),
            _reserved_stream);
    }

    void
    credentials_write()
    {
        cql::cql_message_credentials_impl_t m;
        m.credentials(_credentials);
        create_request(
            &m,
            boost::bind(&cql_connection_impl_t::write_handle,
                        this,
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred),
            _reserved_stream);
    }

    inline void
    check_transport_err(const boost::system::error_code& err)
    {
        if (!_closing && !_transport->lowest_layer().is_open()) {
            _ready = false;
            _defunct = true;
        }

        if (_connect_errback && !_closing) {
            cql::cql_error_t e;
            e.transport = true;
            e.code = err.value();
            e.message = err.message();
            _connect_errback(*this, e);
        }
    }
	
	boost::mutex                             _mutex;
    
    boost::asio::io_service&                 _io_service;
    boost::asio::strand                      _strand;
    
    cql_endpoint_t                           _endpoint;
    boost::asio::ip::tcp::resolver           _resolver;
    std::auto_ptr<TSocket>                   _transport;
    cql::cql_stream_id_t                     _stream_counter;
    cql::cql_header_impl_t                   _response_header;
    std::auto_ptr<cql::cql_message_t>        _response_message;
    callback_storage_t                       _callback_storage;
    int                                      _number_of_free_stream_ids;
    cql_connection_callback_t                _connect_callback;
    cql_connection_errback_t                 _connect_errback;
    cql_log_callback_t                       _log_callback;
    bool                                     _events_registered;
    std::list<std::string>                   _events;
    cql_event_callback_t                     _event_callback;
    cql::cql_connection_t::cql_credentials_t _credentials;
    bool                                     _defunct;
    bool                                     _ready;
    bool                                     _closing;
    cql_stream_t                             _reserved_stream;
    cql_uuid_t                               _uuid;
	boost::shared_ptr<boolkeeper>            _is_disposed;
    
    std::string                              _current_keyspace_name,
                                             _selected_keyspace_name;
    
    std::vector<std::string>                 _stream_id_vs_query_string;

    cql_prepare_statements_t                 _prepare_statements;
    
    cql_session_impl_t*                      _session_ptr;
};

} // namespace cql

#endif // CQL_CLIENT_IMPL_H_
