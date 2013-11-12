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
#include <boost/noncopyable.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/future.hpp>
#include <boost/unordered_map.hpp>
#include <boost/atomic.hpp>

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
#include "cql/cql_serialization.hpp"
#include "cql/cql_uuid.hpp"
#include "cql/cql_stream.hpp"
#include "cql/internal/cql_util.hpp"
#include "cql/internal/cql_promise.hpp"

namespace cql {

template <typename TSocket>
class cql_connection_impl_t : public cql::cql_connection_t
{
    static const int NUMBER_OF_STREAMS = 128;
    // stream with is 0 is dedicated to events messages and
    // connection management.
    static const int NUMBER_OF_USER_STREAMS = 127;

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
        _resolver(io_service),
        _transport(transport),
        _request_buffer(0),
        _callback_storage(NUMBER_OF_STREAMS),
        _numer_of_free_stream_ids(NUMBER_OF_USER_STREAMS),
        _connect_callback(0),
        _connect_errback(0),
        _log_callback(log_callback),
        _events_registered(false),
        _event_callback(0),
        _defunct(false),
        _ready(false),
        _closing(false),
        _reserved_stream(_callback_storage.acquire_stream()),
        _uuid(cql_uuid_t::create())
    {}

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

		create_request(new cql::cql_message_query_impl_t(query),
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

        _callback_storage.set_callbacks(stream, callback_pair_t(callback, errback));

        create_request(
            new cql::cql_message_prepare_impl_t(query),
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

        cql_stream_t stream = acquire_stream();
        assert(0); // TODO: change this method to use new stream allocation mechanism

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
        _closing = true;
        log(CQL_LOG_INFO, "closing connection");

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

    void
    reconnect()
    {
        _closing = false;
        _events_registered = false;
        _ready = false;
        _defunct = false;
        resolve();
    }

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
        return true;
    }

	virtual bool
    is_busy(int max) const
    {
		return (NUMBER_OF_STREAMS - _numer_of_free_stream_ids.load(boost::memory_order_acquire)) >= max;
    }

	virtual bool
    is_free(int min) const
    {
		return (NUMBER_OF_STREAMS - _numer_of_free_stream_ids.load(boost::memory_order_acquire)) <= min;
    }

    virtual bool
    is_empty() const
    {
        return (NUMBER_OF_USER_STREAMS == _numer_of_free_stream_ids.load(boost::memory_order_acquire));
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
        _request_buffer.push_back(header.buffer());

        if (header.length() != 0) {
            buf.push_back(boost::asio::buffer(message->buffer()->data(), message->size()));
            _request_buffer.push_back(message->buffer());
        }
        else {
            _request_buffer.push_back(cql_message_buffer_t());
        }

        boost::asio::async_write(*_transport, buf, callback);
    }

    void
    write_handle(
        const boost::system::error_code& err,
        std::size_t                      num_bytes)
    {
        if (!_request_buffer.empty()) {
            // the write request is complete free the request buffers
            _request_buffer.pop_front();
            if (!_request_buffer.empty()) {
                _request_buffer.pop_front();
            }
        }

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
                                boost::bind(&cql_connection_impl_t<TSocket>::header_read_handle, this, boost::asio::placeholders::error));
    }

    void
    header_read_handle(
        const boost::system::error_code& err)
    {
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
                                boost::bind(&cql_connection_impl_t<TSocket>::body_read_handle, this, header, boost::asio::placeholders::error));
    }


    void
    body_read_handle(
        const cql::cql_header_impl_t& header,
        const boost::system::error_code& err)
    {
        log(CQL_LOG_DEBUG, "received body for message " + header.str());

        if (err) {
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
                release_stream(stream);

                callback_pair.first(*this, header.stream(), dynamic_cast<cql::cql_message_result_impl_t*>(_response_message.release()));
            }
            break;
        }

        case CQL_OPCODE_EVENT:
            log(CQL_LOG_DEBUG, "received event message");
            if (_event_callback) {
                _event_callback(*this, dynamic_cast<cql::cql_message_event_impl_t*>(_response_message.release()));
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
            if (!_events_registered) {
                events_register();
            }
            else  {
                _ready = true;
                if (_connect_callback) {
                    // let the caller know that the connection is ready
                    _connect_callback(*this);
                }
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
    events_register()
    {
        std::auto_ptr<cql::cql_message_register_impl_t> m(new cql::cql_message_register_impl_t());
        m->events(_events);

        create_request(m.release(),
                       boost::bind(&cql_connection_impl_t::write_handle,
                                   this,
                                   boost::asio::placeholders::error,
                                   boost::asio::placeholders::bytes_transferred),
                        _reserved_stream);

        _events_registered = true;
    }

    void
    options_write()
    {
		create_request(
            new cql::cql_message_options_impl_t(),
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
        std::auto_ptr<cql::cql_message_startup_impl_t> m(new cql::cql_message_startup_impl_t());
        m->version(CQL_VERSION_IMPL);
        create_request(
            m.release(),
            boost::bind(&cql_connection_impl_t::write_handle,
                        this,
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred),
            _reserved_stream);
    }

    void
    credentials_write()
    {
        std::auto_ptr<cql::cql_message_credentials_impl_t> m(new cql::cql_message_credentials_impl_t());
        m->credentials(_credentials);
        create_request(
            m.release(),
            boost::bind(&cql_connection_impl_t::write_handle,
                        this,
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred),
            _reserved_stream);
    }

    inline void
    check_transport_err(const boost::system::error_code& err)
    {
        if (!_transport->lowest_layer().is_open()) {
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

    cql_endpoint_t                           _endpoint;
    boost::asio::ip::tcp::resolver           _resolver;
    std::auto_ptr<TSocket>                   _transport;
    cql::cql_stream_id_t                     _stream_counter;
    request_buffer_t                         _request_buffer;
    cql::cql_header_impl_t                   _response_header;
    std::auto_ptr<cql::cql_message_t>        _response_message;
    callback_storage_t                       _callback_storage;
    boost::atomic_int                        _numer_of_free_stream_ids;
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
};

} // namespace cql

#endif // CQL_CLIENT_IMPL_H_
