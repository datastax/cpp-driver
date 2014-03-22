#ifndef CQL_QUERY_TRACE_HPP
#define	CQL_QUERY_TRACE_HPP

#include "cql/cql.hpp"
#include "cql/cql_map.hpp"
#include "cql/exceptions/cql_exception.hpp"

#include <boost/format.hpp>
#include <boost/noncopyable.hpp>

#include <cstring>
#include <list>

namespace cql {

struct cql_trace_id_t { // TODO (JS) this should (?) be cql_uuid_t
    cql_trace_id_t(const std::string& trace_id_string_)
        : trace_id_string(trace_id_string_) {}
    
    std::string trace_id_string;
};
    
//-------------------------------------------------------------------------------------
    
struct cql_trace_event_t {
    
    cql_trace_event_t(
        const std::string               name_,
        cql_bigint_t                    timestamp_,
        const boost::asio::ip::address& source_,
        cql_bigint_t                    source_elapsed_,
        const std::string&              thread_name_)
            : name(name_),
              timestamp(timestamp_),
              source(source_),
              source_elapsed(source_elapsed_),
              thread_name(thread_name_) {}
    
    std::string
    to_string() {
        return boost::str(
                boost::format("%1% on %2%[%3%] at %4%")
                    % name % source % thread_name % timestamp);
    }
    
    std::string              name;
    cql_bigint_t             timestamp;
    boost::asio::ip::address source;
    cql_bigint_t             source_elapsed;
    std::string              thread_name;
};
    
//-------------------------------------------------------------------------------------
    
class cql_query_trace_t :
    boost::noncopyable // Required as long as some raw pointers are used inside.
{
public:
    cql_query_trace_t(
        const cql_trace_id_t&                trace_id,
        boost::shared_ptr<cql_session_t>     session,
        cql_connection_t::cql_log_callback_t log_callback = 0)
            : _trace_id(trace_id),
              _session(session),
              _log_callback(log_callback),
              _duration(UNAVAILABLE_YET),
              _parameters(NULL),
              _started_at(0) {}
    
    virtual ~cql_query_trace_t() {
        if (_parameters) {
            delete _parameters;
        }
    }
    
    cql_trace_id_t
    get_trace_id() const {
        return _trace_id;
    }

    /** If the result is unavailable yet, the function returns empty string. */
    std::string
    get_request_type() {
        maybe_fetch_trace();
        return _request_type;
    }
    
    /** If the result is unavailable yet, the function returns cql_query_trace_t::UNAVAILABLE_YET. */
    cql_bigint_t
    get_duration_micros() {
        maybe_fetch_trace();
        return _duration;
    }
    
    /** If the result is unavailable yet, the function returns empty IP address. */
    boost::asio::ip::address
    get_coordinator() {
        maybe_fetch_trace();
        return _coordinator;
    }
    
    /** Returns false if the result is unavailable yet.
        Otherwise, the argument pointer points to the map of parameters.
        Caller (you) DOES NOT have the ownership of this pointer! 
        Lifetime of the underlying map equals the lifetime of this cql_query_trace_t object. */
    bool
    get_parameters(cql_map_t** parameters_) { // TODO(JS): some smart ptr here?
        if (maybe_fetch_trace()) {
            *parameters_ = _parameters;
            return true;
        }
        else return false;
    }
    
    /** If the result is unavailable yet, the function returns zero. */
    cql_bigint_t
    started_at() {
        maybe_fetch_trace();
        return _started_at;
    }
    
    /** Returns false if the result is unavailable yet.
        Otherwise, the list of events is written under the reference argument.
        Any content of the list gets disregarded. */
    bool
    get_events(std::list<cql_trace_event_t>& events_) {
        if (maybe_fetch_trace()) {
            events_ = _events; // Copy the contents of a list.
            return true;
        }
        else return false;
    }
    
    std::string
    to_string() const {
        return boost::str(
                boost::format("%1% [%2%] - %3%us") % _request_type % _trace_id.trace_id_string % _duration);
    }
    
private:
    
    /** Returns true if trace is available; false otherwise. */
    bool
    maybe_fetch_trace() {
        if (_duration != UNAVAILABLE_YET) {
            return true;
        }
        boost::mutex::scoped_lock lock(_mutex);
        if (_duration == UNAVAILABLE_YET) {
            return do_fetch_trace();
        }
        else return true;
    }
    
    /** Returns true if trace is available; false otherwise. */
    bool
    do_fetch_trace() {
        try {
            {// Scope limiter
                boost::shared_future<cql_future_result_t> future_result
                    = _session->query(select_sessions_query());
            
                if (!(future_result.timed_wait(boost::posix_time::seconds(30)))) {
                    if (_log_callback) {
                        _log_callback(CQL_LOG_ERROR,
                                      "Query against system_traces.sessions timed out");
                    }
                    return false;
                }
                else {
                    cql_future_result_t query_result = future_result.get();
                
                    if (query_result.error.is_err()) {
                        if (_log_callback) {
                            _log_callback(CQL_LOG_ERROR,
                                          "Error while querying system_traces.sessions: " + query_result.error.message);
                        }
                        return false;
                    }
                
                    boost::shared_ptr<cql::cql_result_t> result = query_result.result;
                    while (result->next()) {
                        bool is_empty = true;
                        if (!result->get_nullity("request", is_empty) && !is_empty) {
                            result->get_string("request", _request_type);
                        }
                        if (!result->get_nullity("duration", is_empty) && !is_empty) {
                            result->get_bigint("duration", _duration);
                        }
                        if (!result->get_nullity("coordinator", is_empty) && !is_empty) {
                        // TODO(JS): deserialize ip addresses at last.
                        //    result->get_ip_address("coordinator", _coordinator);
                        }
                        if (!result->get_nullity("parameters", is_empty) && !is_empty) {
                            result->get_map("parameters", &_parameters);
                        }
                        if (!result->get_nullity("started_at", is_empty) && !is_empty) {
                            result->get_bigint("started_at", _started_at);
                        }
                        break;
                    }
                }
            }// End of scope limiter
            {// Scope limiter
                _events.clear();
                
                boost::shared_future<cql_future_result_t> future_result
                    = _session->query(select_events_query());
            
                if (!(future_result.timed_wait(boost::posix_time::seconds(30)))) {
                    if (_log_callback) {
                        _log_callback(CQL_LOG_ERROR,
                                      "Query against system_traces.events timed out");
                    }
                    return false;
                }
                else {
                    cql_future_result_t query_result = future_result.get();
                
                    if (query_result.error.is_err()) {
                        if (_log_callback) {
                            _log_callback(CQL_LOG_ERROR,
                                          "Error while querying system_traces.events: " + query_result.error.message);
                        }
                        return false;
                    }
                
                    boost::shared_ptr<cql::cql_result_t> result = query_result.result;
                    while (result->next()) {
                        std::string activity, thread, event_id;
                        cql_bigint_t source_elapsed = 0;
                        boost::asio::ip::address source;
                        
                        bool is_empty = true;
                        
                        if (!result->get_nullity("activity", is_empty) && !is_empty) {
                            result->get_string("activity", activity);
                        }
                        if (!result->get_nullity("event_id", is_empty) && !is_empty) {
                            result->get_string("event_id", event_id); // TODO (JS) this should (?) be cql_uuid_t
                        }
                        if (!result->get_nullity("source", is_empty) && !is_empty) {
                            // TODO(JS) :
                            // result->get_ip_address("source", source);
                        }
                        if (!result->get_nullity("source_elapsed", is_empty) && !is_empty) {
                            result->get_bigint("source_elapsed", source_elapsed);
                        }
                        if (!result->get_nullity("thread", is_empty) && !is_empty) {
                            result->get_string("thread", thread);
                        }
                        
                        cql_bigint_t timestamp = 0; // TODO (JS) this should (?) be cql_uuid_t
                        cql_trace_event_t event(activity, timestamp, source, source_elapsed, thread);
                        _events.push_back(event);
                    }
                }
            }// End of scope limiter
        }
        catch (cql_exception& e) {
            if (_log_callback) {
                _log_callback(CQL_LOG_ERROR,
                              std::string("Unexpected exception while fetching query trace ") + e.what());
            }
        }
        return true;
    }
    
    boost::shared_ptr<cql_query_t>
    select_sessions_query() const {
        std::string query_text = boost::str(boost::format(
                                    "SELECT * FROM system_traces.sessions WHERE session_id = %s")
                                    % _trace_id.trace_id_string);
        return boost::shared_ptr<cql_query_t>(new cql_query_t(query_text));
    }
    
    boost::shared_ptr<cql_query_t>
    select_events_query() const {
        std::string query_text = boost::str(boost::format(
                                    "SELECT * FROM system_traces.events WHERE session_id = %s")
                                    % _trace_id.trace_id_string);
        return boost::shared_ptr<cql_query_t>(new cql_query_t(query_text));
    }
    
    static const cql_bigint_t            UNAVAILABLE_YET = -1;
    
    boost::mutex                         _mutex;
    
    cql_trace_id_t                       _trace_id;
    boost::shared_ptr<cql_session_t>     _session;
    cql_connection_t::cql_log_callback_t _log_callback;
    
    std::string                          _request_type;
    cql_bigint_t                         _duration;
    boost::asio::ip::address             _coordinator;
    cql_map_t*                           _parameters;
    std::list<cql_trace_event_t>         _events;
    cql_bigint_t                         _started_at;
};

} // namespace cql

#endif	/* CQL_QUERY_TRACE_HPP */
