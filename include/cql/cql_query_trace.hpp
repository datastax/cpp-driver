#ifndef CQL_QUERY_TRACE_HPP
#define	CQL_QUERY_TRACE_HPP

#include "cql/cql.hpp"
#include "cql/cql_map.hpp"
#include "cql/cql_connection.hpp"
#include "cql/cql_uuid.hpp"

#include <boost/noncopyable.hpp>
#include <boost/asio/ip/address.hpp>

#include <string>
#include <list>

namespace cql {

class cql_session_t;
class cql_query_t;
    
//-------------------------------------------------------------------------------------
    
struct CQL_EXPORT cql_trace_event_t {
    
    cql_trace_event_t(
        const std::string               name_,
        cql::cql_bigint_t               timestamp_,
        const boost::asio::ip::address& source_,
        cql::cql_bigint_t               source_elapsed_,
        const std::string&              thread_name_)
            : name(name_),
             timestamp(timestamp_),
             source(source_),
             source_elapsed(source_elapsed_),
             thread_name(thread_name_) {}

    std::string
    to_string() const;
    
    std::string              name;
    cql_bigint_t             timestamp;
    boost::asio::ip::address source;
    cql_bigint_t             source_elapsed;
    std::string              thread_name;
};
    
//-------------------------------------------------------------------------------------
    
class CQL_EXPORT cql_query_trace_t :
    boost::noncopyable
{
public:
    cql_query_trace_t(
        const cql_uuid_t&                    trace_id,
        boost::shared_ptr<cql_session_t>     session,
        cql_connection_t::cql_log_callback_t log_callback = 0)
            : _trace_id(trace_id),
              _session(session),
              _log_callback(log_callback),
              _duration(UNAVAILABLE_YET),
              _started_at(0) {}
    
    cql_uuid_t
    get_trace_id() const {
        return _trace_id;
    }

    /** If the result is unavailable yet, the function returns empty string. */
    inline std::string
    get_request_type() {
        maybe_fetch_trace();
        return _request_type;
    }
    
    /** If the result is unavailable yet, the function returns cql_query_trace_t::UNAVAILABLE_YET. */
    inline cql_bigint_t
    get_duration_micros() {
        maybe_fetch_trace();
        return _duration;
    }
    
    /** If the result is unavailable yet, the function returns empty IP address. */
    inline boost::asio::ip::address
    get_coordinator() {
        maybe_fetch_trace();
        return _coordinator;
    }
    
    /** Returns false if the result is unavailable yet.
        Otherwise, the argument pointer points to the map of parameters.
        Caller (you) DOES NOT have the ownership of this pointer! 
        Lifetime of the underlying map equals the lifetime of this cql_query_trace_t object. */
    inline bool
    get_parameters(boost::shared_ptr<cql_map_t>& parameters_) {
        if (maybe_fetch_trace()) {
            parameters_ = _parameters;
            return true;
        }
        else return false;
    }
    
    /** If the result is unavailable yet, the function returns zero. */
    inline cql_bigint_t
    started_at() {
        maybe_fetch_trace();
        return _started_at;
    }
    
    /** Returns false if the result is unavailable yet.
        Otherwise, the list of events is written under the reference argument.
        Any content of the list gets disregarded. */
    inline bool
    get_events(std::list<cql_trace_event_t>& events_) {
        if (maybe_fetch_trace()) {
            events_ = _events; // Copy the contents of a list.
            return true;
        }
        else return false;
    }
    
    std::string
    to_string() const;
    
private:
    
    /** Returns true if trace is available; false otherwise. */
    bool
    maybe_fetch_trace();
    
    /** Returns true if trace is available; false otherwise. */
    bool
    do_fetch_trace();
    
    boost::shared_ptr<cql_query_t>
    select_sessions_query() const;
    
    boost::shared_ptr<cql_query_t>
    select_events_query() const;
    
    static const cql_bigint_t            UNAVAILABLE_YET = -1;
    
    boost::mutex                         _mutex;
    
    cql_uuid_t                           _trace_id;
    boost::shared_ptr<cql_session_t>     _session;
    cql_connection_t::cql_log_callback_t _log_callback;
    
    std::string                          _request_type;
    cql_bigint_t                         _duration;
    boost::asio::ip::address             _coordinator;
    boost::shared_ptr<cql_map_t>         _parameters;
    std::list<cql_trace_event_t>         _events;
    cql_bigint_t                         _started_at;
};

} // namespace cql

#endif	/* CQL_QUERY_TRACE_HPP */
