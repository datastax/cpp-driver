#include "cql/cql_query_trace.hpp"
#include "cql/cql_query.hpp"
#include "cql/cql_session.hpp"
#include "cql/exceptions/cql_exception.hpp"

#include <boost/format.hpp>

//-------------------------------------------------------------------------------------
std::string
cql::cql_trace_event_t::to_string() const {
    return boost::str(
            boost::format("%1% on %2%[%3%] at %4%")
                % name % source % thread_name % timestamp);
}
//------------------------------------------------------------------------------------
std::string
cql::cql_query_trace_t::to_string() const {
    return boost::str(
            boost::format("%1% [%2%] - %3%us")
                % _request_type % _trace_id.to_string() % _duration);
}
    
bool
cql::cql_query_trace_t::maybe_fetch_trace() {
    if (_duration != UNAVAILABLE_YET) {
        return true;
    }
    boost::mutex::scoped_lock lock(_mutex);
    if (_duration == UNAVAILABLE_YET) {
        return do_fetch_trace();
    }
    else return true;
}
    
bool
cql::cql_query_trace_t::do_fetch_trace() {
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
                    if (result->get_nullity("request", is_empty) && !is_empty) {
                        result->get_string("request", _request_type);
                    }
                    if (result->get_nullity("duration", is_empty) && !is_empty) {
                        result->get_bigint("duration", _duration);
                    }
                    if (result->get_nullity("coordinator", is_empty) && !is_empty) {
                    // TODO(JS): deserialize ip addresses at last.
                    //    result->get_ip_address("coordinator", _coordinator);
                    }
                    if (result->get_nullity("parameters", is_empty) && !is_empty) {
                        result->get_map("parameters", &_parameters);
                    }
                    if (result->get_nullity("started_at", is_empty) && !is_empty) {
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
                        
                    if (result->get_nullity("activity", is_empty) && !is_empty) {
                        result->get_string("activity", activity);
                    }
                    if (result->get_nullity("event_id", is_empty) && !is_empty) {
                        result->get_string("event_id", event_id); // TODO (JS) this should (?) be cql_uuid_t
                    }
                    if (result->get_nullity("source", is_empty) && !is_empty) {
                        // TODO(JS) :
                        // result->get_ip_address("source", source);
                    }
                    if (result->get_nullity("source_elapsed", is_empty) && !is_empty) {
                        result->get_bigint("source_elapsed", source_elapsed);
                    }
                    if (result->get_nullity("thread", is_empty) && !is_empty) {
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
    
boost::shared_ptr<cql::cql_query_t>
cql::cql_query_trace_t::select_sessions_query() const {
    std::string query_text = boost::str(boost::format(
                                "SELECT * FROM system_traces.sessions WHERE session_id = %s")
                                % _trace_id.to_string());
    return boost::shared_ptr<cql_query_t>(new cql_query_t(query_text));
}
    
boost::shared_ptr<cql::cql_query_t>
cql::cql_query_trace_t::select_events_query() const {
    std::string query_text = boost::str(boost::format(
                                "SELECT * FROM system_traces.events WHERE session_id = %s")
                                % _trace_id.to_string());
    return boost::shared_ptr<cql_query_t>(new cql_query_t(query_text));
}