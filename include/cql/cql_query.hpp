/*
 * File:   cql_query.hpp
 * Author: mc
 *
 * Created on September 29, 2013, 4:39 PM
 */

#ifndef CQL_QUERY_HPP
#define	CQL_QUERY_HPP

#include <boost/noncopyable.hpp>

#include "cql/cql.hpp"
#include "cql/policies/cql_default_retry_policy.hpp"
#include "cql/cql_stream.hpp"

namespace cql {

class cql_query_t
{
public:
    cql_query_t(
        const std::string& query_string) :
        _query_string(query_string),
        _consistency(CQL_CONSISTENCY_DEFAULT),
        _is_traced(false),
        _retry_policy(new cql_default_retry_policy_t()),
        _retry_counter(0)
    {}

    cql_query_t(
        const std::string&         query_string,
        const cql_consistency_enum consistency) :
        _query_string(query_string),
        _consistency(consistency),
        _is_traced(false),
        _retry_policy(new cql_default_retry_policy_t()),
        _retry_counter(0)
    {}
    
    cql_query_t(
        const std::string&         query_string,
        const cql_consistency_enum consistency,
        const bool                 is_traced) :
        _query_string(query_string),
        _consistency(consistency),
        _is_traced(is_traced),
        _retry_policy(new cql_default_retry_policy_t()),
        _retry_counter(0)
    {}

    cql_query_t(
        const std::string&                          query_string,
        const cql_consistency_enum                  consistency,
        const bool                                  is_traced,
        const boost::shared_ptr<cql_retry_policy_t> retry_policy) :
        _query_string(query_string),
        _consistency(consistency),
        _is_traced(is_traced),
        _retry_policy(retry_policy),
        _retry_counter(0)
    {}

    inline bool
    is_traced() const
    {
        return _is_traced;
    }

    inline void
    enable_tracing()
    {
        _is_traced = true;
    }

    inline void
    disable_tracing()
    {
        _is_traced = false;
    }

    inline cql_consistency_enum
    consistency() const
    {
        return _consistency;
    }

    inline void
    set_consistency(
        const cql_consistency_enum consistency)
    {
        _consistency = consistency;
    }

    inline boost::shared_ptr<cql_retry_policy_t>
    retry_policy() const
    {
        return _retry_policy;
    }

    inline void
    set_retry_policy(
        const boost::shared_ptr<cql_retry_policy_t>& retry_policy)
    {
        _retry_policy = retry_policy;
    }

    inline bool
    has_retry_policy() const {
        return (bool)_retry_policy;
    }

	inline const cql_stream_t&
	stream() const
    {
        return _stream;
    }

	inline void
	set_stream(
        const cql_stream_t& stream)
    {
		_stream = stream;
	}

	inline const std::string&
	query() const
    {
        return _query_string;
    }

	inline void
	set_query(
        const std::string& query_string)
    {
        _query_string = query_string;
    }
    
    inline void
    increment_retry_counter() {
        ++_retry_counter;
    }
    
    inline int
    get_retry_counter() const {
        return _retry_counter;
    }

private:
	std::string							  _query_string;
    cql_consistency_enum                  _consistency;
    bool                                  _is_traced;
    boost::shared_ptr<cql_retry_policy_t> _retry_policy;
	cql_stream_t						  _stream;
    int                                   _retry_counter;
};

}

#endif	/* CQL_QUERY_HPP */
