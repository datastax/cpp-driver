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
#include "cql/policies/cql_retry_policy.hpp"

namespace cql {
    
class cql_query_t
    : private boost::noncopyable 
{
public:
    cql_query_t()
        : _consistency(CQL_CONSISTENCY_DEFAULT), 
          _is_traced(false),
          _retry_policy() { }
    
        cql_query_t(const cql_consistency_enum consistency,
                    const bool  is_traced = false,
                    const boost::shared_ptr<cql_retry_policy_t> retry_policy = 0)
            : _consistency(consistency)
            , _is_traced(is_traced)
            , _retry_policy(retry_policy) { }
    
    inline bool
    is_traced() const { return _is_traced; }
    
    inline void
    enable_tracing() { _is_traced = true; }
    
    inline void
    disable_tracing() { _is_traced = false; }
    
    inline cql_consistency_enum
    consistency() const { return _consistency; }

    inline void
    set_consistency(const cql_consistency_enum consistency) {
        _consistency = consistency;
    }
    
    inline boost::shared_ptr<cql_retry_policy_t>
    retry_policy() const { return _retry_policy; }
    
    inline void
    set_retry_policy(const boost::shared_ptr<cql_retry_policy_t>& retry_policy) {
        _retry_policy = retry_policy;
    }
    
    inline bool
    has_retry_policy() const { return (bool)_retry_policy; }
    
private:
    cql_consistency_enum                    _consistency;
    bool                                    _is_traced;
    boost::shared_ptr<cql_retry_policy_t>   _retry_policy;
};

}

#endif	/* CQL_QUERY_HPP */

