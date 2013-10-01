/* 
 * File:   cql_promise.hpp
 * Author: mc
 *
 * Created on October 1, 2013, 2:31 PM
 */

#ifndef CQL_PROMISE_HPP_
#define	CQL_PROMISE_HPP_

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/exception_ptr.hpp>

#include "cql/internal/cql_util.hpp"

namespace cql {

template<typename TResult>
class cql_promise_t {
public:
    cql_promise_t()
        : _value_set(new boost::atomic_bool(false))
        , _promise(new boost::promise<TResult>())
        , _future(_promise->get_future()) { } 
    
    // in multithraded environment only
    // one thread will succeedd in setting  promise value.
    inline bool
    set_value(const TResult& value) {
        if(try_acquire_lock(_value_set)) {
            _promise->set_value(value);
            return true;
        }
        return false;
    }
    
    inline bool
    set_exception(const boost::exception_ptr& exception) {
        if(try_acquire_lock(_value_set)) {
            _promise->set_exception(exception);
            return true;
        }
        
        return false;
    }
   
   inline boost::shared_future<TResult>
   shared_future() const {
       return _future;
   }
    
private:
    boost::shared_ptr<boost::atomic_bool>        _value_set;
    boost::shared_ptr<boost::promise<TResult> >  _promise;
    boost::shared_future<TResult>                _future;
};

}

#endif	/* CQL_PROMISE_HPP */

