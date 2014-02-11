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
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/exception_ptr.hpp>

namespace cql {

template<typename TResult>
class cql_promise_t {
public:
    cql_promise_t() :
        _mutex(new boost::mutex()),
        _value_set(new bool(false)),
        _promise(new boost::promise<TResult>()),
        _future(_promise->get_future()),
        _timeout_callback(0)
    {}

    // in multithraded environment only
    // one thread will succeedd in setting  promise value.
    inline bool
    set_value(
        const TResult& value)
    {
        boost::mutex::scoped_lock lock(*_mutex);
        if (!*_value_set) {
            *_value_set = true;
            _promise->set_value(value);
            return true;
        }
        return false;
    }

    typedef boost::function<void(const boost::system::error_code&)>
        timeout_callback_t;

    /** Sets the time after which the callback will be called unless promise is fulfilled. */
    bool
    set_timeout(
        boost::asio::io_service& io_service,
        const boost::posix_time::time_duration& duration,
        timeout_callback_t timeout_callback)
    {
        _timeout_callback = timeout_callback;
        
        _timer(new boost::asio::deadline_timer(io_service));
        _timer->expires_from_now(duration);
        _timer->async_wait(boost::bind(timeout_precallback, this, _1));
    }

    inline bool
    set_exception(
        const boost::exception_ptr& exception)
    {
        boost::mutex::scoped_lock lock(*_mutex);
        if (!*_value_set) {
            *_value_set = true;
            _promise->set_exception(exception);
            return true;
        }

        return false;
    }

    inline boost::shared_future<TResult>
    shared_future() const
    {
        return _future;
    }

private:
    
    void
    timeout_precallback(const boost::system::error_code&) const
    {
        if (!_value_set && _timeout_callback) {
            _timeout_callback();
        }
    }
    
    boost::shared_ptr<boost::mutex>              _mutex;
    boost::shared_ptr<bool>                      _value_set;
    boost::shared_ptr<boost::promise<TResult> >  _promise;
    boost::shared_future<TResult>                _future;
    
    boost::shared_ptr<boost::asio::deadline_timer> _timer;
    timeout_callback_t                             _timeout_callback;
};

}

#endif	/* CQL_PROMISE_HPP */
