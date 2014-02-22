/*
 * File:   cql_exponential_reconnection_policy_t.hpp
 * Author: mc
 *
 * Created on September 26, 2013, 8:49 AM
 */

#ifndef CQL_EXPONENTIAL_RECONNECTION_POLICY_T_HPP_
#define	CQL_EXPONENTIAL_RECONNECTION_POLICY_T_HPP_

#include <boost/date_time/posix_time/posix_time.hpp>

#include "cql_reconnection_policy.hpp"

namespace cql {
    class cql_exponential_reconnection_schedule_t;

    class CQL_EXPORT cql_exponential_reconnection_policy_t
        : public cql_reconnection_policy_t
    {
    public:

        inline boost::posix_time::time_duration
        base_delay() const { return _base_delay; }

        inline boost::posix_time::time_duration
        max_delay() const { return _max_delay; }

        virtual boost::shared_ptr<cql_reconnection_schedule_t>
		new_schedule();

        cql_exponential_reconnection_policy_t(
            const boost::posix_time::time_duration& base_delay,
            const boost::posix_time::time_duration& max_delay);

    private:

        boost::posix_time::time_duration    _base_delay;
        boost::posix_time::time_duration    _max_delay;
    };

    class CQL_EXPORT cql_exponential_reconnection_schedule_t
        : public cql_reconnection_schedule_t
    {
    public:
        virtual boost::posix_time::time_duration
		get_delay();

    private:
        cql_exponential_reconnection_schedule_t(
            const cql_exponential_reconnection_policy_t& policy)
            : _policy(policy), _attempts(0),
              _last_delay(boost::posix_time::microseconds(0)) { }

        friend class cql_exponential_reconnection_policy_t;

        const cql_exponential_reconnection_policy_t     _policy;
        int                                             _attempts;
        boost::posix_time::time_duration                _last_delay;
    };
}

#endif	/* CQL_EXPONENTIAL_RECONNECTION_POLICY_T_HPP_ */
