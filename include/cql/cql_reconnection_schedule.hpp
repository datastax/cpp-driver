#ifndef CQL_RECONNECTION_SCHEDULT_HPP_
#define CQL_RECONNECTION_SCHEDULT_HPP_

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>

#include "cql/cql.hpp"
#include "cql/cql_config.hpp"

namespace cql {
	//  Schedules reconnection attempts to a node.
	class CQL_EXPORT cql_reconnection_schedule_t {
	public:
		//  When to attempt the next reconnection. This method will be called once when
		//  the host is detected down to schedule the first reconnection attempt, and
		//  then once after each failed reconnection attempt to schedule the next one.
		//  Hence each call to this method are free to return a different value.
		// 
		//  @returns a time in milliseconds to wait before attempting the next reconnection.
		virtual boost::posix_time::time_duration
		get_delay() = 0;

		virtual 
        ~cql_reconnection_schedule_t() {  }
	};
}

#endif // CQL_RECONNECTION_SCHEDULT_HPP_