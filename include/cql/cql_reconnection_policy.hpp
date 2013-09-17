#ifndef CQL_RECONNECTION_POLICY_HPP_
#define CQL_RECONNECTION_POLICY_HPP_

#include <boost/shared_ptr.hpp>
#include <cql/cql_config.hpp>
#include <cql/cql_reconnection_schedule.hpp>

namespace cql {
	// Policy that decides how often the reconnection to a dead node is attempted.
	//  Each time a node is detected dead (because a connection error occurs), a new
	//  cql_reconnection_schedule_t instance is created (through the
	//  new_schedule()). Then each call to the
	//  cql_reconnection_schedule_t::get_delay() method of this instance will
	//  decide when the next reconnection attempt to this node will be tried. Note
	//  that if the driver receives a push notification from the Cassandra cluster
	//  that a node is UP, any existing cql_reconnection_schedule_t on that
	//  node will be canceled and a new one will be created (in effect, the driver
	//  reset the scheduler). The default cql_exponential_reconnection_policy
	//  policy is usually adequate.
	class DLL_PUBLIC cql_reconnection_policy_t {
	public:
		//  Creates a new schedule for reconnection attempts.
		virtual boost::shared_ptr<cql_reconnection_schedule_t>
		new_schedule() = 0;

		virtual ~cql_reconnection_policy_t() { }
	};
}

#endif // CQL_RECONNECTION_POLICY_HPP_