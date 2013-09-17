#ifndef CQL_HOSTS_HPP_
#define CQL_HOSTS_HPP_

#include <boost/shared_ptr.hpp>
#include <boost/asio/ip/address.hpp>

#include "cql/cql_reconnection_policy.hpp"

namespace cql {
	class cql_hosts_t {
	public:
		bool
		add_if_not_exists_or_bring_up_if_down(
			const boost::asio::ip::address ip_address);

		bool
		set_down_if_exists(
			const boost::asio::ip::address ip_address);

		void
		remove_if_exists(
			const boost::asio::ip::address ip_address);

		static cql_hosts_t
		create(const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy);
	
	private:
		cql_hosts_t(
			const boost::shared_ptr<cql_reconnection_policy_t>& rp)
			: _reconnection_policy(rp) { }

		boost::shared_ptr<cql_reconnection_policy_t>
			_reconnection_policy;
	};
}

#endif // CQL_HOSTS_HPP_