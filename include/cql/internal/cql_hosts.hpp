#ifndef CQL_HOSTS_HPP_
#define CQL_HOSTS_HPP_

#include <vector>

#include <boost/shared_ptr.hpp>
#include <boost/asio/ip/address.hpp>

#include "cql/cql_host.hpp"
#include "cql/policies/cql_reconnection_policy.hpp"
#include "cql/lockfree/boost_ip_address_traits.hpp"
#include "cql/lockfree/cql_lockfree_hash_map.hpp"

namespace cql {
	class cql_hosts_t {
		typedef 
			::boost::asio::ip::address
			ip_addr;

		typedef
			::boost::shared_ptr<cql_host_t>
			host_ptr_t;

	public:

		inline bool
		try_get(const ip_addr& ip_address, host_ptr_t* host) {
			if(!host)
				throw std::invalid_argument("host cannot be null.");

			return _hosts.try_get(ip_address, host);
		}

		// Concurrently returns list of hosts. 
		// This may NOT return all hosts, due to concurrency issues.
		// How to use:
		// std::vector<> hosts;
		// this_class->get_hosts(&hosts);
		// Thorws std::invalid_argument when passed pointer is null.
		void
		get_hosts(std::vector<host_ptr_t>* empty_vector);

		// See get_hosts comment.
		void
		get_addresses(std::vector<ip_addr>* empty_vector);

		bool
		add_if_not_exists_or_bring_up_if_down(const ip_addr& ip_address);

		bool
		set_down_if_exists(const ip_addr& ip_address);

		void
		remove_if_exists(const ip_addr& ip_address);

		// expected_load - specify how many host you expect will be used
		// by this class. This can speed up certain operations like host lookup.
		static ::boost::shared_ptr<cql_hosts_t>
		create(
			const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy,
			const size_t expected_load = 1024);
	
	private:
		cql_hosts_t(
			const boost::shared_ptr<cql_reconnection_policy_t>& rp,
			const size_t expected_size)
			: _reconnection_policy(rp),
			  _hosts(expected_size) { }

		boost::shared_ptr<cql_reconnection_policy_t>
			_reconnection_policy;

		cql_lockfree_hash_map_t<ip_addr, host_ptr_t> _hosts;
	};
}

#endif // CQL_HOSTS_HPP_