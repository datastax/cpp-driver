#ifndef CQL_HOST_HPP_
#define CQL_HOST_HPP_

#include <string>
#include <boost/shared_ptr.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "cql/cql.hpp"
#include "cql/cql_config.hpp"
#include "cql/policies/cql_reconnection_policy.hpp"
#include "cql/cql_reconnection_schedule.hpp"

namespace cql {
	class DLL_PUBLIC cql_host_t {
	public:
		typedef 
			boost::asio::ip::address
			ip_address;

		inline bool 
		is_up() const { return _is_up; }
		
		inline ip_address
		address() const { return _ip_address; }
		
		inline const std::string&
		datacenter() const { return _datacenter; }
		
		inline const std::string&
		rack() const { return _rack; }
		
		bool
		is_considerably_up() const;
		
		bool
		set_down();
		
		bool
		bring_up_if_down();
		
		void
		set_location_info(
			const std::string& datacenter,
			const std::string& rack);
		
		static ::boost::shared_ptr<cql_host_t>
		create(
			const ip_address& address, 
			const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy);
	private:
		cql_host_t(
			const ip_address& address, 
			const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy);

		boost::posix_time::ptime utc_now() const;

		ip_address	_ip_address;
		std::string _datacenter;
		std::string _rack;
		bool		_is_up;
		boost::posix_time::ptime _next_up_time;
		
		boost::shared_ptr<cql_reconnection_policy_t>
			_reconnection_policy;
			
		boost::shared_ptr<cql_reconnection_schedule_t>
			_reconnection_schedule;
	};
}

#endif // CQL_HOST_HPP_