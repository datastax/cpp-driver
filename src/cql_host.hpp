#ifndef CQL_HOST_HPP_
#define CQL_HOST_HPP_

#include <string>
#include <boost/shared_ptr.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "cql/cql.hpp"
#include "cql/cql_config.hpp"
#include "cql/cql_endpoint.hpp"
#include "cql_reconnection_policy.hpp"
#include "cql/cql_reconnection_schedule.hpp"
#include "cql/cql_builder.hpp"

namespace cql {
	class CQL_EXPORT cql_host_t {
	public:
		typedef
			boost::asio::ip::address
			ip_address_t;

		inline bool
		is_up() const
        {
            return _is_up;
        }

		inline const ip_address_t&
		address() const
        {
            return _endpoint.address();
        }

        inline unsigned short
        port() const
        {
            return _endpoint.port();
        }

        inline const cql_endpoint_t&
        endpoint() const
        {
            return _endpoint;
        }

		inline const std::string&
        datacenter() const
        {
            return _datacenter;
        }

		inline const std::string&
		rack() const
        {
            return _rack;
        }

		bool
		is_considerably_up() const;

        cql_host_distance_enum
        distance(
            const cql_policies_t& policies) const;

		bool
		set_down();

		bool
		bring_up();

		void
		set_location_info(
			const std::string& datacenter,
			const std::string& rack);

		static ::boost::shared_ptr<cql_host_t>
		create(
			const cql_endpoint_t&                               endpoint,
			const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy);
	private:
		cql_host_t(
            const cql_endpoint_t&                               endpoint,
			const boost::shared_ptr<cql_reconnection_policy_t>& reconnection_policy);

        cql_endpoint_t                                 _endpoint;
		std::string                                    _datacenter;
		std::string                                    _rack;
		bool                                           _is_up;
		boost::posix_time::ptime                       _next_up_time;
		boost::shared_ptr<cql_reconnection_policy_t>   _reconnection_policy;
		boost::shared_ptr<cql_reconnection_schedule_t> _reconnection_schedule;
	};
}

#endif // CQL_HOST_HPP_
