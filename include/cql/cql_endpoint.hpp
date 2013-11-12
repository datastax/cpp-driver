/*
 * File:   cql_endpoint_t.hpp
 * Author: mc
 *
 * Created on September 27, 2013, 10:21 AM
 */

#ifndef CQL_ENDPOINT_HPP_
#define	CQL_ENDPOINT_HPP_

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>

namespace cql {

    class cql_endpoint_t
    {
    public:
        cql_endpoint_t() :
            _address(),
            _port(0)
        {}

        cql_endpoint_t(
            const ::boost::asio::ip::address& address,
            unsigned short                    port) :
            _address(address), _port(port)
        {}

        inline const ::boost::asio::ip::address&
        address() const
        {
            return _address;
        }

        inline unsigned short
        port() const
        {
            return _port;
        }

        inline boost::asio::ip::tcp::resolver::query
        resolver_query() const
        {
            return boost::asio::ip::tcp::resolver::query(
                _address.to_string(), boost::lexical_cast<std::string>(_port));
        }

        inline std::string
        to_string() const
        {
            return boost::str(boost::format("%1%:%2%") % _address % _port);
        }

        inline bool
        is_unspecified() const
        {
            return (_address == ::boost::asio::ip::address());
        }

        friend bool
        operator ==(const cql_endpoint_t& left, const cql_endpoint_t& right)
        {
            return (left._port == right._port)
                && (left._address == right._address);
        }

        friend bool
        operator !=(const cql_endpoint_t& left, const cql_endpoint_t& right)
        {
            return (left._port != right._port)
                || (left._address != right._address);
        }

    private:
        ::boost::asio::ip::address      _address;
        unsigned short                  _port;
    };

}

namespace std {

	// template<>
	// struct hash<cql::cql_endpoint_t> {
	// public:
	// 	typedef
	// 		::cql::cql_endpoint_t
	// 		argument_type;

	// 	typedef
	// 		::std::size_t
	// 		result_type;



	// 	inline result_type
	// 	operator()(
    //         const argument_type& endpoint) const
    //     {
    //         return (1234567u * endpoint.port()) + get_address_hash(endpoint.address());
	// 	}

    // private:
    //     inline result_type
    //     get_address_hash(
    //         const boost::asio::ip::address& address) const
    //     {
    //         if (address.is_v4()) {
    //             return get_ip_hash(address.to_v4().to_bytes());
    //         }
    //         else {
    //             return get_ip_hash(address.to_v6().to_bytes());
    //         }
    //     }

	// 	template<typename TBytesType>
	// 	inline result_type
	// 	get_ip_hash(
    //         const TBytesType& ip_bytes) const
    //     {
	// 		// implemented using djb2 algorithm
	// 		// see: http://www.cse.yorku.ca/~oz/hash.html

	// 		unsigned long hash = 5381;

	// 		for (typename TBytesType::const_iterator it = ip_bytes.cbegin();
    //              it != ip_bytes.cend(); ++it)
	// 		{
	// 			hash = ((hash << 5) + hash) + *it;
	// 		}

	// 		return static_cast<result_type>(hash);
	// 	}
	// };

	template<>
	struct less< ::cql::cql_endpoint_t> {
	public:
		typedef
			::cql::cql_endpoint_t
			first_argument_type;

		typedef
			::cql::cql_endpoint_t
			second_argument_type;

		typedef
			bool
			result_type;

		inline result_type
		operator ()(
            const ::cql::cql_endpoint_t& first,
            const ::cql::cql_endpoint_t& second) const
        {
            bool is_less = _address_less(first.address(), second.address());
            if(is_less)
                return true;

            bool is_greater = _address_less(second.address(), first.address());
            if(is_greater)
                return false;

            return first.port() < second.port();
		}

    private:
        std::less<boost::asio::ip::address>   _address_less;
    };
}

#endif	/* CQL_ENDPOINT_HPP_ */
