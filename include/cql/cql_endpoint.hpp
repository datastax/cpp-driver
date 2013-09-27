/* 
 * File:   cql_endpoint_t.hpp
 * Author: mc
 *
 * Created on September 27, 2013, 10:21 AM
 */

#ifndef CQL_ENDPOINT_HPP_
#define	CQL_ENDPOINT_HPP_

#include <boost/asio/ip/address.hpp>
#include "cql/lockfree/boost_ip_address_traits.hpp"

namespace cql {
    
    class cql_endpoint_t {
    public:
        cql_endpoint_t(
            const ::boost::asio::ip::address& address,
            unsigned short                    port)
                : _address(address), _port(port) { }
        
        inline const ::boost::asio::ip::address&
        address() const { return _address; }
                
        inline unsigned short
        port() const { return _port; }
        
        inline bool
        is_unspecified() const {
            return _address.is_unspecified();
        }
        
        friend bool 
        operator ==(const cql_endpoint_t& left, const cql_endpoint_t& right) {
            return (left._port == right._port) 
                && (left._address == right._address);
        }
        
        friend bool
        operator !=(const cql_endpoint_t& left, const cql_endpoint_t& right) {
            return (left._port != right._port) 
                || (left._address != right._address);
        }
        
    private:
        ::boost::asio::ip::address      _address;
        unsigned short                  _port;
    };
    
}

namespace std {

	template<>
	struct hash< ::cql::cql_endpoint_t> {
	public:
		typedef 
			::cql::cql_endpoint_t
			argument_type;

		typedef 
			::std::size_t
			result_type;

		inline result_type 
		operator ()(const argument_type& endpoint) const {
            return 
                    (1234567u * endpoint.port()) 
                +   _address_hash(endpoint.address());
		}
        
    private:
        std::hash< ::boost::asio::ip::address> _address_hash;
	};

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
        std::less< ::boost::asio::ip::address>   _address_less;
    };
}

#endif	/* CQL_ENDPOINT_HPP_ */

