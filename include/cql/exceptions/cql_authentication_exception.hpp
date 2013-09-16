
/*
 * File:   cql_authentication_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_AUTHENTICATION_EXCEPTION_H_
#define	CQL_AUTHENTICATION_EXCEPTION_H_

#include <string>

#include <boost/asio/ip/address.hpp>
#include "cql/exceptions/cql_generic_exception.hpp"

namespace cql {
// Indicates an error during the authentication phase while connecting to a node.
class cql_authentication_exception: public cql_generic_exception {
public:
    cql_authentication_exception(
            const char* message,
            boost::asio::ip::address& host)
        : cql_generic_exception(create_message(message, host).c_str()),
          _ip_address(host)  
        { }

	// Gets the host for which the authentication failed. 
	inline boost::asio::ip::address 
    host() const throw() {
        return _ip_address;
    }

private:
    std::string
    create_message(const char* message, const boost::asio::ip::address& ip_address);
    
	boost::asio::ip::address _ip_address;
};

}

#endif	/* CQL_AUTHENTICATION_EXCEPTION_H_ */
