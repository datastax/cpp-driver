
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
#include "cql_exception.hpp"

namespace cql {
// Indicates an error during the authentication phase while connecting to a node.
class CQL_EXPORT cql_authentication_exception: public cql_exception {
public:
    cql_authentication_exception(
            const std::string& message,
            boost::asio::ip::address& host)
        : cql_exception(create_message(message, host)),
          _ip_address(host)  
        { }

	// Gets the host for which the authentication failed. 
	inline boost::asio::ip::address 
    host() const throw() {
        return _ip_address;
    }

private:
    std::string
    create_message(const std::string& message, const boost::asio::ip::address& ip_address);
    
	boost::asio::ip::address _ip_address;
};

}

#endif	/* CQL_AUTHENTICATION_EXCEPTION_H_ */
