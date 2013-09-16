
/*
 * File:   cql_authentication_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_AUTHENTICATION_EXCEPTION_H_
#define	CQL_AUTHENTICATION_EXCEPTION_H_

#include <boost/asio/ip/address.hpp>
#include "cql/exceptions/cql_exception.hpp"

namespace cql {
// Indicates an error during the authentication phase while connecting to a node.
class cql_authentication_exception: public cql_exception {
public:
    cql_authentication_exception(
		const char* message,
		boost::asio::ip::address& host);

	// Gets the host for which the authentication failed. 
	boost::asio::ip::address host() const throw();

protected:
    // @override
    virtual void prepare_what_buffer() const;
    
private:
	boost::asio::ip::address _ip_address;
	char _user_message[CQL_EXCEPTION_BUFFER_SIZE];
};

}

#endif	/* CQL_AUTHENTICATION_EXCEPTION_H_ */
