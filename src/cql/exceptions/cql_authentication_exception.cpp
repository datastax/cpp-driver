  
#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_authentication_exception.hpp"

cql::cql_authentication_exception::cql_authentication_exception(
	const char* message, 
	boost::asio::ip::address& host)
	: cql_exception(""),
	  _ip_address(host)
{
	message = message ? message : "";
	safe_strncpy(_user_message, message, sizeof(_user_message));
}

const char* 
cql::cql_authentication_exception::what() const throw() {
	snprintf(_what_buffer, sizeof(_what_buffer),
		"Authentication error on host %s: %s",
		_ip_address.to_string().c_str(),
		_user_message);

	return _what_buffer;
}

boost::asio::ip::address
cql::cql_authentication_exception::host() const throw() {
	return _ip_address;
}