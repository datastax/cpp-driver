  
#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_authentication_exception.hpp"

cql::cql_authentication_exception::cql_authentication_exception(
        const char* message, 
        boost::asio::ip::address& host)
	: _ip_address(host)
{
	safe_strncpy(_user_message, empty_when_null(message), sizeof(_user_message));
}

void 
cql::cql_authentication_exception::prepare_what_buffer() const {
    snprintf(_what_buffer, sizeof(_what_buffer),
		"Authentication error on host %s: %s",
		_ip_address.to_string().c_str(),
		_user_message);
}

boost::asio::ip::address
cql::cql_authentication_exception::host() const throw() {
	return _ip_address;
}