#include <boost/format.hpp>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_authentication_exception.hpp"

std::string
cql::cql_authentication_exception::create_message(
        const std::string& message, 
        const boost::asio::ip::address& ip_address) 
{
    return boost::str(boost::format("Authentication error on host %1%: %2%")
        % _ip_address.to_string()
        % message);
}
