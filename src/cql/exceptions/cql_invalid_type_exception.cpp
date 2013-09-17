#include <boost/format.hpp>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_invalid_type_exception.hpp"

using namespace boost;

std::string
cql::cql_invalid_type_exception::create_message(
        const std::string& param_name,
        const std::string& expected_type,
        const std::string& received_type) const 
{
    return str(format(
        "Received object of type: %1%, expected type: %2%. " 
        "(Parameter name that caused exception: %s)")
			% received_type
			% expected_type
			% param_name);
}