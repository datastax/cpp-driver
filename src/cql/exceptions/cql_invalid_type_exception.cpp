#include <boost/format.hpp>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_invalid_type_exception.hpp"


std::string
cql::cql_invalid_type_exception::create_message(
        const char* param_name,
        const char* expected_type,
        const char* received_type) const 
{
    return boost::str(boost::format(
        "Received object of type: %1%, expected type: %2%. " 
        "(Parameter name that caused exception: %s)")
            % empty_when_null(received_type)
            % empty_when_null(expected_type)
            % empty_when_null(param_name));
}