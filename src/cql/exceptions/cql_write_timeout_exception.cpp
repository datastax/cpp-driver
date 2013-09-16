#include <string>
#include <boost/format.hpp>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_write_timeout_exception.hpp"

std::string
cql::cql_write_timeout_exception::create_message(
    cql::cql_consistency_enum consistency_level, 
    cql::cql_int_t received, 
    cql::cql_int_t required)
{
    return boost::str(boost::format(
        "Cassandra timeout during write query at consitency %1% "
        "(%2% replica acknowledged the write over %3% required)")
            % get_consistency_string(consistency_level)
            % received
            % required);
}
