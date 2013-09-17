#include <string>
#include <boost/format.hpp>

#include "cql/exceptions/cql_unavailable_exception.hpp"
#include "cql/internal/cql_util.hpp"

std::string
cql::cql_unavailable_exception::create_message(
    cql_consistency_enum consistency_level, 
    cql_int_t required, 
    cql_int_t alive) 
{
    return boost::str(boost::format(
        "Not enough replica available for query at consistency %1% (%2% required but only %3% alive)")
        % to_string(consistency_level)
        % required
        % alive);
}