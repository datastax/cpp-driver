#include <string>
#include <boost/format.hpp>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_read_timeout_exception.hpp"

std::string
cql::cql_read_timeout_exception::create_message(
        cql::cql_consistency_enum consistency_level, 
        cql::cql_int_t received, 
        cql::cql_int_t required,
        bool data_present)
{
    return boost::str(
        boost::format("Cassandra timeout during read query at consistency %1% (%2%)")
            % to_string(consistency_level)
            % get_message_details(received, required, data_present)
            );
}

std::string
cql::cql_read_timeout_exception::get_message_details(
    cql::cql_int_t received, 
    cql::cql_int_t required,
    bool data_present)
{
    if (received < required)
        return boost::str(boost::format("%1% replica responded over %2% required") % received % required);
    else if (!data_present)
        return "the replica queried for data didn't responded";
    else
        return "timeout while waiting for repair of inconsistent replica";

}
    