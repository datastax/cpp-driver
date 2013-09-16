#include <boost/format.hpp>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_keyspace_already_exists_exception.hpp"
#include "cql/exceptions/cql_exception.hpp"

cql::cql_keyspace_already_exists_exception::cql_keyspace_already_exists_exception(const char* keyspace) 
    : cql_query_validation_exception(create_message(keyspace).c_str())
{ }

std::string
cql::cql_keyspace_already_exists_exception::create_message(const char* keyspace) {
    return boost::str(boost::format("Keyspace '%1%' already exists.") % empty_when_null(keyspace));
}
