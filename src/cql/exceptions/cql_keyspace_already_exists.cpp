#include <boost/format.hpp>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_keyspace_already_exists_exception.hpp"
#include "cql/exceptions/cql_exception.hpp"

using namespace boost;

cql::cql_keyspace_already_exists_exception::cql_keyspace_already_exists_exception(const std::string& keyspace) 
    : cql_query_validation_exception(create_message(keyspace))
{ }

std::string
cql::cql_keyspace_already_exists_exception::create_message(const std::string& keyspace) {
    return str(format("Keyspace '%1%' already exists.") % keyspace);
}
