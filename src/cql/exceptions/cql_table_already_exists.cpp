#include <boost/format.hpp>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_table_already_exists.hpp"

cql::cql_table_already_exists_exception::cql_table_already_exists_exception(const char* table_name) 
    : cql::cql_query_validation_exception(create_message("", table_name).c_str())
{ }

cql::cql_table_already_exists_exception::cql_table_already_exists_exception(const char* keyspace, const char* table_name) 
    : cql::cql_query_validation_exception(create_message(keyspace, table_name).c_str())
{ }

std::string
cql::cql_table_already_exists_exception::create_message(
        const char* table_name, 
        const char* keyspace) 
{
    table_name = empty_when_null(table_name);
    keyspace = empty_when_null(keyspace);
    
    const char* format_string = *keyspace 
        ? "Table '%1%.%2%' already exists."
        : "Table '%1%%2%' already exists.";
    
    return boost::str(boost::format(format_string) % keyspace % table_name);
}