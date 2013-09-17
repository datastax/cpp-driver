#include <boost/format.hpp>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_table_already_exists.hpp"

cql::cql_table_already_exists_exception::
	cql_table_already_exists_exception(const std::string& table_name) 
    : cql::cql_query_validation_exception(create_message("", table_name))
{ }

cql::cql_table_already_exists_exception::
	cql_table_already_exists_exception(const std::string& keyspace, const std::string& table_name) 
    : cql::cql_query_validation_exception(create_message(keyspace, table_name))
{ }

std::string
cql::cql_table_already_exists_exception::create_message(
	const std::string& table_name, 
	const std::string& keyspace) 
{   
    const char* format_string = keyspace.empty() 
        ? "Table '%1%%2%' already exists."
        : "Table '%1%.%2%' already exists.";
    
    return boost::str(boost::format(format_string) % keyspace % table_name);
}