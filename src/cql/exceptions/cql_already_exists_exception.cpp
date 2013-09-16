
#include "cql/internal/cql_system_dependent.hpp"
#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_already_exists_exception.hpp"  

cql::cql_already_exists_exception::cql_already_exists_exception(
			const char* keyspace, const char* table)
			: cql::cql_exception("")	
{
	keyspace = keyspace ? keyspace : "";
	table = table ? table : "";

	safe_strncpy(_keyspace_buffer, keyspace, sizeof(_keyspace_buffer));
	safe_strncpy(_table_buffer, table, sizeof(_table_buffer));
}

const char*
cql::cql_already_exists_exception::keyspace() const throw() {
	return _keyspace_buffer;
}

const char*
cql::cql_already_exists_exception::table() const throw() {
	return _table_buffer;
}

bool
cql::cql_already_exists_exception::table_creation() const throw() {
	return (_table_buffer[0] != '\0');
}

const char*
cql::cql_already_exists_exception::what() const throw() {
	const char* format = table_creation()
		? "Table %s.%s already exists."
		: "Keyspace %s already exists.";

	// we pass more arguments that we may use, but it is safe
	// see: http://stackoverflow.com/questions/3578970/passing-too-many-arguments-to-printf
	snprintf(_what_buffer, sizeof(_what_buffer),
			 format, _keyspace_buffer, _table_buffer);

	return _what_buffer;
}
