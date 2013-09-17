
/*
 * File:   cql_query_execution_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_QUERY_EXECUTION_EXCEPTION_H_
#define	CQL_QUERY_EXECUTION_EXCEPTION_H_

#include <string>
#include "cql/exceptions/cql_query_validation_exception.hpp"

namespace cql {
// Exception related to the execution of a query. This correspond to the
// exception that Cassandra throw when a (valid) query cannot be executed
// (TimeoutException, UnavailableException, ...).
class DLL_PUBLIC cql_query_execution_exception: public cql_query_validation_exception {
public:
    cql_query_execution_exception(const char* message)
        : cql_query_validation_exception(message) { }

	cql_query_execution_exception(const std::string& message)
		: cql_query_validation_exception(message) { }
};
}

#endif	/* CQL_QUERY_EXECUTION_EXCEPTION_H_ */
