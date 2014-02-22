
/*
 * File:   cql_truncate_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_TRUNCATE_EXCEPTION_H_
#define	CQL_TRUNCATE_EXCEPTION_H_

#include <string>
#include "cql_query_execution_exception.hpp"

namespace cql {
// Error during a truncation operation.
class CQL_EXPORT cql_truncate_exception: public cql_query_execution_exception {
public:
    cql_truncate_exception(const char* message)
        : cql_query_execution_exception(message) { }

	cql_truncate_exception(const std::string& message)
		: cql_query_execution_exception(message) { }
};
}

#endif	/* CQL_TRUNCATE_EXCEPTION_H_ */
