
/*
 * File:   cql_invalid_query_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_INVALID_QUERY_EXCEPTION_H_
#define	CQL_INVALID_QUERY_EXCEPTION_H_

#include <string>
#include "cql_query_validation_exception.hpp"

namespace cql {
// Indicates a syntactically correct but invalid query.
class CQL_EXPORT cql_invalid_query_exception: public cql_query_validation_exception {
public:
    cql_invalid_query_exception(const char* message)
        : cql_query_validation_exception(message) { }

	cql_invalid_query_exception(const std::string& message)
		: cql_query_validation_exception(message) { }
};
}

#endif	/* CQL_INVALID_QUERY_EXCEPTION_H_ */
