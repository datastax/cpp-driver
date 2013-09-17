
/*
 * File:   cql_syntax_error.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_SYNTAX_ERROR_H_
#define	CQL_SYNTAX_ERROR_H_

#include <string>
#include "cql/exceptions/cql_query_validation_exception.hpp"

namespace cql {
// Indicates a syntax error in a query.
class cql_syntax_error: public cql_query_validation_exception {
public:
    cql_syntax_error(const char* message)
        : cql_query_validation_exception(message) { }

	cql_syntax_error(const std::string& message)
		: cql_query_validation_exception(message) { }
};
}

#endif	/* CQL_SYNTAX_ERROR_H_ */
