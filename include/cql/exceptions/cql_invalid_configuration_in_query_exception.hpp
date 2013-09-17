
/*
 * File:   cql_invalid_configuration_in_query_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_INVALID_CONFIGURATION_IN_QUERY_EXCEPTION_H_
#define	CQL_INVALID_CONFIGURATION_IN_QUERY_EXCEPTION_H_

#include <string>
#include "cql/exceptions/cql_invalid_query_exception.hpp"

namespace cql {
//	A specific invalid query exception that indicates that the query is invalid
//  because of some configuration problem. This is generally throw by query
//  that manipulate the schema (CREATE and ALTER) when the required configuration
//  options are invalid.
class cql_invalid_configuration_in_query_exception: public cql_invalid_query_exception {
public:
    cql_invalid_configuration_in_query_exception(const char* message)
        : cql_invalid_query_exception(message) { }

	cql_invalid_configuration_in_query_exception(const std::string& message)
		: cql_invalid_query_exception(message) { }
};
}

#endif	/* CQL_INVALID_CONFIGURATION_IN_QUERY_EXCEPTION_H_ */
