
/*
 * File:   cql_unauthorized_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_UNAUTHORIZED_EXCEPTION_H_
#define	CQL_UNAUTHORIZED_EXCEPTION_H_

#include <string>
#include "cql_query_validation_exception.hpp"

namespace cql {
//  Indicates that a query cannot be performed due to the authorization restrictions of the logged user.
class CQL_EXPORT cql_unauthorized_exception: public cql_query_validation_exception {
public:
    cql_unauthorized_exception(const char* message)
        : cql_query_validation_exception(message) { }

	cql_unauthorized_exception(const std::string& message)
		: cql_query_validation_exception(message) { }
};
}

#endif	/* CQL_UNAUTHORIZED_EXCEPTION_H_ */
