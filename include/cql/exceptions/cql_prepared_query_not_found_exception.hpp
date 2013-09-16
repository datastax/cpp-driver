
/*
 * File:   cql_prepared_query_not_found_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_PREPARED_QUERY_NOT_FOUND_EXCEPTION_H_
#define	CQL_PREPARED_QUERY_NOT_FOUND_EXCEPTION_H_

#include "cql/exceptions/cql_exception.hpp"

namespace cql {
class cql_prepared_query_not_found_exception: public cql_exception {
public:
    cql_prepared_query_not_found_exception(const char* message)
        : cql_exception(message) { }
};
}

#endif	/* CQL_PREPARED_QUERY_NOT_FOUND_EXCEPTION_H_ */
