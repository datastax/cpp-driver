/*
 * File:   cql_connection_allocation_error.hpp
 */

#ifndef CQL_CONNECTION_ALLOCATION_ERROR_H_
#define	CQL_CONNECTION_ALLOCATION_ERROR_H_

#include "cql/exceptions/cql_exception.hpp"

namespace cql {
class CQL_EXPORT cql_connection_allocation_error: public cql_exception {
public:
    cql_connection_allocation_error()
        : cql_exception("Network error when allocating connection") { }
        
    cql_connection_allocation_error(const char* message)
        : cql_exception(message) { }
};
}

#endif	/* CQL_CONNECTION_ALLOCATION_ERROR_H_ */
