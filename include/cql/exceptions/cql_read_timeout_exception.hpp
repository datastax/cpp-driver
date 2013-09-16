
/*
 * File:   cql_read_timeout_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_READ_TIMEOUT_EXCEPTION_H_
#define	CQL_READ_TIMEOUT_EXCEPTION_H_

#include "cql/exceptions/cql_exception.hpp"

namespace cql {
class cql_read_timeout_exception: public cql_exception {
public:
    cql_read_timeout_exception(const char* message)
        : cql_exception(message) { }
};
}

#endif	/* CQL_READ_TIMEOUT_EXCEPTION_H_ */
