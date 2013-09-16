
/*
 * File:   cql_truncate_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_TRUNCATE_EXCEPTION_H_
#define	CQL_TRUNCATE_EXCEPTION_H_

#include "cql/exceptions/cql_exception.hpp"

namespace cql {
class cql_truncate_exception: public cql_exception {
public:
    cql_truncate_exception(const char* message)
        : cql_exception(message) { }
};
}

#endif	/* CQL_TRUNCATE_EXCEPTION_H_ */
