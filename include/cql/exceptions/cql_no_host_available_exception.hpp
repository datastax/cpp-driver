
/*
 * File:   cql_no_host_available_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_NO_HOST_AVAILABLE_EXCEPTION_H_
#define	CQL_NO_HOST_AVAILABLE_EXCEPTION_H_

#include "cql/exceptions/cql_exception.hpp"

namespace cql {
class cql_no_host_available_exception: public cql_exception {
public:
    cql_no_host_available_exception()
        : cql_exception("All host tried for query are in error.") { }
};
}

#endif	/* CQL_NO_HOST_AVAILABLE_EXCEPTION_H_ */
