
/*
 * File:   cql_trace_retrieval_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_TRACE_RETRIEVAL_EXCEPTION_H_
#define	CQL_TRACE_RETRIEVAL_EXCEPTION_H_

#include "cql/exceptions/cql_generic_exception.hpp"

namespace cql {
//  Exception thrown if a query trace cannot be retrieved.
class cql_trace_retrieval_exception: public cql_generic_exception {
public:
    cql_trace_retrieval_exception(const char* message)
        : cql_generic_exception(message) { }
};
}

#endif	/* CQL_TRACE_RETRIEVAL_EXCEPTION_H_ */
