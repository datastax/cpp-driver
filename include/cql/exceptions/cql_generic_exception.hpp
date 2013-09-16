/* 
 * File:   cql_generic_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 1:37 PM
 */

#ifndef CQL_GENERIC_EXCEPTION_HPP_
#define	CQL_GENERIC_EXCEPTION_HPP_

#include "cql/exceptions/cql_exception.hpp"

namespace cql {
    // Exception that carry user supplied message.
    class cql_generic_exception : public cql_exception {
    public:
        cql_generic_exception(const char* message);
    
    protected:
        // @override
        virtual void prepare_what_buffer() const;
    };
}

#endif	/* CQL_GENERIC_EXCEPTION_HPP_ */

