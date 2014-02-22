/* 
 * File:   cql_initialization_exception.hpp
 * Author: mc
 *
 * Created on September 20, 2013, 12:19 PM
 */

#include "cql_exception.hpp"

#ifndef CQL_INITIALIZATION_EXCEPTION_HPP_
#define	CQL_INITIALIZATION_EXCEPTION_HPP_

namespace cql {
    class CQL_EXPORT cql_initialization_exception
        : public cql_exception 
    {
    public:
        cql_initialization_exception(const char* message)
            : cql_exception(message) { }
            
        cql_initialization_exception(const std::string& message)
            : cql_exception(message) { }
    };
}

#endif	/* CQL_INITIALIZATION_EXCEPTION_HPP_ */

