/* 
 * File:   cql_invalid_type_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 2:12 PM
 */

#ifndef CQL_INVALID_TYPE_EXCEPTION_HPP
#define	CQL_INVALID_TYPE_EXCEPTION_HPP

#include "cql/exceptions/cql_exception.hpp"

namespace cql {
    class cql_invalid_type_exception : public cql_exception {
    public:
        cql_invalid_type_exception(
            const char* param_name,
            const char* expected_type,
            const char* received_type);
        
    protected:
        // @override
        virtual void
        prepare_what_buffer() const;
        
    private:
        char _parameter_name[CQL_EXCEPTION_BUFFER_SIZE];
        char _received_type[CQL_EXCEPTION_BUFFER_SIZE];
        char _expected_type[CQL_EXCEPTION_BUFFER_SIZE];
    };
}

#endif	/* CQL_INVALID_TYPE_EXCEPTION_HPP */

