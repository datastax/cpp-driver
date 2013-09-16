/* 
 * File:   cql_invalid_type_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 2:12 PM
 */

#ifndef CQL_INVALID_TYPE_EXCEPTION_HPP
#define	CQL_INVALID_TYPE_EXCEPTION_HPP

#include <string>

#include "cql/exceptions/cql_generic_exception.hpp"

namespace cql {
    class cql_invalid_type_exception : public cql_generic_exception {
    public:
        cql_invalid_type_exception(
                const char* param_name,
                const char* expected_type,
                const char* received_type)
            : cql_generic_exception(create_message(param_name, expected_type, received_type).c_str()) { }
        
    private:
        std::string
        create_message(
            const char* param_name,
            const char* expected_type,
            const char* received_type) const;
    };
}

#endif	/* CQL_INVALID_TYPE_EXCEPTION_HPP */

