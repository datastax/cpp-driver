/* 
 * File:   cql_invalid_type_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 2:12 PM
 */

#ifndef CQL_INVALID_TYPE_EXCEPTION_HPP
#define	CQL_INVALID_TYPE_EXCEPTION_HPP

#include <string>

#include "cql/exceptions/cql_exception.hpp"

namespace cql {
    class CQL_EXPORT cql_invalid_type_exception : public cql_exception {
    public:
        cql_invalid_type_exception(
                const std::string& param_name,
                const std::string& expected_type,
                const std::string& received_type)
            : cql_exception(create_message(param_name, expected_type, received_type)) { }
        
    private:
        std::string
        create_message(
		const std::string& param_name,
		const std::string& expected_type,
		const std::string& received_type) const;
    };
}

#endif	/* CQL_INVALID_TYPE_EXCEPTION_HPP */

