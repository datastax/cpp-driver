/* 
 * File:   cql_keyspace_already_exists.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 1:27 PM
 */

#ifndef CQL_KEYSPACE_ALREADY_EXISTS_EXCEPTION_HPP_
#define	CQL_KEYSPACE_ALREADY_EXISTS_EXCEPTION_HPP_

#include <string>
#include "cql/exceptions/cql_query_validation_exception.hpp"

namespace cql {
    // Exception thrown when a query attemps to create a keyspace that
    // already exists.
    class cql_keyspace_already_exists_exception : public cql_query_validation_exception {
    public:
        cql_keyspace_already_exists_exception(const char* keyspace);
    
    private:
        std::string
        create_message(const char* keyspace);
    };
}

#endif	/* CQL_KEYSPACE_ALREADY_EXISTS_EXCEPTION_HPP_ */

