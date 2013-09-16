/* 
 * File:   cql_table_already_exists.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 1:09 PM
 */

#include <string>
#include "cql/exceptions/cql_query_validation_exception.hpp"

#ifndef CQL_TABLE_ALREADY_EXISTS_EXCEPTION_HPP_
#define	CQL_TABLE_ALREADY_EXISTS_EXCEPTION_HPP_

namespace cql {
    // Exception thrown when a query attemps to create a table that
    // already exists.
    class cql_table_already_exists_exception : public cql_query_validation_exception {
    public:
        cql_table_already_exists_exception(const char* table_name);
        cql_table_already_exists_exception(const char* keyspace, 
                                const char* table_name);
    private:
        std::string
        create_message(const char* table_name, const char* keyspace);
    };
}


#endif	/* CQL_TABLE_ALREADY_EXISTS_EXCEPTION_HPP_ */

