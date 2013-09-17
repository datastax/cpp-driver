
/*
 * File:   cql_write_timeout_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_WRITE_TIMEOUT_EXCEPTION_H_
#define	CQL_WRITE_TIMEOUT_EXCEPTION_H_

#include <string>
#include "cql/exceptions/cql_query_timeout_exception.hpp"

namespace cql {
// A Cassandra timeout during a write query.
class DLL_PUBLIC cql_write_timeout_exception: public cql_query_timeout_exception {
public:
    cql_write_timeout_exception( 
            cql_consistency_enum consistency_level,
            cql_int_t received, 
            cql_int_t required,
            const char* write_type)
    
        : cql_query_timeout_exception(
            create_message(consistency_level, received, required),
            consistency_level, 
			received, 
			required),
          _write_type(write_type)
        { }
        
        
    inline const char*
    write_type() const {
        return _write_type;
    }
    
private:
    std::string 
    create_message(
        cql_consistency_enum consistency_level,
        cql_int_t received,
        cql_int_t required);
    
    const char* _write_type;
};
}

#endif	/* CQL_WRITE_TIMEOUT_EXCEPTION_H_ */
