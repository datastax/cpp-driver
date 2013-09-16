
/*
 * File:   cql_unavailable_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_UNAVAILABLE_EXCEPTION_H_
#define	CQL_UNAVAILABLE_EXCEPTION_H_

#include <string>

#include "cql/cql.hpp"
#include "cql/exceptions/cql_query_execution_exception.hpp"

namespace cql {
// Exception thrown when the coordinator knows there is not enough replica alive
// to perform a query with the requested consistency level.
class cql_unavailable_exception: public cql_query_execution_exception {
public:
    cql_unavailable_exception(
            cql_consistency_enum consistency_level,
            cql_int_t required,
            cql_int_t alive)
        : 
        cql_query_execution_exception(create_message(consistency_level, required, alive).c_str()),
        _consistency_level(consistency_level),
        _required(required),
        _alive(alive)
        { }

        // Gets the consistency level of the operation triggering this unavailable exception.
        cql_consistency_enum consistency_level() const {
            return _consistency_level;
        }
        
        // Gets the number of replica acknowledgements/responses required to perform the operation (with its required consistency level). 
        cql_int_t required_replicas() const {
            return _required;
        }
        
        // Gets the number of replica that were known to be alive by the Cassandra coordinator node when it tried to execute the operation. 
        cql_int_t alive_replicas() const {
            return _alive;
        }
private:
    std::string 
    create_message(
        cql_consistency_enum consistency_level,
        cql_int_t required,
        cql_int_t alive);
    
    cql_consistency_enum    _consistency_level;
    cql_int_t               _required;
    cql_int_t               _alive;
};
}

#endif	/* CQL_UNAVAILABLE_EXCEPTION_H_ */
