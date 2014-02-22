
/*
 * File:   cql_query_timeout_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_QUERY_TIMEOUT_EXCEPTION_H_
#define	CQL_QUERY_TIMEOUT_EXCEPTION_H_

#include "cql.hpp"
#include "cql_query_execution_exception.hpp"

namespace cql {
// A Cassandra timeout during a query. Such an exception is returned when the
// query has been tried by Cassandra but cannot be achieved with the requested
// consistency level within the rpc timeout set for Cassandra.
class CQL_EXPORT cql_query_timeout_exception: public cql_query_execution_exception {
public:
    cql_query_timeout_exception(
        const char* message,
        cql_consistency_enum consistency_level,
        cql_int_t received, 
        cql_int_t required)
        
		:		cql_query_execution_exception(message),
		_consistency(consistency_level),
		_recv_acknowledgements(received),
		_req_acknowledgements(required)
        { }

	cql_query_timeout_exception(
		const std::string& message,
		cql_consistency_enum consistency_level,
		cql_int_t received, 
		cql_int_t required)

		:   cql_query_execution_exception(message),
		_consistency(consistency_level),
		_recv_acknowledgements(received),
		_req_acknowledgements(required)
	{ }
        
    // Gets the number of replica that had acknowledged/responded to the operation before it time outed. 
    cql_int_t 
    received_acknowledgements() const {
        return _recv_acknowledgements;
    }
    
    // Gets the minimum number of replica acknowledgments/responses that were required to fulfill the operation. 
    cql_int_t
    required_acknowledgements() const  {
        return _req_acknowledgements;
    }
    
    // Gets the consistency level of the operation that time outed. 
    cql_consistency_enum consistency_level() const {
        return _consistency;
    }
        
private:
    cql_consistency_enum    _consistency;
    cql_int_t               _recv_acknowledgements;
    cql_int_t               _req_acknowledgements;
};
}

#endif	/* CQL_QUERY_TIMEOUT_EXCEPTION_H_ */
