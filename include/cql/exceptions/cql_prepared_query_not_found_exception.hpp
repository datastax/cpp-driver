
/*
 * File:   cql_prepared_query_not_found_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_PREPARED_QUERY_NOT_FOUND_EXCEPTION_H_
#define	CQL_PREPARED_QUERY_NOT_FOUND_EXCEPTION_H_

#include <string>

#include "cql/cql.hpp"
#include "cql/exceptions/cql_exception.hpp"

namespace cql {
class cql_prepared_query_not_found_exception: public cql_exception {
public:
    cql_prepared_query_not_found_exception(cql_byte_t query_id)
        : cql_exception("prepared query not found."),
          _query_id(query_id) { }
        
	// Returns ID of FIRST prepared queries that was not found.
    cql_byte_t query_id() const {
        return _query_id;
    }
   
private:
    cql_byte_t _query_id;
};
}

#endif	/* CQL_PREPARED_QUERY_NOT_FOUND_EXCEPTION_H_ */
