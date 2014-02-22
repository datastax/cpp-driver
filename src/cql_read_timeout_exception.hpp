
/*
 * File:   cql_read_timeout_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_READ_TIMEOUT_EXCEPTION_H_
#define	CQL_READ_TIMEOUT_EXCEPTION_H_

#include <string>
#include "cql_query_timeout_exception.hpp"

namespace cql {
// A Cassandra timeout during a read query.
class CQL_EXPORT cql_read_timeout_exception: public cql_query_timeout_exception {
public:
    cql_read_timeout_exception(
        cql_consistency_enum consistency_level,
        cql_int_t received, 
		cql_int_t required,
        bool data_present
        )
        : cql_query_timeout_exception(
                create_message(consistency_level, received, required, data_present), 
                consistency_level, 
				received, 
				required),
          _data_present(data_present) { }
        
        inline bool
        data_retrieved() const {
            return _data_present;
        }
private:
    std::string
    create_message(
        cql_consistency_enum consistency_level,
        cql_int_t	received, 
		cql_int_t	required,
        bool		data_present);
    
    std::string
    get_message_details(
        cql_int_t	received, 
		cql_int_t	required,
        bool		data_present);
    
    bool _data_present;
};
}

#endif	/* CQL_READ_TIMEOUT_EXCEPTION_H_ */
