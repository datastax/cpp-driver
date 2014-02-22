/* 
 * File:   cql_to_many_connections_per_host.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 2:06 PM
 */

#ifndef CQL_TO_MANY_CONNECTIONS_PER_HOST_EXCEPTION_HPP_
#define	CQL_TO_MANY_CONNECTIONS_PER_HOST_EXCEPTION_HPP_

#include "cql_exception.hpp"

namespace cql {
    class CQL_EXPORT cql_too_many_connections_per_host_exception: public cql_exception {
    public:
        cql_too_many_connections_per_host_exception()
            : cql_exception("maximum number of connections per host is reached.") { }
    };
}

#endif	/* CQL_TO_MANY_CONNECTIONS_PER_HOST_EXCEPTION_HPP_ */

