#include <boost/atomic.hpp>
#include "cql/cql_uuid.hpp"

cql::cql_uuid_t
cql::cql_uuid_t::create() {
    static boost::atomic_ulong id; 
    return cql::cql_uuid_t(id++);
}    

namespace cql {
bool
operator <(const cql::cql_uuid_t& left, const cql::cql_uuid_t& right) {
    return (left._uuid < right._uuid);
}
}
