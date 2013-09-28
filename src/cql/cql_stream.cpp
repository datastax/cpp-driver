#include <cql/cql_stream.hpp>
#include <cql/cql.hpp>

const cql::cql_stream_t&
cql::cql_stream_t::invalid_stream() {
    static cql_stream_t invalid = 
                    cql_stream_t::from_stream_id(INVALID_STREAM_ID);
    return invalid;
}

cql::cql_stream_t
cql::cql_stream_t::from_stream_id(const cql_stream_id_t& stream_id) {
    return cql_stream_t(stream_id);
}
    
