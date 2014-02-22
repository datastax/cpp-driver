/*
 * File:   cql_stream.hpp
 * Author: mc
 *
 * Created on September 28, 2013, 1:21 PM
 */

#ifndef CQL_STREAM_HPP
#define	CQL_STREAM_HPP

#include "cql/cql.hpp"

namespace cql {

// class represents virtual stream inside
// single connection.
class cql_stream_t {
    static const int INVALID_STREAM_ID = -1;

public:
    cql_stream_t()
        : _stream_id(INVALID_STREAM_ID) { }

    bool
    is_invalid() const { return (_stream_id == INVALID_STREAM_ID); }

    cql_stream_id_t
    stream_id() const { return _stream_id; }

    static cql_stream_t
    from_stream_id(const cql_stream_id_t& stream_id);

private:
    cql_stream_t(cql_stream_id_t stream_id)
        : _stream_id(stream_id) { }

    static const cql_stream_t&
    invalid_stream();

    cql_stream_id_t _stream_id;

    template<typename _TType>
    friend class cql_callback_storage_t;

    friend class cql_header_impl_t;
};

}


#endif	/* CQL_STREAM_HPP */
