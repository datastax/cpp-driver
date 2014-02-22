/*
  Copyright (c) 2013 Matthew Stump

  This file is part of cassandra.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef CQL_HEADER_IMPL_H_
#define CQL_HEADER_IMPL_H_

#include "cql.hpp"
#include "cql_message.hpp"
#include "cql_stream.hpp"

namespace cql {

struct cql_error_t;

class cql_header_impl_t {

public:

    cql_header_impl_t();

    cql_header_impl_t(cql::cql_byte_t         version,
                      cql::cql_byte_t           flags,
                      const cql::cql_stream_t&  stream,
                      cql::cql_byte_t           opcode,
                      cql_int_t                 length);

    std::string
    str() const;

    bool
    consume(cql::cql_error_t* error);

    bool
    prepare(cql::cql_error_t* error);

    cql_message_buffer_t
    buffer();

    cql_int_t
    size() const;

    cql::cql_byte_t
    version() const;

    cql::cql_byte_t
    flags() const;

    const cql::cql_stream_t&
    stream() const;

    cql::cql_byte_t
    opcode() const;

    cql_int_t
    length() const;

    void
    set_version(cql::cql_byte_t v);

    void
    set_flags(cql::cql_byte_t v);

    void
    set_stream(const cql::cql_stream_t& v);

    void
    set_opcode(cql::cql_byte_t v);

    void
    set_length(cql_int_t v);

private:
    cql_message_buffer_t _buffer;
    cql::cql_byte_t      _version;
    cql::cql_byte_t      _flags;
    cql::cql_stream_t    _stream;
    cql::cql_byte_t      _opcode;
    cql::cql_int_t       _length;
};

} // namespace cql

#endif // CQL_HEADER_IMPL_H_
