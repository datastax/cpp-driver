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

#ifndef CQL_MESSAGE_EXECUTE_IMPL_H_
#define CQL_MESSAGE_EXECUTE_IMPL_H_

#include <vector>
#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_list.hpp>

#include "cql/cql.hpp"
#include "cql/cql_stream.hpp"
#include "cql/internal/cql_message.hpp"

namespace cql {

class cql_message_execute_impl_t :
    boost::noncopyable,
    public cql_message_t {

public:
    typedef std::vector<cql::cql_byte_t> param_t;

    cql_message_execute_impl_t();

    cql_message_execute_impl_t(size_t size);

    cql_message_execute_impl_t(const std::vector<cql::cql_byte_t>& id,
                               cql::cql_consistency_enum consistency);

    const std::vector<cql::cql_byte_t>&
    query_id() const;

    void
    query_id(const std::vector<cql::cql_byte_t>& id);

    cql::cql_consistency_enum
    consistency() const;

    void
    consistency(const cql::cql_consistency_enum consistency);

    void
    push_back(const param_t& val);

    void
    push_back(const std::string& val);

    void
    push_back(const cql::cql_short_t val);

    void
    push_back(const cql_int_t val);

    void
    push_back(const cql::cql_bigint_t val);

    void
    push_back(const float val);

    void
    push_back(const double val);

    void
    push_back(const bool val);

    void
    pop_back();

    cql::cql_opcode_enum
    opcode() const;

    cql_int_t
    size() const;

    std::string
    str() const;

    bool
    consume(cql::cql_error_t* err);

    bool
    prepare(cql::cql_error_t* err);

    cql_message_buffer_t
    buffer();
        
    cql_stream_t
    stream();

    void
    set_stream(const cql_stream_t& stream);

private:
    typedef std::list<param_t> params_container_t;

    cql::cql_message_buffer_t    _buffer;
    std::vector<cql::cql_byte_t> _query_id;
    cql::cql_consistency_enum    _consistency;
    params_container_t           _params;
    cql_stream_t                 _stream;
};

} // namespace cql

#endif // CQL_MESSAGE_EXECUTE_IMPL_H_
