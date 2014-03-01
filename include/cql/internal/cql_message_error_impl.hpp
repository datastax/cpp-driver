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

#ifndef CQL_MESSAGE_ERROR_IMPL_H_
#define CQL_MESSAGE_ERROR_IMPL_H_

#include "cql/cql.hpp"
#include "cql/internal/cql_message.hpp"

namespace cql {

class cql_message_error_impl_t :
    public cql_message_t {

public:

    cql_message_error_impl_t();

    cql_message_error_impl_t(size_t size);

    cql_message_error_impl_t(cql_int_t code,
                             const std::string& message);

    const std::string&
    message() const;

    void
    message(const std::string& m);

    cql_int_t
    code() const;

    void
    code(cql_int_t c);

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

private:
        
    bool
    _read_server_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_protocol_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_bad_credentials_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_unavailable_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_overloaded_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_is_bootstrapping_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_truncate_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_write_timeout_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_read_timeout_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_syntax_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_unauthorized_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_invalid_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_config_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_already_exists_error(cql_error_t* error, std::istream& input) const;
    bool
    _read_unprepared_error(cql_error_t* error, std::istream& input) const;
        
    cql::cql_message_buffer_t _buffer;
    cql_int_t                 _code;
    std::string               _message;
};

} // namespace cql

#endif // CQL_MESSAGE_ERROR_IMPL_H_
