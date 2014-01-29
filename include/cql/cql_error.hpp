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

#ifndef CQL_ERROR_H_
#define CQL_ERROR_H_

#include <string>
#include "cql/cql.hpp"

namespace cql {

struct cql_error_t {
    bool        cassandra;
    bool        transport;
    bool        library;
    int         code;
    std::string message;

    cql_error_t() :
        cassandra(false),
        transport(false),
        library(false),
        code(0)
    {}
    
    cql_error_t(bool cassandra_,
                bool transport_,
                bool library_,
                int code_,
                const std::string& message_)
                :   cassandra(cassandra_),
                    transport(transport_),
                    library(library_),
                    code(code_),
                    message(message_) { }
    
    inline bool
    is_err() const {
        return cassandra || transport || library;
    }
    
    static cql_error_t
    cassandra_error(int code, const std::string& message) {
        return cql_error_t(
            /*cassandra:*/  true,
            /*transport:*/  false,
            /*library:*/    false,
            /*code:*/       code,
            /*message:*/    message);
    }

    static cql_error_t
    transport_error(int code, const std::string& message) {
        return cql_error_t(
                           /*cassandra:*/  false,
                           /*transport:*/  true,
                           /*library:*/    false,
                           /*code:*/       code,
                           /*message:*/    message);
    }
};
} // namespace cql

#endif // CQL_ERROR_H_
