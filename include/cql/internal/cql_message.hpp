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

#ifndef CQL_MESSAGE_H_
#define CQL_MESSAGE_H_

#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include "cql/cql.hpp"

namespace cql {
    
struct cql_error_t;

typedef 
    boost::shared_ptr<std::vector<cql::cql_byte_t> > 
    cql_message_buffer_t;

class cql_message_t {
public:
    cql_message_t(bool is_compressed=false)
        : _is_compressed(is_compressed) {}
    
    // Returns message type.
    virtual cql::cql_opcode_enum
    opcode() const = 0;
    
    /** Returns the flag (i.e. compression/tracing flag) if applicable.
     If not applicable, returns cql_header_flag_enum::CQL_FLAG_NOFLAG */
    virtual cql_byte_t
    flag() const {
        cql_byte_t ret_flag = static_cast<cql_byte_t>(CQL_FLAG_NOFLAG);
        if (_is_compressed) {
            ret_flag |= static_cast<cql_byte_t>(CQL_FLAG_COMPRESSION);
        }
        return ret_flag;
    }
    
    void
    enable_compression() {
        _is_compressed = true;
    }
    
    bool
    is_compressed() const {
        return _is_compressed;
    }
    
    virtual cql_int_t
    size() const = 0;

    virtual std::string
    str() const = 0;
    
    // Parsers binary message.
    virtual bool
    consume(cql::cql_error_t* err) = 0;

    // Creates binary message.
    virtual bool
    prepare(cql::cql_error_t* err) = 0;

    // Buffer that contains message to parse, or
    // that will contain created binary message.
    virtual cql_message_buffer_t
    buffer() = 0;

    virtual
    ~cql_message_t() { };
    
private:
    bool _is_compressed;
};

} // namespace cql

#endif // CQL_MESSAGE_H_
