/*
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

#ifndef CQL_UUID_HPP_
#define	CQL_UUID_HPP_

#include "cql/cql.hpp"

#include <string>
#include <vector>

namespace cql {
    class cql_uuid_t;
}

namespace cql {

class CQL_EXPORT cql_uuid_t
{
public:
    static cql_uuid_t
    create();
    
    static cql_uuid_t
    from_timestamp(cql_bigint_t ts);
    
    static size_t
    size() {
        return _size;
    }

    cql_uuid_t();
    explicit cql_uuid_t(const std::string& uuid_string);
    explicit cql_uuid_t(cql_byte_t* bytes);
    explicit cql_uuid_t(const std::vector<cql_byte_t>& bytes);

    bool
    empty() const;
    
    cql_bigint_t
    get_timestamp() const;

    std::string
    to_string() const;
    
    std::vector<cql_byte_t>
    get_data() const;
        
    /** The `<' operator sorts the UUIDs according to their timestamps. */
    friend bool
    operator <(const cql_uuid_t& left, const cql_uuid_t& right);
    
    bool
    operator==(const cql_uuid_t& other) const;

    static const size_t _size = 16; // Size in bytes.

private:
    cql_byte_t          _uuid[_size];
};

}

#endif	/* CQL_UUID_HPP_ */
