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

/*
namespace std {
     template<>
     struct hash<cql::cql_uuid_t>;
}
*/

namespace cql {

class CQL_EXPORT cql_uuid_t {
public:
    static cql_uuid_t
    create();
    
    static size_t
    size() {
        return _size;
    }

    cql_uuid_t();
    cql_uuid_t(const std::string& uuid_string);
    cql_uuid_t(cql_byte_t* bytes);

    bool
    empty() const;

    std::string
    to_string() const;
    
    std::vector<cql_byte_t>
    get_data() const;
    
    // friend struct std::hash<cql_uuid_t>;
    friend bool
    operator <(const cql_uuid_t& left, const cql_uuid_t& right);
    
private:
    static const size_t _size = 16; // Size in bytes.
    cql_byte_t          _uuid[_size];
};

}

// namespace std {

//     template<>
//     struct hash<cql::cql_uuid_t> {
// 	public:
//         typedef
//             cql::cql_uuid_t
//             argument_type;

//         typedef
//             size_t
//             result_type;

//         size_t
//         operator ()(const cql::cql_uuid_t& id) const {
//             return
//                 ((id._uuid * 3169) << 16) +
//                 ((id._uuid * 23) << 8) +
//                 id._uuid;
//         }
//     };
// }

#endif	/* CQL_UUID_HPP_ */
