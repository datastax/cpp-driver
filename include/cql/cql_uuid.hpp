/* 
 * File:   cql_uuid_t.hpp
 * Author: mc
 *
 * Created on September 26, 2013, 12:35 PM
 */

#ifndef CQL_UUID_HPP_
#define	CQL_UUID_HPP_

#include <functional>

namespace cql {
    class cql_uuid_t;
}

namespace std {
    template<>
    struct hash<cql::cql_uuid_t>;
}

namespace cql {
    
class cql_uuid_t {
public:
    // TODO: This is currently implemented as simple long
    // but soon we switch to official UUID implementation
    // as described here:
    // http://www.ietf.org/rfc/rfc4122.txt
    
    static cql_uuid_t
    create();
    
    friend bool
    operator <(const cql_uuid_t& left, const cql_uuid_t& right);
    
private:
    cql_uuid_t(unsigned long uuid): _uuid(uuid) { }
    
    unsigned long    _uuid;
    
    friend class std::hash<cql_uuid_t>;
};

}

namespace std {

    template<>
    struct hash<cql::cql_uuid_t> {
        typedef
            cql::cql_uuid_t
            argument_type;
        
        typedef
            size_t
            result_type;
        
        size_t
        operator ()(const cql::cql_uuid_t& id) const {
            return 
                ((id._uuid * 3169) << 16) +
                ((id._uuid * 23) << 8) +
                id._uuid;
        }
    };
}

#endif	/* CQL_UUID_HPP_ */

