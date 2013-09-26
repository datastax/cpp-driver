/* 
 * File:   cql_uuid_t.hpp
 * Author: mc
 *
 * Created on September 26, 2013, 12:35 PM
 */

#ifndef CQL_UUID_HPP_
#define	CQL_UUID_HPP_

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
};

}

#endif	/* CQL_UUID_HPP_ */

