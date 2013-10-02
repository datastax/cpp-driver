/* 
 * File:   cql_2hashmap_t.hpp
 * Author: mc
 *
 * Created on October 2, 2013, 8:39 AM
 */

#ifndef CQL_2HASHMAP_T_HPP
#define	CQL_2HASHMAP_T_HPP

#include <utility>
#include "cql/lockfree/cql_lockfree_hash_map.hpp"

namespace cql {
    template<typename TKey1, typename TKey2>
    struct cql_2key_t {
        cql_2key_t(const TKey1& key1, const TKey2& key2)
            : _key1(key1), _key2(key2) { }
        
        inline const TKey1&
        key1() const { return _key1; }
        
        inline const TKey2&
        key2() const { return _key2; }
        
    private:
        TKey1       _key1;
        TKey2       _key2;
    };
}

namespace std {
    template<typename TKey1, typename TKey2>
    struct hash<cql::cql_2key_t<TKey1, TKey2> > {
        typedef 
            cql::cql_2key_t<TKey1, TKey2>
			argument_type;

		typedef 
			::std::size_t
			result_type;
        
        hash()
            : _key1_hash()
            , _key2_hash() { }
        
        inline result_type 
		operator ()(const argument_type& key) const {
            return 
                _key1_hash(key.key1()) * 178493u +
                _key2_hash(key.key2());
		}
        
    private:
        ::std::hash<TKey1>    _key1_hash;
        ::std::hash<TKey2>    _key2_hash;
    };
    
    template<typename TKey1, typename TKey2>
    struct less<cql::cql_2key_t<TKey1, TKey2> > {
		typedef
			cql::cql_2key_t<TKey1, TKey2>
			first_argument_type;

		typedef
			cql::cql_2key_t<TKey1, TKey2>
			second_argument_type;

		typedef
			bool
			result_type;
    
        less()
            : _key1_less()
            , _key2_less() { }
        
        inline result_type
		operator ()(const first_argument_type& first, const second_argument_type& second) const {
            if(_key1_less(first.key1(), second.key1()))
                return true;
            
            if(_key1_less(second.key1(), first.key1()))
                return false;
            
            return _key2_less(first.key2(), second.key2());
        }
        
    private:
        ::std::less<TKey1>  _key1_less;
        ::std::less<TKey2>  _key2_less;
    };
}

namespace cql {
    template<typename TKey1, typename TKey2, typename TValue> 
    class cql_2hashmap_t 
        : public cql_lockfree_hash_map_t<cql_2key_t<TKey1, TKey2>, TValue> 
    {
    public:
        cql_2hashmap_t() 
            : cql_lockfree_hash_map_t<cql_2key_t<TKey1, TKey2>, TValue>() { }
        
         cql_2hashmap_t(size_t expected_items_count, size_t load_factor = 1)
            : cql_lockfree_hash_map_t<cql_2key_t<TKey1, TKey2>, TValue> (expected_items_count, load_factor) { } 
        
        inline virtual
        ~cql_2hashmap_t() { }
    };
}

#endif	/* CQL_2HASHMAP_T_HPP */

