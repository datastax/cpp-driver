/* 
 * File:   lockfree_hash_map.hpp
 * Author: mc
 *
 * Created on September 18, 2013, 1:20 PM
 */

#ifndef LOCKFREE_HASH_MAP_HPP_
#define	LOCKFREE_HASH_MAP_HPP_

//
// REQUIRES NEW C++11 STANDARD
// 
#include <functional>
#include <utility>
#include <cassert>

#include <cds/opt/hash.h>
#include <cds/container/michael_list_hp.h>
#include <cds/container/split_list_map.h>

namespace cql {
    template<typename TKey, typename TValue>
    struct lockfree_hash_map_traits_t
        : public cds::container::split_list::type_traits 
    {

        typedef cds::container::michael_list_tag  ordered_list    ;   // what type of ordered list we want to use
        typedef std::hash<TKey>                   hash            ;   // hash functor for the key stored in split-list map

        // Type traits for our MichaelList class
        struct ordered_list_traits: public cds::container::michael_list::type_traits {
            typedef std::less<TKey> less;   // comparer that specifies order of list nodes
        };
    };
    
    template<typename TKey, typename TValue>
    class lockfree_hash_map_t { 
    public:
        typedef
            cds::container::SplitListMap<cds::gc::HP, TKey, TValue, lockfree_hash_map_traits_t<TKey, TValue> > 
            base_hash_map;
    
        private:
        struct copy_value_functor_t {
            typedef
                typename base_hash_map::value_type
                value_type;

            copy_value_functor_t(value_type* storage)
                    : _storage(storage) 
            {
                assert(storage != NULL);
            };
            
            void operator ()(value_type const& item_in_map) {
                // we must sync access to item_in_map if it is modified
                _item.first = item_in_map.first;
                _item.second = item_in_map.second;
            }

            inline value_type const& 
            value() const {
                return _item;
            }
            
        private:
            value_type _storage;
        };
        
    public:
        
        lockfree_hash_map_t() 
            : _map() { }
    
        lockfree_hash_map_t(size_t expected_items_count, size_t load_factor = 1)
            : _map(expected_items_count, load_factor) { }        
        
        inline size_t
        size() const {
            return _map.size();
        }
    
        inline bool 
        try_add(TKey const& key, TValue const& value) {
            return _map.insert(key, value);
        }
    public:
        base_hash_map   _map;
    };
}

#endif	/* LOCKFREE_HASH_MAP_HPP_ */

