#include <cstdlib>
#include <iostream>

#include <functional>

#include <cds/init.h>
#include <cds/gc/hp.h>

#include <cds/opt/hash.h>
#include <cds/container/michael_list_hp.h>
#include <cds/container/split_list_map.h>
namespace cc = cds::container;

#include "lockfree_hash_map.hpp"
using namespace cql;

template<typename TKey, typename TValue>
struct get_item {
    typedef
        std::pair<TKey, TValue> 
        map_value_type;
    
    void operator ()(const map_value_type& item_in_map) {
        // we must sync access to item_in_map if it is modified
        _item.first = item_in_map.first;
        _item.second = item_in_map.second;
    }
    
    inline const map_value_type& 
    item() const {
        return _item;
    }
private:
    map_value_type _item;
};

void
do_job() {
    std::cout << "it's working!" << std::endl;
    
    lockfree_hash_map_t<std::string, std::string> foo;
    
    std::string strings[] = {
        "foo", "bar", "nyu", "super", "upper",  
        "psi", "signma", "omega", "delta", "ksi"
    };
    
    std::cout << "first attempt: " << foo._map.insert("foo", "bar") << std::endl;
    std::cout << "second attempt: " << foo._map.insert("foo", "bar") << std::endl;
        
    for(int i = 0; i < 1000; i++) {
        foo._map.insert(strings[i % 10], strings[i % 10]);
    }
    
    foo._map.insert("foo", "9");
    
    
    get_item<std::string, std::string> gi;
    if(foo._map.find("bar", gi)) {
        std::cout << "value associated with bar is: " << gi.item().second << std::endl;
    }
    
    std::pair<std::string, std::string> foop = make_pair(std::string("foo"), std::string("bar"));
     std::pair<std::string, std::string> copy = foop;
    
    std::cout << foop.first << ", " << foop.second;
    
    auto it = foo._map.begin();
    for(; it != foo._map.end(); ++it) {
         std::cout << "map[" << it->first << "] = " << it->second << std::endl;
    }
    
    std::cout << "it's working!" << std::endl;
}

int 
main(int argc, char* argv[]) {
    cds::Initialize();
    {
        cds::gc::HP hp_infrastructure;
        cds::gc::HP::thread_gc hp_thread_gc;
    
        do_job();
    }
    cds::Terminate();
}
