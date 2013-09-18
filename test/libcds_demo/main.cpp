#include <cstdlib>
#include <iostream>

#include <functional>

#include <cds/init.h>
#include <cds/gc/hp.h>

#include <cds/opt/hash.h>
#include <cds/container/michael_list_hp.h>
#include <cds/container/split_list_map.h>
namespace cc = cds::container;

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "lockfree_hash_map.hpp"
using namespace cql;

lockfree_hash_map_t<std::string, std::string>* map;

struct thread_main_t {
    static void
    do_job(bool inc, int i) {
        cds::gc::HP::thread_gc hp_thread_gc;
        
        std::string names[] = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" };
        map->try_add(names[i], "");
        
        for(int i = 0; i < 1000; i++) {
            std::string foo;
            map->try_erase(names[i % 10], &foo);
            map->try_get(names[(i + 1) % 10], &foo);
            
            if(i % 200 == 0)
                boost::this_thread::sleep(boost::posix_time::millisec(10));
            
            std::cout << "rund " << i << std::endl;
        }
    }
};



void
do_job() {
    map = new lockfree_hash_map_t<std::string, std::string>(1000);
  
    std::vector<boost::shared_ptr<boost::thread>> tthreads;
    
    for(int i = 0; i < 10; i++) {
        tthreads.push_back(boost::shared_ptr<boost::thread>(new boost::thread(boost::bind(&thread_main_t::do_job, i < 5, i))));
    }
    
    
    for(int i = 0; i < 10; i++) {
        tthreads[i]->join();
    }
    
    delete map;
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
