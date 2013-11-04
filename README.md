cassandra
======

A high performance implementation of the [Cassandra binary protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v1.spec) in C++

### Current Functionality and Design
- Completely asynchronous
- An emphasis on avoiding copying of memory when possible
- Designed to be easily wrapped from C or other languages
- Exception safe
- SSL support
- Ad-hoc queries
- Prepared statements
- Authentication
- Connection pool with auto-reconnect
- Cassandra 1.2 native collections
- Event registration and notification
- Packaging scripts for Debian/Ubuntu

### TODO
- Compression
- More docs
- Packaging for Redhat/Centos and OSX
- Integration tests
- Query tracing

## Building
The library known to work on Ubuntu >= 10.04.4 LTS (Lucid Lynx), and OSX 10.8.3 with Clang/libc++. The build-chain is CMake, so it should be fairly straightforward to get cassandra to build on other systems, but no attempt has been made to do so. Please refer to particular build instructions in separate files in this folder.

The library has two dependencies [Boost::Asio](http://www.boost.org/doc/libs/1_53_0/doc/html/boost_asio.html) and [OpenSSL](http://www.openssl.org/). It's required that Boost::Asio be installed prior to build. If OpenSSL isn't present (OSX 10.8) cassandra will automaticly download, build, and staticly link the library.

```
git clone https://github.com/mstump/cassandra
cd cassandra
cmake . && make && make cql_demo && make cql_test && make test && make install
```

Running ```make help``` will give you a list of available make targets

## Examples
In addition to the sample code below there is a fully functional [demo](https://github.com/mstump/cassandra/blob/master/demo/main.cpp) which exploits most of the functionality of the library.

### Minimal Working Example - simple query against system.schema_keyspaces.
```c++
// Includes
#include <boost/asio.hpp>
#include <cql/cql.hpp>
#include <cql/cql_connection.hpp>
#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>
#include <cql/cql_result.hpp>
#include <cds/gc/hp.h>

int
main(int argc, char**)
{
    using namespace cql;
    using boost::shared_ptr;
    
    // Boilerplate: initialize cql with 512 hazard pointers.
    cql_initialize(512);
    cql_thread_infrastructure_t cql_ti;
    
    // Suppose you have the Cassandra cluster at 127.0.0.1,
    // listening at default port (9042).
    shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
    builder->add_contact_point(boost::asio::ip::address::from_string("127.0.0.1"));
    
    // Now build a model of cluster and connect it to DB.
    shared_ptr<cql::cql_cluster_t> cluster(builder->build());
    shared_ptr<cql::cql_session_t> session(cluster->connect());
    
    // Write a query, switch keyspaces.
    shared_ptr<cql::cql_query_t> my_first_query(new cql::cql_query_t("SELECT * FROM system.schema_keyspaces;"));
        
    // Send the query.
    boost::shared_future<cql::cql_future_result_t> future = session->query(my_first_query);
        
    // Wait for the query to execute; retrieve the result.
    future.wait();
    shared_ptr<cql_result_t> result = future.get().result;
    
    // Boilerplate: close the connection session and perform the cleanup.
    session->close();
    cluster->shutdown();
    cds::gc::HP::force_dispose();
    return 0;
}
```

## Credits
This C++ driver is based on the work of Matt Stump on libcql (https://github.com/mstump/libcql). Matt has recently joined DataStax and the effort to build and maintain a modern C++ driver for Cassandra will be continued with this project.

## License
Copyright 2013, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

