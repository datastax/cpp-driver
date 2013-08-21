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
The library known to work on Ubuntu >= 10.04.4 LTS (Lucid Lynx), and OSX 10.8.3 with homebrew. The build-chain is CMake, so it should be fairly straight forward to get cassandra to build on other systems, but no attempt has been made to do so.

The library has two dependencies [Boost::Asio](http://www.boost.org/doc/libs/1_53_0/doc/html/boost_asio.html) and [OpenSSL](http://www.openssl.org/). It's required that Boost::Asio be installed prior to build. If OpenSSL isn't present (OSX 10.8) cassandra will automaticly download, build, and staticly link the library.

```
git clone https://github.com/mstump/cassandra
cd cassandra
cmake . && make && make cql_demo && make cql_test && make test && make install
```

Running ```make help``` will give you a list of available make targets

## Examples
In addition to the sample code below there is a fully functional [demo](https://github.com/mstump/cassandra/blob/master/demo/main.cpp) which exploits most of the functionality of the library.


### Instantiate a client and connect to Cassandra
```c++
#include <boost/asio.hpp>
#include <cassandra/cql.hpp>
#include <cassandra/cql_error.hpp>
#include <cassandra/cql_client.hpp>
#include <cassandra/cql_client_factory.hpp>
#include <cassandra/cql_result.hpp>

void
query_callback(cql::cql_client_t& client,
               int8_t stream,
               const cql::cql_result_t* result)
{
    // close the connection
    client.close();
}

void
message_errback(cql::cql_client_t& client,
                int8_t stream,
                const cql::cql_error_t& err)
{
    std::cerr << "ERROR " << err.message << std::endl;
}

void
connect_callback(cql::cql_client_t& client)
{
    // Called after a successfull connection, or
    // if using SSL it's called after a successfull handshake.

    // perform a query
    client.query("use system;",
                 cql::CQL_CONSISTENCY_ALL,
                 &query_callback,
                 &message_errback);
}

int
main(int argc,
     char* argv[])
{
    // setup Boost ASIO
    boost::asio::io_service io_service;

    // instantiate a client
    std::auto_ptr<cql::cql_client_t> client(cql::cql_client_factory_t::create_cql_client_t(io_service));

    // Connect to localhost
    client->connect("localhost", 9042, &connect_callback, NULL);

    // Start the event loop
    io_service.run();
    return 0;
}

```


### Connect to the Cassandra cluster using a connection pool
```c++
#include <boost/asio.hpp>
#include <cassandra/cql.hpp>
#include <cassandra/cql_error.hpp>
#include <cassandra/cql_client_pool.hpp>
#include <cassandra/cql_client_pool_factory.hpp>
#include <cassandra/cql_result.hpp>

void
message_errback(cql::cql_client_t& client,
                int8_t stream,
                const cql::cql_error_t& err)
{
    std::cerr << "ERROR " << err.message << std::endl;
}

void
use_query_callback(cql::cql_client_t& client,
                   int8_t stream,
                   const cql::cql_result_t* result)
{
    // close the connection
    client.close();
}

void
connect_callback(cql::cql_client_pool_t* pool)
{
    // Called after a successfull connection, or
    // if using SSL it's called after a successfull handshake.
    // Lets perform an ad-hoc query.
    pool->query("USE system;",
                cql::CQL_CONSISTENCY_ALL,
                &use_query_callback,
                &message_errback);
}

/**
  A factory functor which instantiates clients.
  For SSL connections this is how you set OpenSSL cert validation and algo options
*/
struct client_functor_t
{

public:

    client_functor_t(boost::asio::io_service&              service,
                     cql::cql_client_t::cql_log_callback_t log_callback) :
        _io_service(service),
        _log_callback(log_callback)
    {}

    cql::cql_client_t*
    operator()()
    {
        return cql::cql_client_factory_t::create_cql_client_t(_io_service, _log_callback);
    }

private:
    boost::asio::io_service&              _io_service;
    cql::cql_client_t::cql_log_callback_t _log_callback;
};

int
main(int argc,
     char* argv[])
{
    // setup Boost ASIO
    boost::asio::io_service io_service;

    // Create a client factory for the pool
    cql::cql_client_pool_t::cql_client_callback_t client_factory = client_functor_t(io_service, &log_callback);

    // Instantiate the pool
    std::auto_ptr<cql::cql_client_pool_t> pool(cql::cql_client_pool_factory_t::create_client_pool_t(client_factory, &connect_callback, NULL));

    // Connect to localhost
    pool->add_client("localhost", 9042);

    // Start the event loop
    io_service.run();
    return 0;
}

```
