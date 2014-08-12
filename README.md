DataStax C/C++ Driver for Apache Cassandra (Beta)
===============================================

A C/C++ client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's Native Protocol (version 2).

- JIRA: https://datastax-oss.atlassian.net/browse/CPP
- MAILING LIST: https://groups.google.com/a/lists.datastax.com/forum/#!forum/cpp-driver-user
- IRC: #datastax-drivers on `irc.freenode.net <http://freenode.net>`

### Current Functionality and Design
- Completely asynchronous
- Exception safe
- Ad-hoc queries
- Prepared statements
- Batch statements
- Connection pool with auto-reconnect
- Cassandra collections
- Compatibility with native protcol version 1

### TODO
- Compression
- SSL support
- Authentication
- More docs
- Packaging for Redhat/Centos and OSX
- Integration tests
- Query tracing
- Event registration and notification
- Binary releases for Windows and Linux

## Building
The driver is known to work on OS X 10.9, Windows 7, and Ubuntu 14.04. The driver itself currently has two dependencies: [libuv 0.10](https://github.com/joyent/libuv) and [OpenSSL](http://www.openssl.org/). To build the driver you will need [CMake](http://www.cmake.org). To test the driver you will also need to install [boost 1.41+](http://www.boost.org),  [libssh2](http://www.libssh2.org) and [ccm](https://github.com/pcmanus/ccm).

It has been built using GCC 4.1.2+, Clang 3.4+, and MSVC 2010/2013.

### OS X
The driver has been built and tested using the Clang compiler provided by XCode 5.1. The dependencies were obtained using [Homebrew](http://brew.sh).

To obtain dependencies:
```
brew install libuv cmake
```

Note: We currently use the OpenSSL library included with XCode.

To obtain test dependencies (This is not required):
```
brew install boost libssh2
```

To build:
```
git clone https://github.com/datastax/cpp-driver.git
cd cpp-driver
cmake .
make
```

### Windows
The driver has been built and tested using Microsoft Visual Studio 2010 and 2013 (using the "Express" versions) on Windows 7 SP1. The dependencies need to be manually built or obtained.

To obtain dependencies:
* Download and install CMake for Windows. Make sure to select the option "Add CMake to the system PATH for all users" or "Add CMake to the system PATH for current user".
* Download and build the latest release of libuv 0.10 from https://github.com/joyent/libuv/releases.
  1. Follow the instructions [here](https://github.com/joyent/libuv#windows).
  2. Open up the generated Visual Studio solution "uv.sln".
  3. If you want a 64-bit build you will need to create a "x64" solution platform in the "Configuration Manager".
  4. Open "Properties" on the "libuv" project. Set "Multi-threaded DLL (/MD)" for the "Configuration Properties -> C/C++ -> Code Generation -> Runtime Library" option.
  5. Build the "libuv" project
  6. Copy the files in "libuv/include" to "cpp-driver/lib/libuv/include" and "libuv/Release/lib" to "cpp-driver/lib/libuv/lib".
* Download and install either the 32-bit or 64-bit version of OpenSSL from http://slproweb.com/products/Win32OpenSSL.html. You may also need to install the "Visual C++ 2008 Redistributables".

To build 32-bit (using "VS2013 x86 Native Tools Command Prompt"):
```
cd <path to driver>/cpp-driver
cmake -G "Visual Studio 12"
msbuild cassandra.vcxproj /p:Configuration=Release /t:Clean,Build
```

To build 64-bit (using "VS2013 x64 Cross Tools Command Prompt"):
```
cd <path to driver>/cpp-driver
cmake -G "Visual Studio 12 Win64"
msbuild cassandra.vcxproj /p:Configuration=Release /t:Clean,Build
```

Note: Use "cmake -G "Visual Studio 10" for Visual Studio 2010

### Linux
The driver was built and tested using both GCC and Clang on Ubuntu 14.04.

To obtain dependencies (GCC):
```
sudo apt-get install g++ make cmake libuv-dev libssl-dev
```

To obtain dependencies (Clang):
```
sudo apt-get install clang make cmake libuv-dev libssl-dev
```

To obtain test dependencies (This is not required):
```
sudo apt-get install libboost-chrono-dev libboost-date-time-dev libboost-log-dev libboost-program-options-dev libboost-system-dev libboost-thread-dev libboost-test-dev libssh2-1-dev
```

To build:
```
git clone https://github.com/datastax/cpp-driver.git
cd cpp-driver
cmake .
make
```

## Examples
There are several examples provided here: [examples](https://github.com/datastax/cpp-driver/tree/1.0/examples).

### A Simple Example
```c
#include <stdio.h>
#include <string.h>
#include <cassandra.h>

int main() {
  CassError rc = CASS_OK;
  CassCluster* cluster = cass_cluster_new();
  CassFuture* session_future = NULL;
  CassString contact_points = cass_string_init("127.0.0.1,127.0.0.2,127.0.0.3");

  cass_cluster_set_contact_points(cluster, contact_points);

  session_future = cass_cluster_connect(cluster);
  cass_future_wait(session_future);
  rc = cass_future_error_code(session_future);

  if(rc == CASS_OK) {
    CassSession* session = cass_future_get_session(session_future);
    CassFuture* result_future = NULL;
    CassFuture* close_future = NULL;
    CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces;");
    CassStatement* statement = cass_statement_new(query, 0);

    result_future = cass_session_execute(session, statement);
    cass_future_wait(result_future);

    rc = cass_future_error_code(result_future);
    if(rc == CASS_OK) {
      const CassResult* result = cass_future_get_result(result_future);
      CassIterator* rows = cass_iterator_from_result(result);

      while(cass_iterator_next(rows)) {
        const CassRow* row = cass_iterator_get_row(rows);
        CassString keyspace_name;
        cass_bool_t durable_writes;
        CassString strategy_class;
        CassString strategy_options;

        cass_value_get_string(cass_row_get_column(row, 0), &keyspace_name);
        cass_value_get_bool(cass_row_get_column(row, 1), &durable_writes);
        cass_value_get_string(cass_row_get_column(row, 2), &strategy_class);
        cass_value_get_string(cass_row_get_column(row, 3), &strategy_options);

        printf("keyspace_name: '%.*s', durable_writes: %s, strategy_class: '%.*s', strategy_options: %.*s\n",
               (int)keyspace_name.length, keyspace_name.data,
               durable_writes ? "true" : "false",
               (int)strategy_class.length, strategy_class.data,
               (int)strategy_options.length, strategy_options.data);
      }

      cass_result_free(result);
      cass_iterator_free(rows);
    } else {
      CassString message = cass_future_error_message(result_future);
      fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
    }

    cass_future_free(result_future);
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    CassString message = cass_future_error_message(session_future);
    fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
  }

  cass_future_free(session_future);
  cass_cluster_free(cluster);

  return 0;
}
```

## License
Copyright (c) 2014 DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
