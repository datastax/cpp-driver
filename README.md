DataStax C/C++ Driver for Apache Cassandra (Beta)
===============================================

[![Build Status](https://travis-ci.org/datastax/cpp-driver.svg?branch=1.0)](https://travis-ci.org/datastax/cpp-driver)

A C/C++ client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's Binary Protocol (version 1 and 2).

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
- Compatibility with binary protcol version 1 and 2
- Authentication (via credentials using SASL PLAIN)

### Upgrading from a beta to a RC release

There were a couple breaking API changes between beta5 and rc1 that are documented in detail [here](http://www.datastax.com/dev/blog/datastax-c-driver-rc1-released).

### TODO
- Compression
- Query tracing
- Event registration and notification
- Callback intefaces for load balancing, authenticaiton, reconnection and retry
- Packaging for Debian-based Linux, RHEL-based Linux and OS X
- Binary releases for Windows and Linux
- Getting started guide
- Improved documentation

## Building
The driver is known to work on OS X 10.9, Windows 7, RHEL 5/6, and Ubuntu 12.04/14.04. The driver itself currently has two dependencies: [libuv 0.10.x or libuv 1.x](https://github.com/libuv/libuv) and [OpenSSL](http://www.openssl.org/). To build the driver you will need [CMake](http://www.cmake.org). To test the driver you will also need to install [boost 1.55+](http://www.boost.org),  [libssh2](http://www.libssh2.org) and [ccm](https://github.com/pcmanus/ccm).

Note: The driver doesn't work with libuv 0.11

It has been built using GCC 4.1.2+, Clang 3.4+, and MSVC 2010/2012/2013.

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
The driver has been built and tested using Microsoft Visual Studio 2010, 2012 and 2013 (using the "Express" and Professional versions) and Windows SDK v7.1, 8.0, and 8.1 on Windows 7 SP1. The library dependencies will automatically download and build; however the following build dependencies will need to be installed.

#### Windows Build Dependencies
To obtain build dependencies:
* Download and install [CMake](http://www.cmake.org/download).
 * Make sure to select the option "Add CMake to the system PATH for all users" or "Add CMake to the system PATH for current user".
* Download and install [Git](http://git-scm.com/download/win)
 * Make sure to select the option "Use Git from Windows Command Prompt" or manually add the git executable to the system PATH.
 * NOTE: This build dependency is required if building with OpenSSL support
* Download and install [ActiveState Perl](https://www.perl.org/get.html#win32)
 * Make sure to select the option "Add Perl to PATH environment variable".
* Download and install [Python v2.7.x](https://www.python.org/downloads)
 * Make sure to select/install the feature "Add python.exe to Path"

#### Performing the Windows Build
A batch script has been created to detect installed Visual Studio version(s) (and/or Windows SDK installtion) in order to simplify the build operations on Windows.  If you have more than one version of Visual Studio (and/or Windows SDK) installed you will be prompted to select which version to use when compiling the driver.

First you will need to open a "Command Prompt" (or Windows SDK Command Prompt) to execute the batch script.

```
Usage: VC_BUILD.BAT [OPTION...]

        --DEBUG                         Enable debug build
        --RELEASE                       Enable release build (default)
        --DISABLE-CLEAN                 Disable clean build
        --DISABLE-OPENSSL               Disable OpenSSL support
	--ENABLE-PACKAGES [version]     Enable package generation (**)
        --SHARED                        Build shared library (default)
        --STATIC                        Build static library
        --X86                           Target 32-bit build (*)
        --X64                           Target 64-bit build (*)

        --HELP                          Display this message

*  Default target architecture is determined based on system architecture
** Packages are only generated using detected installations of Visual Studio
```

To build 32-bit shared library:
```
VC_BUILD.BAT --X86
```

To build 64-bit shared library:
```
VC_BUILD.BAT --X64
```

To build static library:
```
VC_BUILD.BAT --STATIC
```

To build library without OpenSSL support:
```
VC_BUILD.BAT --DISABLE-OPENSSL
```

To build 32-bit static library without OpenSSL support:
```
VC_BUILD.BAT --DISABLE-OPENSSL --STATIC --X86
```

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
sudo apt-get install libboost-chrono-dev libboost-date-time-dev libboost-log-dev libboost-system-dev libboost-thread-dev libboost-test-dev libssh2-1-dev
```

To build:
```
git clone https://github.com/datastax/cpp-driver.git
cd cpp-driver
cmake .
make
```

## Building Tests
Tests are not built by default. To build the tests add "-DCASS_BUILD_TESTS=ON" when running cmake.

## Examples
There are several examples provided here: [examples](https://github.com/datastax/cpp-driver/tree/1.0/examples).

### A Simple Example
```c
#include <stdio.h>
#include <cassandra.h>

int main() {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  cass_cluster_set_contact_points(cluster, "127.0.0.1,127.0.0.2,127.0.0.3");

  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;

    /* Build statement and execute query */
    CassString query = cass_string_init("SELECT keyspace_name "
                                        "FROM system.schema_keyspaces;");
    CassStatement* statement = cass_statement_new(query, 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if(cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and iterator over the rows */
      const CassResult* result = cass_future_get_result(result_future);
      CassIterator* rows = cass_iterator_from_result(result);

      while(cass_iterator_next(rows)) {
        const CassRow* row = cass_iterator_get_row(rows);
        const CassValue* value = cass_row_get_column_by_name(row, "keyspace_name");

        CassString keyspace_name;
        cass_value_get_string(value, &keyspace_name);
        printf("keyspace_name: '%.*s'\n", (int)keyspace_name.length,
                                               keyspace_name.data);
      }

      cass_result_free(result);
      cass_iterator_free(rows);
    } else {
      /* Handle error */
      CassString message = cass_future_error_message(result_future);
      fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message.length,
                                                            message.data);
    }

    cass_statement_free(statement);
    cass_future_free(result_future);

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    /* Handle error */
    CassString message = cass_future_error_message(connect_future);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message.length,
                                                        message.data);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

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
