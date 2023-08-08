# DataStax C/C++ Driver for Apache Cassandra® and DataStax Products

A modern, feature-rich and highly tunable C/C++ client library for
[Apache Cassandra®] 2.1+ using exclusively Cassandra's binary protocol and
Cassandra Query Language v3. This driver can also be used with other DataStax
products:

* [DataStax Enterprise]
* [DataStax Astra]

## Getting the Driver

The source code is made available via [GitHub].  Additionally binary versions of
the driver (for multiple operating systems and multiple architectures) can be
obtained from our [Artifactory server].  Binaries are available for the following
operating systems:

* CentOS 7
* Rocky Linux 8.8
* Rocky Linux 9.2
* Ubuntu 20.04
* Ubuntu 22.04
* Windows

The driver depends on the following libraries:

* libuv (1.x)
* OpenSSL
* zlib

The version of OpenSSL and zlib provided with each Linux distribution above can be used
to build the driver.  A version of libuv > 1.x is provided for CentOS 7 and Rocky
Linux; this can be found under the `dependencies` directory for each platform.
Packages for all three dependencies are provided for Windows distributions.

## Upgrading

Starting with DataStax C/C++ driver for Apache Cassandra® v2.15.0, DataStax
Enterprise (DSE) support is now available; using the DSE driver exclusively is no
longer required for DSE customers.

### For DSE driver users

Linking changes will be required when migrating to this driver. Replace `-ldse` with `-lcassandra`.

### For Cassandra driver users

No changes will be required when upgrading to this driver. There will be new
driver dependencies when using any of the binary versions obtained from our
[Artifactory server] as [Kerberos] is utilized in the [DSE features] of this
driver. See the [installation] section for more information on obtaining the
dependencies for a specific platform.

## Features

* [Asynchronous API]
* [Simple], [Prepared], and [Batch] statements
* [Asynchronous I/O], [parallel execution], and request pipelining
* Connection pooling
* Automatic node discovery
* Automatic reconnection
* Configurable [load balancing]
* Works with any cluster size
* [Authentication]
* [SSL]
* [Latency-aware routing]
* [Performance metrics]
* [Tuples] and [UDTs]
* [Nested collections]
* [Retry policies]
* [Client-side timestamps]
* [Data types]
* [Idle connection heartbeats]
* Support for materialized view and secondary index metadata
* Support for clustering key order, `frozen<>` and Cassandra version metadata
* [Blacklist], [whitelist DC], and [blacklist DC] load balancing policies
* [Custom] authenticators
* [Reverse DNS] with SSL peer identity verification support
* Randomized contact points
* [Speculative execution]
* DSE Features
  * [DSE authentication]
    * Plaintext/DSE
    * LDAP
    * GSSAPI (Kerberos)
  * [DSE geospatial types]
  * DSE [proxy authentication][DSE Proxy Authentication] and [proxy execution][DSE Proxy Execution]
  * [DSE DateRange]
* Support for [DataStax Astra] Cloud Data Platform

## Compatibility

This driver works exclusively with the Cassandra Query Language v3 (CQL3) and
Cassandra's native protocol. The current version works with the following
server versions:

* Apache Cassandra® versions 3.0.x, 3.11.x and 4.0.x
* DSE versions 6.8.x and 5.1.x

Both 32-bit (x86) and 64-bit (x64) architectures are supported

We build and test the driver on the following platforms:

* CentOS 7 w/ gcc 4.8.5
* Rocky Linux 8.8 w/ gcc 8.5.0
* Rocky Linux 9.2 w/ gcc 11.3.1
* Ubuntu 20.04 w/ gcc 9.4.0
* Ubuntu 22.04 w/ gcc 11.3.0
* Microsoft Visual Studio 2013, 2015, 2017 and 2019

A complete compatibility matrix for both Apache Cassandra®
and DataStax Enterprise can be found [here][cpp-driver-compatability-matrix].

__Disclaimer__: DataStax products do not support big-endian systems.

## Documentation

* [Home]
* [API]
* [Getting Started]
* [Building]

## Getting Help

* JIRA: [jira]
* Mailing List: [mailing list]

## Examples

The driver includes several examples in the [examples] directory.

## A Simple Example
```c
#include <cassandra.h>
/* Use "#include <dse.h>" when connecting to DataStax Enterpise */
#include <stdio.h>

int main(int argc, char* argv[]) {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;

    /* Build statement and execute query */
    const char* query = "SELECT release_version FROM system.local";
    CassStatement* statement = cass_statement_new(query, 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);

      if (row) {
        const CassValue* value = cass_row_get_column_by_name(row, "release_version");

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        printf("release_version: '%.*s'\n", (int)release_version_length, release_version);
      }

      cass_result_free(result);
    } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(result_future, &message, &message_length);
      fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
    }

    cass_statement_free(statement);
    cass_future_free(result_future);

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    /* Handle error */
    const char* message;
    size_t message_length;
    cass_future_error_message(connect_future, &message, &message_length);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length, message);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
```

## License

&copy; DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the “License”); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

[Apache Cassandra®]: http://cassandra.apache.org
[DataStax Enterprise]: http://www.datastax.com/products/datastax-enterprise
[Examples]: examples/
[Artifactory server]: https://datastax.jfrog.io/artifactory/cpp-php-drivers/cpp-driver/builds
[GitHub]: https://github.com/datastax/cpp-driver
[cpp-driver-compatability-matrix]: https://docs.datastax.com/en/driver-matrix/docs/cpp-drivers.html
[Home]: http://docs.datastax.com/en/developer/cpp-driver/latest
[API]: http://docs.datastax.com/en/developer/cpp-driver/latest/api
[Getting Started]: http://docs.datastax.com/en/developer/cpp-driver/latest/topics
[Building]: http://docs.datastax.com/en/developer/cpp-driver/latest/topics/building
[jira]: https://datastax-oss.atlassian.net/browse/CPP
[mailing list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/cpp-driver-user
[DataStax Astra]: https://astra.datastax.com
[Kerberos]: https://web.mit.edu/kerberos

[Asynchronous API]: http://datastax.github.io/cpp-driver/topics/#futures
[Simple]: http://datastax.github.io/cpp-driver/topics/#executing-queries
[Prepared]: http://datastax.github.io/cpp-driver/topics/basics/prepared_statements/
[Batch]: http://datastax.github.io/cpp-driver/topics/basics/batches/
[Asynchronous I/O]: http://datastax.github.io/cpp-driver/topics/#asynchronous-i-o
[parallel execution]: http://datastax.github.io/cpp-driver/topics/#thread-safety
[load balancing]: http://datastax.github.io/cpp-driver/topics/configuration/#load-balancing
[Authentication]: http://datastax.github.io/cpp-driver/topics/security/#authentication
[SSL]: http://datastax.github.io/cpp-driver/topics/security/ssl/
[Latency-aware routing]: http://datastax.github.io/cpp-driver/topics/configuration/#latency-aware-routing
[Performance metrics]: http://datastax.github.io/cpp-driver/topics/metrics/
[Tuples]: http://datastax.github.io/cpp-driver/topics/basics/tuples/
[UDTs]: http://datastax.github.io/cpp-driver/topics/basics/user_defined_types/
[Nested collections]: http://datastax.github.io/cpp-driver/topics/basics/binding_parameters/#nested-collections
[Data types]: http://datastax.github.io/cpp-driver/topics/basics/data_types/
[Retry policies]: http://datastax.github.io/cpp-driver/topics/configuration/retry_policies/
[Client-side timestamps]: http://datastax.github.io/cpp-driver/topics/basics/client_side_timestamps/
[Idle connection heartbeats]: http://datastax.github.io/cpp-driver/topics/configuration/#connection-heartbeats
[Blacklist]: http://datastax.github.io/cpp-driver/topics/configuration/#blacklist
[whitelist DC]: http://datastax.github.io/cpp-driver/topics/configuration/#datacenter
[blacklist DC]: http://datastax.github.io/cpp-driver/topics/configuration/#datacenter
[Custom]: http://datastax.github.io/cpp-driver/topics/security/#custom
[Reverse DNS]: http://datastax.github.io/cpp-driver/topics/security/ssl/#enabling-cassandra-identity-verification
[Speculative execution]: http://datastax.github.io/cpp-driver/topics/configuration/#speculative-execution
[DSE authentication]: http://docs.datastax.com/en/developer/cpp-driver/latest/dse_features/authentication
[DSE geospatial types]: http://docs.datastax.com/en/developer/cpp-driver/latest/dse_features/geotypes
[DSE Proxy Authentication]: http://docs.datastax.com/en/developer/cpp-driver/latest/dse_features/authentication/#proxy-authentication
[DSE Proxy Execution]: http://docs.datastax.com/en/developer/cpp-driver/latest/dse_features/authentication/#proxy-execution
[DSE DateRange]: https://github.com/datastax/cpp-driver/blob/master/examples/dse/date_range/date_range.c
[DSE features]: http://docs.datastax.com/en/developer/cpp-driver/latest/dse_features
