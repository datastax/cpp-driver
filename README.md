# DataStax C/C++ Driver for Apache Cassandra

[![Build Status](https://travis-ci.org/datastax/cpp-driver.svg?branch=master)](https://travis-ci.org/datastax/cpp-driver)

A modern, [feature-rich](#features), and highly tunable C/C++ client library for
Apache Cassandra (1.2+) and DataStax Enterprise (3.1+) using exclusively
Cassandra's native protocol and Cassandra Query Language v3.

- Code: https://github.com/datastax/cpp-driver
- Docs: http://datastax.github.io/cpp-driver
- JIRA: https://datastax-oss.atlassian.net/browse/CPP
- Mailing List: https://groups.google.com/a/lists.datastax.com/forum/#!forum/cpp-driver-user
- IRC: [#datastax-drivers on `irc.freenode.net <http://freenode.net>`](http://webchat.freenode.net/?channels=datastax-drivers)

## What's New in 2.0

- [Latency-aware routing]
- [Performance metrics]

More information about all the changes found in the 2.0 release can be found in
this [blog post] and in the [changelog](CHANGELOG.md).

## Upgrading from 1.0 to 2.0

There were a couple breaking API changes between 1.0 and 2.0 that are
documented [here](http://www.datastax.com/dev/blog/datastax-c-driver-2-0-released).

## Features
- [Asynchronous API]
- [Simple], [Prepared], and [Batch] statements
- [Asynchronous I/O], [parallel execution], and request pipelining
- Connection pooling
- Automatic node discovery
- Automatic reconnection
- Configurable [load balancing]
- Works with any cluster size
- Compatibility with binary protocol version 1 and 2
- [Authentication]
- [SSL]

## Examples
There are several examples provided here: [examples](https://github.com/datastax/cpp-driver/tree/1.0/examples).

## A Simple Example
```c
#include <cassandra.h>
#include <stdio.h>

int main() {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, "127.0.0.1,127.0.0.2,127.0.0.3");

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;

    /* Build statement and execute query */
    CassStatement* statement
      = cass_statement_new("SELECT keyspace_name "
                           "FROM system.schema_keyspaces", 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if(cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and iterate over the rows */
      const CassResult* result = cass_future_get_result(result_future);
      CassIterator* rows = cass_iterator_from_result(result);

      while(cass_iterator_next(rows)) {
        const CassRow* row = cass_iterator_get_row(rows);
        const CassValue* value = cass_row_get_column_by_name(row, "keyspace_name");

        const char* keyspace;
        size_t keyspace_length;
        cass_value_get_string(value, &keyspace, &keyspace_length);
        printf("keyspace_name: '%.*s'\n",
               (int)keyspace_length, keyspace);
      }

      cass_result_free(result);
      cass_iterator_free(rows);
    } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(result_future, &message, &message_length);
      fprintf(stderr, "Unable to run query: '%.*s'\n",
              (int)message_length, message);
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
    fprintf(stderr, "Unable to connect: '%.*s'\n",
            (int)message_length, message);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
```

## License
Copyright (c) 2014-2015 DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[Latency-aware routing]: http://datastax.github.io/cpp-driver/topics/configuration/#latency-aware-routing
[Performance metrics]: http://datastax.github.io/cpp-driver/topics/metrics/
[blog post]: http://www.datastax.com/dev/blog/datastax-c-driver-2-0-released

[Asynchronous API]: http://datastax.github.io/cpp-driver/topics/#futures
[Simple]: http://datastax.github.io/cpp-driver/topics/#executing-queries
[Prepared]: http://datastax.github.io/cpp-driver/topics/basics/prepared_statements/
[Batch]: http://datastax.github.io/cpp-driver/topics/basics/batches/
[Asynchronous I/O]: http://datastax.github.io/cpp-driver/topics/#asynchronous-i-o
[parallel execution]: http://datastax.github.io/cpp-driver/topics/#thread-safety
[Authentication]: http://datastax.github.io/cpp-driver/topics/security/#authentication
[load balancing]: http://datastax.github.io/cpp-driver/topics/configuration/#load-balancing
[SSL]: http://datastax.github.io/cpp-driver/topics/security/ssl/
