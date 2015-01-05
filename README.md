# DataStax C/C++ Driver for Apache Cassandra (Beta)

[![Build Status](https://travis-ci.org/datastax/cpp-driver.svg?branch=1.0)](https://travis-ci.org/datastax/cpp-driver)

A C/C++ client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's Binary Protocol (version 1 and 2).

- JIRA: https://datastax-oss.atlassian.net/browse/CPP
- MAILING LIST: https://groups.google.com/a/lists.datastax.com/forum/#!forum/cpp-driver-user
- IRC: #datastax-drivers on `irc.freenode.net <http://freenode.net>`

## Current Functionality and Design
- Completely asynchronous
- Exception safe
- Ad-hoc queries
- Prepared statements
- Batch statements
- Connection pool with auto-reconnect
- Cassandra collections
- Compatibility with binary protcol version 1 and 2
- Authentication (via credentials using SASL PLAIN)

## Upgrading from a beta to a RC release

There were a couple breaking API changes between beta5 and rc1 that are documented in detail [here](http://www.datastax.com/dev/blog/datastax-c-driver-rc1-released).

## TODO
- Compression
- Query tracing
- Event registration and notification
- Callback intefaces for load balancing, authenticaiton, reconnection and retry
- Packaging for Debian-based Linux, RHEL-based Linux and OS X
- Binary releases for Windows and Linux
- Getting started guide
- Improved documentation

## Examples
There are several examples provided here: [examples](https://github.com/datastax/cpp-driver/tree/1.0/examples).

## A Simple Example
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
