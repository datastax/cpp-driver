# Getting started

## Installation

Binary packages can be downloaded from [here](TODO) or the driver can also be [built](TODO).

## Connecting

```c
#include <cassandra.h>
#include <stdio.h>

int main() {
  CassCluster* cluster = cass_cluster_new();

  /* Add contact points */
  cass_cluster_set_contact_points("127.0.0.1");

  /* Create and connect a session */
  CassSession* session = cass_session_new();

  /* Provide the cluster object as configuration to connect the session */
  CassFuture* connect_future = cass_session_connect(session, cluster);

  /* This operation will block until the result is ready */
  CassError rc = cass_future_error_code(connect_future);

  printf("Connect result: %s\n", cass_error_desc(rc));

  /* Run queries... */

  cass_future_free(connect_future);
  cass_session_free(session);
  cass_cluster_free(cluster);

  return 0;
}
```

To connect a session you need to create a [`CassCluster`](http://datastax.github.io/cpp-driver/api/struct_cass_cluster/) object and configure it. The minimal configuration needed to connect is a list of contact points. The contact points are used to initialize the driver and it will automatically discover the rest of the nodes in your cluster. It's a good idea to include more than one contact point to be robust against node failures.

## Futures

The driver is designed so that no operation will force your application to block. Operations that would normally cause your application to block, such as connecting to a cluster or running a query, instead return a [`CassFuture`](http://datastax.github.io/cpp-driver/api/struct_cass_future/) object that can be waited on, polled or used to register a callback. The API can also be used synchronously by immediately attempting to get the result from a future.

## Executing a Query

Queries are executed using [`CassStatement`](http://datastax.github.io/cpp-driver/api/struct_cass_future/) objects. Statements encapsulate the query string and the query parameters. Query parameters are not supported by ealier versions of Cassandra (1.2 and below) and values need to be inlined in the query string itself.

```c
CassString insert_query
  = cass_string_init("INSERT INTO example (key, value) VALUES ('abc', 123);");

/ * Create a statement with zero parameters */
CassStatement* statement = cass_statement_new(insert_query, 0);

CassFuture* query_future = cass_session_execute(session, statement);

/* Statement objects can be freed immediately after being executed */
cass_statement_free(statement);

/* This will block until the query has finished */
CassError rc = cass_future_error_code(query_future);

printf("Query result: %s\n", cass_error_desc(rc));

cass_future_free(query_future);
```

## Query Parameters

Cassandra 2.0+ supports the use of query parameters. This allows the same query string to be executed mulitple times with different values.  Your application can then avoid string manipulation to pass values. If you're repeating the same query mulitple time you should use prepared staements.

```c
CassString insert_query
  = cass_string_init("INSERT INTO example (key, value) VALUES (?, ?);");

/* There are two bind variables in the query string */
CassStatement* statement = cass_statement_new(insert_query, 2);

/* Bind the values using the indices of the bind variables */
cass_statement_bind_string(statement, 0, cass_string_init("abc"));
cass_statement_bind_int32(statement, 1, 123);

CassFuture* query_future = cass_session_execute(session, statement);

/* Statement objects can be freed immediately after being executed */
cass_statement_free(statement);

/* This will block until the query has finished */
CassError rc = cass_future_error_code(query_future);

printf("Query result: %s\n", cass_error_desc(rc));

cass_future_free(query_future);
```

## Handling query results

A single row can be retrieved using the convenience function [`cass_result_first_row()`](TODO) to get the first row. If multiple rows are returned a CassIterator object can be used to iterate over the returned rows.

```c
/* Execute "SELECT * FROM example (key, value) WHERE key = 'abc'" */
 
/* This will also block until the query returns */
const CassResult* result = cass_future_get_result(future);
 
/* If there was an error then the result won't be available */
if (result == NULL) {
  /* Handle error */
  cass_future_free(query_future);
  return -1;
}
 
/* The future can be freed immediately after getting the result object */
cass_future_free(query_future);
 
/* This can be used to retrieve on the first row of the result */
const CassRow* row = cass_result_first_row(result);
 
/* Now we can retrieve the column values from the row */
CassString key;
/* Get the column value of "key" by name */
cass_value_get_string(cass_row_get_column_by_name(row, "key"), &key);
 
cass_int32_t value;
/* Get the column value of "value" by name */
cass_value_get_int32(cass_row_get_column_by_name(row, "value"), &value);
 
 
/* This will free the future as well as the string pointed to by the CassString 'key' */
cass_result_free(result);
```

# Architecture

## Cluster

The CassCluster object describes your Cassandra cluster’s configuration. The default cluster object is good for most clusters and only a list of contact points needs to be configured. Once a session is connected using a cluster object its configuration is constant. Modify the cluster object configuration after connecting a session doesn't change the session's configuration.

## Session

The session object is used to execute queries. Internally, it also manages a pool of client connections to Cassandra and uses a load balancing policy to distribute requests across those connections. It’s recommend that your application only creates a single session object per keyspace as a session object is designed to be created once, reused and shared by multiple application threads. The throughput of a session can be scaled by increasing the number of I/O threads. An I/O thread is used to handle reading and writing query request data to and from Cassandra. The number of I/O threads defaults to one per CPU core, but it can be configured using [`cass_cluster_set_num_threads_io()`](). It’s generally better to create a single session with more I/O threads than multiple sessions with a smaller number of I/O threads. More DataStax driver best practices can be found in this post.

## Thread safety

[`CassSession`](TODO) is designed to be used concurrently from multiple threads. It is best practice to create a single session per keysapce. [`CassFuture`](TODO) is also thread safe. Other than these exclusions, in general, functions that might modify an object's state are NOT thread safe. Object's that are immutable (marked 'const') can be read safely by multiple threads. None of the 'free' functions can be called conncurently on the same instance of an object.

## Memory handling

Values such as [`CassString`](TODO) and [`CassBytes`](TODO) point to memory held by the result object. The lifetimes of those values are valid as long as the result object isn’t freed. These values need to be copied into application memory if they need to live longer than the result object’s lifetime. Primitive types such as [`cass_int32_t`](TODO) are copied by the driver because it can be done cheaply without incurring extra allocations.

Moving an iterator to the next value invalidates the value it previously returned.

## TODO

Here are some features that are missing from the C/C++ driver, but are included with other drivers. The schedule for these features can be found on [JIRA](https://datastax-oss.atlassian.net/browse/CPP).

- Compression
- Query tracing
- Event registration and notification
- Callback intefaces for load balancing, authenticaiton, reconnection and retry
- Packaging for Debian-based Linux, RHEL-based Linux and OS X
- Binary releases for Windows and Linux

