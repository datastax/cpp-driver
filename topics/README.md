# Getting Started

## Connecting

### Cluster object

The first step to using the driver is to create a `CassCluster` object that describes your Cassandra cluster’s configuration. The default cluster object is good for most clusters and only a list of contact points needs to be configured. The list of contact points doesn’t need to contain every host in your cluster, only a small subset is required, because the rest of the cluster will be automatically discovered through the control connection. It’s a good idea to change the order of your contact points for each of your client hosts to prevent a single Cassandra host from becoming the control connection on every client machine in your cluster. The [plan](https://datastax-oss.atlassian.net/browse/CPP-193) is to do this automatically in a future release. The control connection also monitors changes in your cluster’s topology (automatically handling node outages, adding new nodes, and removal of old nodes) and tracks schema changes.

```c
CassCluster* cluster = cass_cluster_new();

/* Contact points can be added as a comma-delimited list */
cass_cluster_set_contact_points("127.0.0.1,127.0.0.2");

/* Or individually */
cass_cluster_set_contact_points("127.0.0.3");
cass_cluster_set_contact_points("127.0.0.4");

/* DNS can also be used */
cass_cluster_set_contact_points("node1.datastax.com,node2.datastax.com");

cass_cluster_free(cluster);
```

### Session object

The session object is used to execute queries. Internally, it also manages a pool of client connections to Cassandra and uses a load balancing policy to distribute requests across those connections. It’s recommend that your application only creates a single session object per keyspace as a session object is designed to be created once, reused and shared by multiple application threads. The throughput of a session can be scaled by increasing the number of I/O threads. An I/O thread is used to handle reading and writing query request data to and from Cassandra. The number of I/O threads defaults to one per CPU core, but it can be configured using `cass_cluster_set_num_threads_io()`.

### Connecting a session

The C/C++ driver’s API is designed so that no operation will force your application to block. Operations that would normally cause your application to block, such as connecting to a cluster or running a query, instead return a CassFuture object that can be waited on, polled or used to register a callback. The API can also be used synchronously by immediately attempting to get the result from a future. To demonstrate the use of CassFuture let’s create and connect a CassSession using the cluster object we created earlier.

```c
CassCluster* cluster = cass_cluster_new();

/* Add contact points */
cass_cluster_set_contact_points("127.0.0.1,127.0.0.2");

/* Create and connect a session */
CassSession* session = cass_session_new();
 
/* Provide the cluster object as configuration to connect the session */
CassFuture* connect_future = cass_session_connect(session, cluster);
 
/* This operation will block until the result is ready */
CassError rc = cass_future_error_code(connect_future);
 
printf("Connect result: %s\n", cass_error_desc(rc));
 
cass_future_free(connect_future);
cass_session_free(session);
cass_cluster_free(cluster);
```

## Executing Queries

The connected session can now be used to run queries. Queries are constructed using CassStatement objects. There are two types of statement objects, regular and prepared. Regular statements are most useful for ad hoc queries and applications where the query string will change often. A prepared statement caches the query on the Cassandra server and requires the extra step of preparing the query server-side first.

CassStatement objects can also be used to bind variables. The ‘?’ marker is used to denote the bind variables in a query string. In addition to adding the bind marker to your query string your application must also provide the number of bind variables to `cass_statement_new()` when constructing a new statement. If a query doesn’t require any bind variables then 0 can be used. `cass_statement_bind_*()` functions are then used to bind values to the statement’s variables. Bind variables can be bound by the marker’s position (index) or by name. Variables can only be bound by name for prepared statements (see the prepared statement example below). This limitation exists because query metadata provided by Cassandra is required to map the variable name to the variable’s marker index.

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

## TODO

Here are some features that are missing from the C/C++ driver, but are included with other drivers. The schedule for these features can be found on [JIRA](https://datastax-oss.atlassian.net/browse/CPP).

- Compression
- Query tracing
- Event registration and notification
- Callback intefaces for load balancing, authenticaiton, reconnection and retry
- Packaging for Debian-based Linux, RHEL-based Linux and OS X
- Binary releases for Windows and Linux

## FAQ
