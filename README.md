# C/C++ Driver for DataStax Enterprise

This driver is built on top of the [C/C++ driver for Apache
Cassandra][cpp-driver], with specific extensions for DataStax Enterprise (DSE):

* DSE PlainText and GSSAPI authentication
* DSE geospatial types
* DSE graph integration

## Installing

Building and installing the C/C++ driver for DSE is almost exactly the same as
the core C/C++ driver. The directions for building and installing the core
driver can be found [here](http://datastax.github.io/cpp-driver/topics/building/).

## Getting Started

Connecting the driver is is the same as the core C/C++ driver except that
`dse.h` should be included instead of `cassandra.h` which is automatically
included by `dse.h`.

```c
/* Include the DSE driver */
#include <dse.h>

int main() {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, "127.0.0.1");

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {

    /* Run queries here */

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    /* Handle error */
    const char* message;
    size_t message_length;
    cass_future_error_message(connect_future, &message, &message_length);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length,
                                                        message);
  }

  /* Cleanup driver objects */
  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
```

### Building against the DSE driver

Your application will need to link against the `dse` library which is `dse.so`
(or `dse_static.a` for static linking) on OS X or Linux and `dse.lib` on
Windows. Here's how you would link on OS X or Linux using Clang.

```
# clang example.c -I<path to dse.h> -L<path to dse.so> -ldse
```

## Examples

The driver includes several examples in the [examples](examples) directory.

## Authentication

Clients that require authentication when connecting to a DSE cluster secured
using `DseAuthenticator` should use the following functions:

* Plain-text authentication: `cass_cluster_set_dse_plaintext_authenticator()`
* GSSAPI authentication: `cass_cluster_set_dse_gssapi_authenticator()`

```c
/* A DSE cluster using plain-text authenction would use: */
cass_cluster_set_dse_plaintext_authenticator(cluster, "cassandra", "cassandra");

/* A DSE cluster using GSSAPI authentication woudl use: */
cass_cluster_set_dse_gssapi_authenticator(cluster, "dse", "cassandra@DATASTAX.COM");
```

## Geospatial types

DSE 5.0 comes with several additional types to represent geospatial data:
`PointType`, `LineStringType` and `PolygonType`. These types can be added
directly using CQL or the C/C++ DSE driver.

```
cqlsh> CREATE TABLE IF NOT EXISTS geotypes (key text PRIMARY KEY, point 'PointType', linestring 'LineStringType', polygon 'PolygonType');
cqlsh> INSERT INTO geotypes (key, point) VALUES ('point', 'POINT(42 3.14159)');
cqlsh> INSERT INTO geotypes (key, linestring) VALUES ('linestring', 'LINESTRING(0 0, 1 1)');
cqlsh> INSERT INTO geotypes (key, linestring) VALUES ('polygon', 'POLYGON((0 0, 1 0, 1 1, 0 0))');
```

These geospatial types can be also be used directly from C/C++ types:

```c
CassStatement* statement =
  cass_statement_new("INSERT INTO examples.geotypes (key, point) VALUES (?, ?)", 2);

cass_statement_bind_string(statement, 0, "point");

/* Bind a point using with the point's components */
cass_statement_bind_dse_point(statement, 1, 42, 3.141459);

/* Execute statement */
```

```c
CassStatement* statement =
  cass_statement_new("INSERT INTO examples.geotypes (key, linestring) VALUES (?, ?)", 2);

/* Construct the line string */
DseLineString* line_string = dse_line_string_new();

/* Optionally reserve space for the points */
dse_line_string_reserve(line_string, 2);

/* Add points to the line string */
dse_line_string_add_point(line_string, 0, 0);
dse_line_string_add_point(line_string, 1, 1);

/* Tell the line string we are not going to add any more points */
dse_line_string_finish(line_string);

cass_statement_bind_string(statement, 0, "linestring");

/* Bind the line string */
cass_statement_bind_dse_line_string(statement, 1, line_string);

/* Execute statement */
```

```c
CassStatement* statement =
  cass_statement_new("INSERT INTO examples.geotypes (key, polygon) VALUES (?, ?)", 2);

/* Construct the polygon */
DsePolygon* polygon = dse_polygon_new();

/* A start a new ring */
dse_polygon_start_ring(polygon);

/* Add points to the current ring */
dse_polygon_add_point(polygon, 0, 0);
dse_polygon_add_point(polygon, 1, 0);
dse_polygon_add_point(polygon, 1, 1);
dse_polygon_add_point(polygon, 0, 0);

/* Tell the polygon string we are not going to add any more rings or points */
dse_polygon_finish(polygon);

cass_statement_bind_string(statement, 0, key);

/* Bind the polygon */
cass_statement_bind_dse_polygon(statement, 1, polygon);

/* Execute statement */
```

## Graph

The driver can execute graph queries via  the `CassSession` function
`cass_session_execute_dse_graph()` and the `DseGraphStatement` type.

The following "test" graph is created using gremlin-shell (via `bin/dse
gremlin-shell`):

```
gremlin> system.graph('test').create()
==>null
gremlin> :remote config alias g test.g
==>g=test.g
gremlin> Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);
==>v[{~label=person, ...}]
gremlin> Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);
==>v[{~label=person, ...}]
gremlin> Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');
==>v[{~label=software, member_id=3, community_id=955191296}]
gremlin> Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);
==>v[{~label=person, ...}]
gremlin> Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');
==>v[{~label=software, ...}]
gremlin> Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);
==>v[{~label=person, ...}]
gremlin> marko.addEdge('knows', vadas, 'weight', 0.5f);
==>e[{out_vertex=...
gremlin> marko.addEdge('knows', josh, 'weight', 1.0f);
==>e[{out_vertex={...}, , in_vertex={...}]
gremlin> marko.addEdge('created', lop, 'weight', 0.4f);
==>e[{out_vertex={...}, , in_vertex={...}]
gremlin> josh.addEdge('created', ripple, 'weight', 1.0f);
==>e[{out_vertex={...}, , in_vertex={...}]
gremlin> josh.addEdge('created', lop, 'weight', 0.4f);
==>e[{out_vertex={...}, , in_vertex={...}]
gremlin> peter.addEdge('created', lop, 'weight', 0.2f);
==>e[{out_vertex={...}, , in_vertex={...}]
```

The "test" graph can then be queried using the driver:

```c
/* Create a graph options so that we can set a specific graph name: "test" */
DseGraphOptions* options = dse_graph_options_new();

/* Set the graph name */
dse_graph_options_set_graph_name(options, "test");

/* Create a graph query */
DseGraphStatement* statement =
  dse_graph_statement_new("g.V().has('name','marko').out('knows').values('name')", options);

/* Execute the graph query */
CassFuture* future =
  cass_session_execute_dse_graph(session, statement);

/* Check and handle the result */
if (cass_future_error_code(future) == CASS_OK) {
  DseGraphResultSet* resultset = cass_future_get_dse_graph_resultset(future);

  /* Handle result set */
} else {
  /* Handle error */
}

/* Cleanup */
cass_future_free(future);
dse_graph_statement_free(statement);
```

### Graph options

A graph name should not be set if for executing system queries. This can also be
accomplished by passing `NULL` for the options argument to
`dse_graph_statement_new()`.

```c
/* Create a system query (note: passing NULL for options) */
DseGraphStatement* statement =
  dse_graph_statement_new("system.graph('test').ifNotExists().create()", NULL);

/* Create and bind "name" value */
DseGraphObject* values = dse_graph_object_new();
dse_graph_object_add_string(values, "name", name);
dse_graph_object_finish(values);
dse_graph_statement_bind_values(statement, values);
dse_graph_object_free(values);

/* Execute the graph query */
CassFuture* future =
  cass_session_execute_dse_graph(session, statement);

/* Handle results... */

/* Cleanup */
cass_future_free(future);
dse_graph_statement_free(statement);
```

The graph options object should be created once and reused for all graph queries
that use the same options.

```c
DseGraphOptions* test_options = NULL;

int main() {
  /* Create a graph options so that we can set a specific graph name: "test" */
  test_options = dse_graph_options_new();

  /* Set the graph name */
  dse_graph_options_set_graph_name(options, "test");

  /* Run application */

  /* Free the options at the end of execution */
  dse_graph_options_free(test_options);
}
```

### Parameter values

Query parameters are bound using a `DseGraphObject`. Parameter values are added
by name using the `dse_graph_object_add_<type>()` functions.

```c
/* Create a system query (note: passing NULL for options) */
DseGraphStatement* statement =
  dse_graph_statement_new("system.graph(name).ifNotExists().create()", NULL);

/* Create a graph object to hold the query's parameters */
DseGraphObject* values = dse_graph_object_new();

/* Add the "name" parameter */
dse_graph_object_add_string(values, "name", "test");

/* Finish must be called after bind the last value */
dse_graph_object_finish(values);

/* Bind the value to the statement */
dse_graph_statement_bind_values(statement, values);

/* ... */

/* Cleanup */
dse_graph_object_free(values);
```

### Handling results

Graph queries return a `DseGraphResultSet` which is able to iterate over the
set of results. The lifetime of `DseGraphResult` objects are bound to the
containing result set. `DseGraphResult` objects are invalidated with each call
to `dse_graph_resultset_next()`.

```c
/* ... */

DseGraphResultSet* resultset = cass_future_get_dse_graph_resultset(future);

size_t i, count = dse_graph_resultset_count(resultset);

for (i = 0; i < count; ++i) {
  /* Gets the next result (and invalidateds the previous result */
  const DseGraphResult* result = dse_graph_resultset_next(resultset);

  /* ... */
}

/* Frees the result set and all contained `DseGraphResult` objects */
dse_graph_resultset_free(resultset);
```

A `DseraphResult` is a variant type that can represent the following types:
number (double or an integer),  boolean, string, an array or an object (a
collection of name/value pairs).

```c

/* ... */

/* Always check the type of a DseGraphResult */
if (dse_graph_result_is_double(result)) {
  cass_double_t value = dse_graph_result_get_double(result);

  /* ... */
}

if (dse_graph_result_is_array(result)) {
  /* The count can be used to iterate over the elements in an array */
  size_t i, count = dse_graph_result_element_count(result);
  for (i = 0; i < count; ++i) {
    const DseGraphResult* element = dse_graph_result_element(result, i);

    /* ... */
  }
}

if (dse_graph_result_is_object(result)) {
  /* The count can be used to iterate over the members in an object */
  size_t i, count = dse_graph_result_member_count(result);
  for (i = 0; i < count; ++i) {
    const char* key = dse_graph_result_member_key(result, i, NULL);
    const DseGraphResult* value = dse_graph_result_member_value(result, i);

    /* ... */
  }
}
```

[cpp-driver]: http://datastax.github.io/cpp-driver/
