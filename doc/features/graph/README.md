# Graph

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
void execute_graph_query(CassSession* session) {
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
}
```

## Graph options

A graph name should not be set if for executing system queries. This can also be
accomplished by passing `NULL` for the options argument to
`dse_graph_statement_new()`.

```c
void execute_graph_query_without_options(CassSession* session) {
  /* Create a system query (note: passing NULL for options) */
  DseGraphStatement* statement =
    dse_graph_statement_new("system.graph('test').ifNotExists().create()", NULL);

  /* Create and bind "name" value */
  DseGraphObject* values = dse_graph_object_new();
  dse_graph_object_add_string(values, "name", "somevalue");
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
}
```

The graph options object should be created once and reused for all graph queries
that use the same options.

```c

int main() {
  /* Create a graph options so that we can set a specific graph name: "test" */
  DseGraphOptions* test_options = dse_graph_options_new();

  /* Set the graph name */
  dse_graph_options_set_graph_name(test_options, "test");

  /* Run application */

  /* Free the options at the end of execution */
  dse_graph_options_free(test_options);
}
```

## Data types

Supported data types can be found in the [DSE Graph documentation]. In the RC
version of the driver there are some known limitations for some data types:

<table class="table table-striped table-hover table-condensed">
  <thead>
    <tr>
      <th>DSE Graph Data Type</th>
      <th>Limitations</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>blob</code></td>
      <td>Returned as a base64 string</td>
    </tr>
    <tr>
      <td><code>decimal</code></td>
      <td>Truncated to a double precision floating point number or may not be handled</td>
    </tr>
    <tr>
      <td><code>duration</code></td>
      <td>Returned as a string using this format: https://en.wikipedia.org/wiki/ISO_8601#Durations</td>
    </tr>
    <tr>
      <td><code>inet</code></td>
      <td>Returned as a string</td>
    </tr>
    <tr>
      <td><code>varint</code></td>
      <td>Truncated to a double precision floating point number, 64-bit integer, or may not be handled</td>
    </tr>
    <tr>
      <td><code>linestring</code></td>
      <td>Returned as a string using Well-known text (https://en.wikipedia.org/wiki/Well-known_text)</td>
    </tr>
    <tr>
      <td><code>point</code></td>
      <td>Returned as a string using Well-known text (https://en.wikipedia.org/wiki/Well-known_text)</td>
    </tr>
    <tr>
      <td><code>polygon</code></td>
      <td>Returned as a string using Well-known text (https://en.wikipedia.org/wiki/Well-known_text)</td>
    </tr>
    <tr>
      <td><code>timestamp</code></td>
      <td>Returned as a string using this format: https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations</td>
    </tr>
    <tr>
      <td><code>uuid</code></td>
      <td>Returned as a string</td>
    </tr>
  </tbody>
</table>

### Geometric types

Graph queries can use well-known text
(https://en.wikipedia.org/wiki/Well-known_text) for handling geometric types or
they can use the driver's built-in geometric types and functions. To bind a
geometric type parameter use `dse_graph_object_add_point()`,
`dse_graph_object_add_line_string()` and `dse_graph_object_add_polygon()`. To
retrieve a geometric type from a graph result use `dse_graph_result_as_point()`,
`dse_graph_result_as_line_string()` and `dse_graph_result_as_polygon()`. See the
[geotypes section](/features/geotypes/) for more information.

## Parameter values

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

## Handling results

Graph queries return a `DseGraphResultSet` which is able to iterate over the
set of results. The lifetime of `DseGraphResult` objects are bound to the
containing result set. `DseGraphResult` objects are invalidated with each call
to `dse_graph_resultset_next()`.

```c
void handle_graph_resultset(CassFuture* future) {
  DseGraphResultSet* resultset = cass_future_get_dse_graph_resultset(future);

  size_t i, count = dse_graph_resultset_count(resultset);

  for (i = 0; i < count; ++i) {
    /* Gets the next result (and invalidateds the previous result */
    const DseGraphResult* result = dse_graph_resultset_next(resultset);

    /* ... */
  }

  /* Frees the result set and all contained `DseGraphResult` objects */
  dse_graph_resultset_free(resultset);
}
```

A `DseGraphResult` is a variant type that can represent the following types:
number (double or an integer),  boolean, string, an array or an object (a
collection of name/value pairs).

```c
void handle_graph_result(const DseGraphResult* result) {
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
}
```

[DSE Graph documentation]: http://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/graph/reference/refDSEGraphDataTypes.html
