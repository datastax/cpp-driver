# Binding Parameters

The ‘?’ marker is used to denote the bind variables in a query string. This can be used for both regular and prepared parameterized queries. In addition to adding the bind marker to your query string your application must also provide the number of bind variables to `cass_statement_new()` when constructing a new statement. If a query doesn’t require any bind variables then 0 can be used. `cass_statement_bind_*()` functions are then used to bind values to the statement’s variables. Bind variables can be bound by the marker’s index or by name and must be supplied for all bound variables.

```c
/* Create a statement with a single parameter */
CassStatement* statement = cass_statement_new("SELECT * FROM table1 WHERE column1 = ?", 1);

cass_statement_bind_string(statement, 0, "abc");

/* Execute statement */

cass_statement_free(statement);
```

Variables also could be bound by name, where name could be inferred from query, or explicitly specified as `:name`. (Until Cassandra 2.1, bind by name might be done only for [prepared statements](../prepared_statements/). This limitation did exist because query metadata provided by Cassandra is required to map the variable name to the variable’s marker index.)

```c
void execute_prepared_statement(const CassPrepared* prepared) {
  /* The prepared query allocates the correct number of paramters automatically */
  CassStatement* statement = cass_prepared_bind(prepared);

  /* The parameter can now be bound by name */
  cass_statement_bind_string_by_name(statement, "column1", "abc");

  /* Execute statement */

  cass_statement_free(statement);
}
```

## Unbound parameters

When using Cassandra 2.2+ the driver will send a special `unset` value for
unbound parameters (leaving the unbound column unaffected). If using older
versions of Cassandra (2.1 and below) the driver will return an error for
unbound parameters. Calling `cass_statement_reset_parameters()` will unbind (or
resize) a statement's parameters.

## Constructing Collections

Collections are supported using [`CassCollection`](http://datastax.github.io/cpp-driver/api/struct.CassCollection/) objects; supporting `list`, `map` and `set` Cassandra types. The code below shows how to construct a `list` collection; however, a set can be constructed in a very similar way. The only difference is the type `CASS_COLLECTION_TYPE_SET` is used to create the collection instead of `CASS_COLLECTION_TYPE_LIST`.

**Important**: Values appended to the collection can be freed immediately afterward because the values are copied.

```c
const char* query = "SELECT * FROM ...";

CassStatement* statement = cass_statement_new(query, 1);

CassCollection* list = cass_collection_new(CASS_COLLECTION_TYPE_LIST, 3);

cass_collection_append_string(list, "123");
cass_collection_append_string(list, "456");
cass_collection_append_string(list, "789");

cass_statement_bind_collection(statement, 0, list);

/* The colleciton can be freed after binding */
cass_collection_free(list);
```

Maps are built similarly, but the key and value need to be interleaved as they are appended to the collection.

```c
const char* query = "SELECT * FROM ...";

CassStatement* statement = cass_statement_new(query, 1);

CassCollection* map = cass_collection_new(CASS_COLLECTION_TYPE_MAP, 2);

/* map["abc"] = 123 */
cass_collection_append_string(map, "abc");
cass_collection_append_int32(map, 123);

/* map["def"] = 456 */
cass_collection_append_string(map, "def");
cass_collection_append_int32(map, 456);

cass_statement_bind_collection(statement, 0, map);

/* The colleciton can be freed after binding */
cass_collection_free(map);
```

## Nested Collections

When using Cassandra 2.1+ it is possible to nest collections. A collection can
be added to another collection using [`cass_collection_append_collection()`].

## Custom types

Custom types can be bound using either the `cass_statement_bind_bytes[_by_name]()` or the
`cass_statement_bind_custom[by_name]()` functions. The latter validates the class
name of the custom type matches the class name of the type being bound.

[`cass_collection_append_collection()`]:
http://datastax.github.io/cpp-driver/api/struct.CassCollection/#cass-collection-append-collection
