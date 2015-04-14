# Binding Parameters

The ‘?’ marker is used to denote the bind variables in a query string. This can be used for both regular and prepared parameterized queries. In addition to adding the bind marker to your query string your application must also provide the number of bind variables to `cass_statement_new()` when constructing a new statement. If a query doesn’t require any bind variables then 0 can be used. `cass_statement_bind_*()` functions are then used to bind values to the statement’s variables. Bind variables can be bound by the marker’s index or by name and must be supplied for all bound variables.

```c
/* Create a statement with a single parameter */
CassStatement* statement = cass_statement_new("SELECT * FROM table1 WHERE column1 = ?", 1);

cass_statement_bind_string(statement, 0, cass_string_init("abc"));

/* Execute statement */

cass_statement_free(statement);
```

Variables can only be bound by name for prepared statements. This limitation exists because query metadata provided by Cassandra is required to map the variable name to the variable’s marker index.

```c
/* Prepare statment */

/* The prepared query allocates the correct number of paramters automatically */
CassStatement* statement = cass_prepared_bind(prepared);

/* The parameter can now be bound by name */
cass_statement_bind_string_by_name(statement, "column1", cass_string_init("abc"));

/* Execute statement */

cass_statement_free(statement);
```

## Large Values

The data of values bound to a statement are copied into the statement. That means that values bound to statements can be freed immediately after being bound. However, this might be problematic for large values so the driver provides `cass_statement_bind_custom()` which allocates a buffer where the large values can be directly stored avoiding an extra allocation and copy.

```c
/* Create a statement with two parameters */
CassStatement* statement
  = cass_statement_new("INSERT INTO table1 (column1, column2) VALUES (?, ?)", 2);

cass_statement_bind_string(statement, 0, cass_string_init("abc"));

cass_byte_t* bytes;
cass_statement_bind_custom(statement, 1, 8 * 1024 * 1024, &bytes);

/* 'bytes' then can be used in the application to store a large value */

/* Execute statment */

```

## Constructing Collections

Collections are supported using [`CassCollection`](http://datastax.github.io/cpp-driver/api/struct_cass_collection/) objects; supporting `list`, `map` and `set` Cassandra types. The code below shows how to construct a `list` collection; however, a set can be constructed in a very similiar way. The only difference is the type `CASS_COLLECTION_TYPE_SET` is used to create the collection instead of `CASS_COLLECTION_TYPE_LIST`.

**Important**: Values appended to the colleciton can be freed immediately afterward because the values are copied.

```c
CassStatement* statement = cass_statement_new(query, 1);

CassCollection* list = cass_collection_new(CASS_COLLECTION_TYPE_LIST, 3);

cass_collection_append_string(list, cass_string_init("123"));
cass_collection_append_string(list, cass_string_init("456"));
cass_collection_append_string(list, cass_string_init("789"));

cass_statement_bind_collection(statement, 0, list);

/* The colleciton can be freed after binding */
cass_collection_free(list);
```

Maps are built similarly, but the key and value need to be interleaved as they are appended to the collecion.

```c
CassStatement* statement = cass_statement_new(query, 1);

CassCollection* map = cass_collection_new(CASS_COLLECTION_TYPE_MAP, 2);

/* map["abc"] = 123 */
cass_collection_append_string(map, cass_string_init("abc"));
cass_collection_append_int32(map, 123);

/* map["def"] = 456 */
cass_collection_append_string(map, cass_string_init("def"));
cass_collection_append_int32(map, 456);

cass_statement_bind_collection(statement, 0, map);

/* The colleciton can be freed after binding */
cass_collection_free(map);
```
