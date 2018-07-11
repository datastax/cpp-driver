# Handling Results

The [`CassResult`](http://datastax.github.io/cpp-driver/api/struct.CassResult/) object
is typically returned for `SELECT` statements. For mutations (`INSERT`, `UPDATE`,
and `DELETE`) only a status code will be present and can be accessed using
`cass_future_error_code()`. However, when using lightweight transactions a
result object will be available to check the status of the transaction. The
result object is obtained from executed statements' future object.

**Important**: Rows, column values, collections, decimals, strings, and bytes
objects are all invalidated when the result object is freed. All of these
objects point to memory held by the result. This allows the driver to avoid
unnecessarily copying data.

```c
void process_result(CassFuture* future) {
  const CassResult* result = cass_future_get_result(future);

  /* Process result */

  cass_result_free(result);
}
```

*Note*: The result object is immutable and can be accessed by multiple threads concurrently.

## Rows and Column Values

The result object represents a collection of rows. The first row, if present,
can be obtained using `cass_result_first_row()`. Multiple rows are accessed
using a [`CassIterator`](http://datastax.github.io/cpp-driver/api/struct.CassIterator/)
object. After a row has been retrieved, the column value(s) can be accessed from
a row by either index or by name. The iterator object can also be used with
enumerated column values.

```c
void process_first_row(const CassResult* result) {
  const CassRow* row = cass_result_first_row(result);

  /* Get the first column value using the index */
  const CassValue* column1 = cass_row_get_column(row, 0);
}
```

```c
void process_first_row_by_name(const CassResult* result) {
  const CassRow* row = cass_result_first_row(result);

  /* Get the value of the column named "column1" */
  const CassValue* column1 = cass_row_get_column_by_name(row, "column1");
}
```

Once the [`CassValue`](http://datastax.github.io/cpp-driver/api/struct.CassValue/)
has been obtained from the column, the actual value can be retrieved and
assigned into the proper datatype.

```c
void get_values_from_row(const CassRow* row) {
  cass_int32_t int_value;
  const CassValue* column1 = cass_row_get_column(row, 0);
  cass_value_get_int32(column1, &int_value);

  cass_int64_t timestamp_value;
  const CassValue* column2 = cass_row_get_column(row, 1);
  cass_value_get_int64(column2, &timestamp_value);

  const char* string_value;
  size_t string_value_length;
  const CassValue* column3 = cass_row_get_column(row, 2);
  cass_value_get_string(column3, &string_value, &string_value_length);
}
```

## Iterators

Iterators can be used to iterate over the rows in a result, the columns in a
row, or the values in a collection.

**Important**: `cass_iterator_next()` invalidates values retrieved by the
previous iteration.

```c
void iterate_over_rows(CassFuture* future) {
  const CassResult* result = cass_future_get_result(future);

  CassIterator* iterator = cass_iterator_from_result(result);

  while (cass_iterator_next(iterator)) {
    const CassRow* row = cass_iterator_get_row(iterator);
    /* Retreive and use values from the row */
  }

  cass_iterator_free(iterator);

  cass_result_free(result);
}
```

All iterators use the same pattern, but will have different iterator creation
and retrieval functions. Iterating over a map collection is slightly different
because it has two values per entry, but utilizes the same basic pattern.

```c
/* Execute SELECT query where a map colleciton is returned */

void iterator_over_map_value(CassFuture* future) {
  const CassResult* result = cass_future_get_result(future);

  const CassRow* row = cass_result_first_row(result);

  const CassValue* map = cass_row_get_column(row, 0);

  CassIterator* iterator = cass_iterator_from_map(map);

  while (cass_iterator_next(iterator)) {
    /* A seperate call is used to get the key and the value */
    const CassValue* key = cass_iterator_get_map_key(iterator);
    const CassValue* value = cass_iterator_get_map_value(iterator);

    /* Use key/value pair */
  }

  cass_iterator_free(iterator);

  cass_result_free(result);
}
```

## Paging

When communicating with Cassandra 2.0 or later, large result sets can be divided
into multiple pages automatically. The
[`CassResult`](http://datastax.github.io/cpp-driver/api/struct.CassResult/) object
keeps track of the pagination state for the sequence of paging queries. When
paging through the result set, the result object is checked to see if more pages
exist where it is then attached to the statement before re-executing the query
to get the next page.

```c
void page_results(CassSession* session) {

  CassStatement* statement = cass_statement_new("SELECT * FROM table1", 0);

  /* Return a 100 rows every time this statement is executed */
  cass_statement_set_paging_size(statement, 100);

  cass_bool_t has_more_pages = cass_true;

  while (has_more_pages) {
    CassFuture* query_future = cass_session_execute(session, statement);

    const CassResult* result = cass_future_get_result(query_future);

    if (result == NULL) {

      /* Handle error */

      cass_future_free(query_future);
      break;
    }

    /* Get values from result... */

    /* Check to see if there are more pages remaining for this result */
    has_more_pages = cass_result_has_more_pages(result);

    if (has_more_pages) {
      /* If there are more pages we need to set the position for the next execute */
      cass_statement_set_paging_state(statement, result);
    }

    cass_future_free(query_future);
    cass_result_free(result);
  }

  cass_statement_free(statement);
}
```

The [`cass_statement_set_paging_state()`] function abstracts the actual paging
state token away from the application. The raw paging state token can be
accessed using [`cass_result_paging_state()`] and added to a statement using
[`cass_statement_set_paging_state_token()`].

**Warning**: The paging state token should not be exposed to or come from
untrusted environments. That paging state could be spoofed and potentially used
to gain access to other data.

[`cass_statement_set_paging_state()`]: http://datastax.github.io/cpp-driver/api/struct.CassStatement/#cass-statement-set-paging-state
[`cass_result_paging_state()`]: http://datastax.github.io/cpp-driver/api/struct.CassResult/#cass-result-paging-state
[`cass_statement_set_paging_state_token()`]: http://datastax.github.io/cpp-driver/api/struct.CassStatement/#cass-statement-set-paging-state-token
