# Batches

**Note**: Cassandra 2.0+ is required.

Batches can be used to group multiple mutations (`UPDATE`, `INSERT`, `DELETE`) together into a single statement; simple or prepared. There are three different types of batches supported by Cassandra 2.0 or later.

* `CASS_BATCH_TYPE_LOGGED` is used to make sure that multiple mutations across multiple partitions happen atomically, that is, all the included mutations will eventually succeed. However, there is a performance penalty imposed by atomicity guarantee.
* `CASS_BATCH_TYPE_UNLOGGED` is generally used to group mutations for a single partition and do not suffer from the performance penalty imposed by logged batches, but there is no atomicity guarantee for  multi-partition updates.
* `CASS_BATCH_TYPE_COUNTER` is used to group counters updates.

**Important**: Be careful when using batches as a [performance optimization](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/useBatch.html).

```c
void execute_batch(CassSession* session) {
  /* This logged batch will make sure that all the mutations eventually succeed */
  CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);

  /* Statements can be immediately freed after being added to the batch */

  {
    CassStatement* statement
      = cass_statement_new("INSERT INTO example1(key, value) VALUES ('a', '1')", 0);
    cass_batch_add_statement(batch, statement);
    cass_statement_free(statement);
  }

  {
    CassStatement* statement
      = cass_statement_new("UPDATE example2 set value = '2' WHERE key = 'b'", 0);
    cass_batch_add_statement(batch, statement);
    cass_statement_free(statement);
  }

  {
    CassStatement* statement
      = cass_statement_new("DELETE FROM example3 WHERE key = 'c'", 0);
    cass_batch_add_statement(batch, statement);
    cass_statement_free(statement);
  }

  CassFuture* batch_future = cass_session_execute_batch(session, batch);

  /* Batch objects can be freed immediately after being executed */
  cass_batch_free(batch);

  /* This will block until the query has finished */
  CassError rc = cass_future_error_code(batch_future);

  printf("Batch result: %s\n", cass_error_desc(rc));

  cass_future_free(batch_future);
}
```
