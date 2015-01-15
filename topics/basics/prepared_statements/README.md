# Prepared Statements

Prepared statements should be used to improve the performance of frequently executed queries. Preparing the query caches it on the Cassandra cluster and only needs to be done once. Once created, prepared statements should be reused with different bind variables. Prepared queries use the `?` marked to denote bind variables in the query string.

```c
CassString insert_query = cass_string_init("INSERT INTO example (key, value) VALUES (?, ?);");
 
/* Prepare the statement on the Cassandra cluster */
CassFuture* prepare_future = cass_session_prepare(session, insert_query);
 
/* Wait for the statement to prepare and get the result */
CassError rc = cass_future_error_code(prepare_future);
 
printf("Prepare result: %s\n", cass_error_desc(rc));
 
if (rc != CASS_OK) {
  /* Handle error */
  cass_future_free(prepare_future);
  return -1;
}
 
/* Get the prepared object from the future */
const CassPrepared* prepared = cass_future_get_prepared(prepared_future);
 
/* The future can be freed immediately after getting the prepared object */
cass_future_free(prepare_future);
 
/* The prepared object can now be used to create statements that can be executed */
CassStatement* statement = cass_prepared_bind(prepared);
 
/* Bind variables by name this time (this can only be done with prepared statements)*/
cass_statement_bind_string_by_name(statement, "key", cass_string_init("abc"));
cass_statement_bind_int32_by_name(statement, "value", 123);
 
/* Execute statement (same a the non-prepared code) */
 
/* The prepared object must be freed */
cass_prepared_free(prepared);
```
