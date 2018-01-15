# Keyspaces

## Setting the Keyspace at Connection Time

A session can be initially connected using a supplied keyspace.

**Performance Tip:**  An application should create a single session object per keyspace as a session object is designed to be created once, reused, and shared by multiple threads within the application.

```c
CassSession* session = cass_session_new();
CassCluster* cluster = cass_cluster_new();

/* Configure cluster */

CassFuture* connect_future
  = cass_session_connect_keyspace(session, cluster, "keyspace1");

/* Handle connect future */

cass_future_free(connect_future);

cass_session_free(session);
cass_cluster_free(cluster);
```

## Changing Keyspaces

You can specify a keyspace to change to by executing a `USE` statement on a connection session object.

```c
void use_keyspace(CassSession* session) {
  CassStatement* use_statement
    = cass_statement_new("USE keyspace1", 0);

  CassFuture* use_future
    = cass_session_execute(session, use_statement);

  /* Check future result... */

  cass_statement_free(use_statement);
  cass_future_free(use_future);
}
```

Be very careful though: if the session is shared by multiple threads, switching the keyspace at runtime could easily cause unexpected query failures.

## Single Session and Multiple Keyspaces

It is possible to interact with multiple keyspaces using a single session object by fully qualifying the table names in your queries e.g. `keyspace_name.table_name`.

### Examples

```cql
SELECT * FROM keyspace_name.table_name WHERE ...;
INSERT INTO keyspace_name.table_name (...) VALUES (...);
```

## Creating Keyspaces and Tables

It is also possible to create keyspaces and tables by executing CQL using a session object.

### Examples

```cql
CREATE KEYSPACE cpp_driver
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE TABLE cpp_driver.contributers (
  lastname text,
  firstname test,
  company text,
  website text,
  since timestamp,
  last_activity timestamp
  PRIMARY KEY(lastname));
```
