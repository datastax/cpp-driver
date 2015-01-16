# Keyspaces

## Setting the keyspace at connection time

It's best to only create a single session per keyspace. A session can be initially connected using a supplied keyspace.

```c
CassFuture* connect_future 
  = cass_session_connect_keyspace(session, cluster, "keyspace1");
```

## Setting the keyspace using a "USE" statement

It's also possible to change the keyspace of an already connected session by executing a "USE" statement.

```c
CassStatement use_statment 
  = cass_statement_new(cass_string_init("USE keyspace1"), 0);

CassFuture* use_future 
  = cass_session_execute(session, use_statement);

/* Check future result... */
```

## Single session mulitple keyspaces

It is possible to use multiple keyspaces from a single session by full qualifying the table names in your queries e.g. `keyspace_name.table_name`.

Examples:

```cql
SELECT * FROM keyspace_name.table_name WHERE ...;
INSERT INTO keyspace_name.table_name (...) VALUES (...);
```
