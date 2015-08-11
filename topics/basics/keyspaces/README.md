# Keyspaces

## Setting the Keyspace at Connection Time

A session can be initially connected using a supplied keyspace.

**Performance Tip:**  An application should create a single session object per keyspace as a session object is designed to be created once, reused, and shared by multiple threads within the application.

```c
CassFuture* connect_future 
  = cass_session_connect_keyspace(session, cluster, "keyspace1");
```

## Changing Keyspaces

You can specify a keyspace to change to by executing a `USE` statement on a connection session object.

```c
CassStatement use_statment 
  = cass_statement_new("USE keyspace1", 0);

CassFuture* use_future 
  = cass_session_execute(session, use_statement);

/* Check future result... */
```

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
