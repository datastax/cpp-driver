# Tracing

Tracing can be used to troubleshoot query performance problems and can be
enabled per request. When enabled, it provides detailed request information
related to internal, server-side operations. Those operations are stored in
tables under the `system_traces` keyspace.

## Enabling

Tracing can be enabled per request for both statements (`CassStatement`) and
batches (`CassBatch`).

### Enable Tracing on a Statement (`CassStatement`)

```c
const char* query = "SELECT * FROM keyspace1.table1";
CassStatement* statement = cass_statement_new(query, 0);

/* Enable tracing on the statement */
cass_statement_set_tracing(statement, cass_true)

/* ... */
```

### Enable Tracing on a Batch (`CassBatch`)

```
CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);

/* Enable tracing on the batch */
cass_batch_set_tracing(batch, cass_true)

/* ... */
```

## Tracing Identifier

When tracing is enabled, a request's future (`CassFuture`) will provide a unique
tracing identifier. This tracing identifier can be used by an application to
query tables in the `system_traces` keyspace.

```c
CassUuid tracing_id;
if (cass_future_tracing_id(future, &tracing_id) == CASS_OK) {
  /* Use `tracing_id` to query tables in the `system_trace` keyspace */
} else {
  /* Handle error. If this happens then either a request error occurred or the
   * request type for the future does not support tracing.
   */
}
```

**Note**: The driver does not return the actual tracing data for the request. The
application itself must use the returned tracing identifier to query the tables.

## Configuration

By default, when tracing is enabled, the driver will wait for the query's tracing
data to become available in the server-side tables before setting the request's
future. The amount of time it will wait, retry, and the consistency level of the
tracing data can be controls by setting `CassCluster` configuration options.

```c
CassCluster* cluster = cass_cluster_new();

/* Wait a maximum of 15 milliseconds for tracing data to become available */
cass_cluster_set_tracing_max_wait_time(cluster, 15);

/* Wait 3 milliseconds before rechecking for the tracing data */
cass_cluster_set_tracing_retry_wait_time(cluster, 3);

/* Check the tracing data tables using consistency level ONE */
cass_cluster_set_tracing_consistency(cluster, CASS_CONSISTENCY_ONE);

/* ... */
```
