# Client-side timestamps

**Note**: Cassandra 2.1+ is required.

Cassandra uses timestamps to serialize write operations. That is, values with a
more current timestamp are considered to be the most up-to-date version of that
information. By default, timestamps are assigned by the driver on the
client-side. This behavior can be overridden by configuring the driver to use a
timestamp generator or assigning a timestamp directly to a [`CassStatement`] or
[`CassBatch`].

## Monotonically Increasing Timestamp Generator

The monotonic timestamp generator guarantees that all writes that share this
generator will be given monotonically increasing timestamps. This generator
produces microsecond timestamps with the sub-millisecond part generated using an
atomic counter. That guarantees that no more than 1000 timestamps will be
generated for a given millisecond clock tick even when shared by multiple
sessions.

**Warning**: If the rate of 1000 timestamps per millisecond is exceeded this
generator will produce duplicate timestamps.

```c
CassCluster* cluster = cass_cluster_new();

CassTimestampGen* timestamp_gen = cass_timestamp_gen_monotonic_new();

cass_cluster_set_timestamp_gen(cluster, timestamp_gen);

/* ... */

/* Connect sessions */

/* Timestamp generators must be freed */
cass_timestamp_gen_free(timestamp_gen);

cass_cluster_free(cluster);
```

All sessions that connect using this cluster object will share this same
timestamp generator.


## Per Statement/Batch timestamps

Timestamps can also be assigned to individuals [`CassStatement`] or
[`CassBatch`] requests.

```c
CassStatement* statement = cass_statement_new("INSERT INTO * ...", 2);

/* Add a timestamp to the statement */
cass_statement_set_timestamp(statement, 123456789);
```

```c
CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);

/* Add a timestamp to the batch */
cass_batch_set_timestamp(batch, 123456789);

/* Add statments to batch */
```

[`CassStatement`]: http://datastax.github.io/cpp-driver/api/struct.CassStatement/
[`CassBatch`]: http://datastax.github.io/cpp-driver/api/struct.CassBatch/
