# Metrics

Performance metrics and diagnostic information can be obtained from the C/C++
driver using [`cass_session_get_metrics()`]. The resulting [`CassMetrics`] object
contains several useful metrics for accessing request performance and/or
debugging issues.

```c
CassMetrics metrics;

/* Get a snapshot of the driver's metrics */
cass_session_get_metrics(session, &metrics);

```

## Request metrics

The `requests` field  contains information about request latency and
throughput. All latency times are in microseconds and throughput
numbers are in requests per seconds.

## Statistics

The `stats` field contains information about connections
and backpressure markers. If the number of `available_connections` is less than
the number of `total_connections` this could mean the number of I/O threads or
number of connections may need to be increased. The same is true for
`exceeded_pending_requests_water_mark` and `exceeded_write_bytes_water_mark`
metrics. It could also mean the Cassandra cluster is unable to handle the
current request load.

## Errors

The `errors` field contains information about the
occurrence of requests and connection timeouts. Request timeouts occur when
a request fails to get a timely response (default: 12 seconds). Pending request
timeouts occur whens a request waits too long to be serviced by an assigned
host. This can occur when too many requests are in-flight for a single host.
Connection timeouts occur when the process of establishing new connections is
unresponsive (default: 5 seconds).

[`cass_session_get_metrics()`]: http://datastax.github.io/cpp-driver/api/struct_cass_session/#1ab3773670c98c00290bad48a6df0f9eae
[`CassMetrics`]: http://datastax.github.io/cpp-driver/api/struct_cass_metrics/
