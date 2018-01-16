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

The [`requests`] field  contains information about request latency and
throughput. All latency times are in microseconds and throughput
numbers are in requests per seconds.

## Statistics

The [`stats`] field contains information about the total number of connections.

## Errors

The [`errors`] field contains information about the
occurrence of requests and connection timeouts. Request timeouts occur when
a request fails to get a timely response (default: 12 seconds). Pending request
timeouts occur whens a request waits too long to be serviced by an assigned
host. This can occur when too many requests are in-flight for a single host.
Connection timeouts occur when the process of establishing new connections is
unresponsive (default: 5 seconds).

[`cass_session_get_metrics()`]: http://docs.datastax.com/en/developer/cpp-driver/latest/api/struct.CassSession/#function-cass_session_get_metrics
[`CassMetrics`]: http://docs.datastax.com/en/developer/cpp-driver/latest/api/struct.CassMetrics/
[`requests`]: http://docs.datastax.com/en/developer/cpp-driver/latest/api/struct.CassMetrics/#attribute-requests
[`stats`]: http://docs.datastax.com/en/developer/cpp-driver/latest/api/struct.CassMetrics/#attribute-stats
[`errors`]: http://docs.datastax.com/en/developer/cpp-driver/latest/api/struct.CassMetrics/#attribute-errors
