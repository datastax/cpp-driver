2.0.1
===========
May 15, 2015

Other
--------
* Removed logging thread. `cass_log_cleanup()` and `cass_log_set_queue_size()`
  are now deprecated. A logging callback registered with
  `cass_log_set_callback()` will now be called on mulitple threads concurrently.
* Fixed an issue where the driver might crash/hang when `system.local` contains
  no entries
* Fixed an issue where the driver might hang under high-load due to missing
  memory fence before `uv_async_send()` in `cass::AsyncQueue::enqueue()`

2.0.0
===========
April 23, 2015

Features
--------
* Added latency-aware load balancing. To enable use:
  `cass_cluster_set_latency_aware_routing()`
* Added performance metrics. To get metrics use:
  `cass_session_get_metrics()`
* Added `CassInet` string conversion functions:
  `cass_inet_string()` and `cass_inet_to_string()`

Other
--------
* Removed internal dependency on Boost. Optional: Boost Atomic can still be used
  as an implementation for internal atomic operations and memory fences.
* Removed wrapper types `CassString`, `CassBytes` and `CassDecimal`
* Added string length functions (functions with the suffix "_n") for all
  functions that previously took a null-terminated or `CassString` parameter.
* Added missing `cass_statement_bind_null_by_name()` function
* Fixed issue where time-based UUIDs (`CassUuid` v4) had the incorrect time on
  32-bit systems

1.0.1
===========
Mar 2, 2015

Other
--------
* Added testing documentation
* Added docmentation for the `varint` data type
* Added a contributing document
* Added version defines to `cassandra.h`
* Fixed an issue where connections were attempted against decommissioned nodes
* Fixed an issue where an invalid key index was used for a single entry
  token-aware routing key
* Improved build error output when an incompatabile version of Boost is used to
  build the driver

1.0.0
===========
Feb 2, 2015

Features
--------
* Libuv 1.x supported

Other
--------
* `cass_cluster_set_num_threads_io()` now returns an error code
* Fixed an issue where a node in a remote DC could reconnect even if it
  was supposed to be ignored.
* Reduced the defaults for IO worker threads (1), core connections (1), and
  max connections (2).
* Fixed crash caused by a null token-aware key value.

1.0.0-rc1
===========
Dec 22, 2014

Features
--------
* Added global logging. Logging is no longer configured per session.
* DC-aware load balancing policy is now the default and uses the DC of the first
  connected contact point. The DC-aware policy now has settings to control the
  number of remote hosts used in each DC (after exhausting the hosts in the
  local DC) and whether to ignore local consistency levels.
* Added socket options for TCP nodelay and keepalive:
  `cass_cluster_set_tcp_nodelay()` and `cass_cluster_set_tcp_keepalive()`.

Other
--------
* `CassSession` objects must now be explictly allocated/freed using
  `cass_session_new()` and `cass_session_free()`. `cass_cluster_connect()` has been
  removed and replaced by `cass_session_connect()`.
* SIGPIPE is ignored on driver threads
* Connection timeout now covers the entire connection handshake instead of just
  the socket connection.
* UUID API has been updated to use explict state via `CassUuidGen` instead of
  using internal global state. See `cass_uuid_gen_new()` and corresponding functions.

1.0.0-beta5
===========
Nov 20, 2014

Features
--------
* Added SSL support
* Added token-aware load balancing. It's enable by default. It can
  be disable using `cass_cluster_set_token_aware_routing()`.
* Added functions to get schema metadata, `cass_session_get_schema()` can
  be use to get a snapshot of the schema metadata and `cass_meta_*()` functions
  can be used to retrieve detailed information about keyspaces, tables, and columns.

Other
--------
* Significantly improved request throughput by batching socket writes.
  Batch sizes can be configured using the function
  `cass_cluster_set_max_requests_per_flush()`. The default is 128 because
  the maximum write size is bounded by the number of pipelined requests
  per connection (for version 1 and 2 of the native protocol).
* Added backpressure mechanism that can be configured for by the
  number of outstanding bytes and the number of requests on a connection. These can
  be configured using `cass_cluster_set_write_bytes_high_water_mark()`,
  `cass_cluster_set_write_bytes_low_water_mark()`,
  `cass_cluster_set_pending_requests_high_water_mark()`, and
  `cass_cluster_set_pending_requests_low_water_mark()`.
* Fixed a crash caused by a request timing out followed by a
  call to the conneciton read callback then the write callback.
* Fixed an issue that prevented the driver from recovering from
  a full cluster outage.
* Fixed an issue that allowed `uv_queue_work()` to be called from an
  application thread.
* Added logic to prevent redundant node refreshes when estabilishing
  a new connection from multiple IO workers.
* Fixed a crash caused by Cassandra 2.1.0 not returning metadata on
  execute requests.
* Fixed a memory leak that could leak request handlers if the control
  connection ran out of stream ids.
* Fixed a crash caused by calling an invalid timeout callback
  when waiting for connection to become available in a pool.
* Build will use an external version of Boost if present.

1.0.0-beta4
===========
Sep 11, 2014

Features
--------
* Added node discovery and now handle topology/status change events
* Queries that modify schema wait for cluster schema agreement
* Added DC aware load balancing policy. The default policy is set to
  round robin.
* Added new methods to configure the load balancing policy,
  `cass_cluster_set_load_balance_round_robin()` and
  `cass_cluster_set_load_balance_dc_aware()`
* It is now possible for a session future to return an error when
  connecting to a cluster. This can occur when the driver is unable to connect
  to any contact points or the provided authentication credentials are invalid.

1.0.0-beta3
===========
Aug 13, 2014

Features
--------
* Added callbacks to futures, `cass_future_set_callback()`
* Added plaintext authentication support, `cass_cluster_set_credentials()`
* Added map iterator, `cass_iterator_from_map()`, key and value pairs can be
  retreived using `cass_iterator_get_map_key()` and `cass_iterator_get_map_value()`
* Values can be bound by name (statements created from prepared statments only)
* Values can be retreived by name from result sets

Other
---------
* Reduced overhead of prepared statements
* Small performance improvement for all queries via reduced allocations

1.0.0-beta2
===========
Jul 17, 2014

Features
--------
* Removed C++1x dependency
* Support for Cassandra 1.2 (native protocol version 1)
* Support for automatic paging
* Added method to set serial consistency on statement
* Added batch type to batches
* Exposed options `cass_cluster_set_max_pending_requests()`,
  `cass_cluster_set_max_simultaneous_creation()`, and
  `cass_cluster_set_max_simultaneous_requests_threshold()`
* Added `cass_result_column_name()` to get column name from a result set

Bug Fixes
---------
* Fixed `StreamManager` so it no longer causes stack corruption
* Fixed issue where connections would log "'Timed out during startup' error on startup for ..."
  when a Session was closed while a connection was during its startup process
* Fixed Row and collections iterator so they return the first and last item properly
* Improved logic for creating new connections so it's based request throughput

Other
---------
* Removed the `setopt` method added set methods for all options
* Removed the `getopt` method
* `cass_batch_new()` and `cass_collection_new()` no longer take a consistency parameter
* `cass_collection_new()` now requires a colleciton type parameter
* `is_map` parameter removed from `cass_statement_bind_collection()`

1.0.0-beta1
===========
Jun 16, 2014

Features
--------
* Support for Cassandra 2.0 (native protocol version 2)
* Support for prepared and batch statements
* Support for collections
