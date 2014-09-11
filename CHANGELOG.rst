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
* Support for Cassandra 1.2 (binary protocol version 1)
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
* Support for Cassandra 2.0 (binary protocol version 2)
* Support for prepared and batch statements
* Support for collections
