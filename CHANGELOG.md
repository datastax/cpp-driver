1.10.1
===========

Bug Fixes
--------
* [CPP-849] Error result doesn't allow access to keyspace, table, and function
  data
* [CPP-851] Disable deprecated warnings for std::ptr_fun
* [CPP-879] Allow remote hosts to come back up even if policy ignores down hosts
  (community PR from kw217)

Other
--------
* [CPP-220] Remove use of external Boost from unit and integration tests. 
  * We ported all integration tests from Boost to Google test. This includes
    several JIRA issues included in the [CPP-220] epic.
* [CPP-853] Correct linking libraries for unix based OS when using
  CASS_USE_STATIC_LIBS=On 
* [CPP-859] Remove vc_build.bat scripts and update building documentation
* [CPP-872] Fix GCC 9.2+ build
* [CPP-878] Correct compile flags for libraries and executable
* [CPP-882] Correct pthread compiler flag for older CMake versions

Community
--------
* Fix build error when compiling without an SSL implementation (kmaragon)

1.10.0
===========

Bug Fixes
--------
* [CPP-819] - Ensure port is updated on already assigned contact points
* [CPP-825] - Cloud should be verifying the peer certificates CN

1.10.0-alpha2
===========

Features
--------
* [CPP-812] - Enable warnings for implicit casts and fix problems
* [CPP-813] - Detect CaaS and change consistency default
* [CPP-817] - Provide error if mixed usage of secure connect bundle and contact points/ssl context
* [CPP-818] - Add missing configuration elements to Insights startup message

Bug Fixes
--------
* [CPP-802] - Handle prepared id mismatch when repreparing on the fly
* [CPP-815] - Schema agreement fails with SNI
* [CPP-811] - Requests won't complete if they exceed the number of streams on a connection

1.10.0-alpha
===========

Features
--------
* [CPP-787] DataStax cloud platform
  * [CPP-788] Support SNI at connection level using `host_id` as host name
    * [CPP-793] Add SNI support to `SocketConnector` and SSL backend
    * [CPP-794] Add domain name resolution to `SocketConnector`
    * [CPP-795] Replace `Address` with endpoint or host type on connection path
    * [CPP-797] Events need to map from affected node address to `host_id`
    * [CPP-800] Node discovery should use the `host_id` (and endpoint address) instead of the
      node's rpc_address
  * [CPP-790] Configuration API for DBaaS
    * [CPP-791] Add creds.zip support for automatic configuration
    * [CPP-798] Configure authentication and SSL from secure connection bundle configuration
    * [CPP-799] Use metadata service to determine contact points
    * [CPP-788] Support SNI at connection level using `host_id` as host name
    * [CPP-803] Propagate `local_dc` from `CloudClusterMetadataResolver` to load balancing policies

Bug Fixes
--------
* [CPP-786] Fix TLS 1.3 support
* [CPP-806] Fix handling of no contact points

Other
--------
* [CPP-796] Correct compiler flags for mixed C and C++ projects

1.9.0
===========

Features
--------
* [CPP-745] Exponential reconnection policy with jitter
* [CPP-769] Use `clang-format` to fix up formatting
* [CPP-775] Add `clang-format` to Windows

Other
--------
* [CPP-743] Move internal components from `namespace cass` to `namespace datastax::internal`
* [CPP-764] Rename namespace from `cass` to `datastax`
* [CPP-766] Allow RPM packaging script to build RPM packages for Amazon Linux
* [CPP-770] Fix header files include guard (\_\_CASS_XXX to DATASTAX_XXX)
* [CPP-772] Remove per cpp file LOG_FILE attribute to speed up Windows builds

1.8.0
===========

Features
--------
* [CPP-722] Insightful monitoring (Insights) startup message
* [CPP-723] Insightful monitoring (Insights) status event message
* [CPP-741] Allow user to configure client ID
* [CPP-751] Call host listener callback for the initial set of hosts

Bug Fixes
--------
* [CPP-755] UDT metadata not being properly populated/updated

Other
--------
* [CPP-705] Deprecate DC-aware multi-DC settings (`used_hosts_per_remote_dc`
            and `allowRemoteDCsForLocalConsistencyLevel `)
* [CPP-720] Streamline custom allocator
* [CPP-752] Add connection count to Host

1.7.0
===========

Features
--------
* [CPP-108] Add tracing support
* [CPP-402] Add support for host event callback
* [CPP-524] Add client configuration information to STARTUP message
* [CPP-597] Provide a means of sending query to a specific node to facilitate virtual table queries

Bug Fixes
--------
* [CPP-632] It's possible for RequestProcessor to miss add/remove event during initialization
* [CPP-701] Don't use Host::is_up() flag inside of load balancing policies
* [CPP-711] Memory leak in `WaitForHandler`/`SchemaChangeHandler` (reference cycle)

Other
--------
* [CPP-183] Concurrent Control Connection Establishment
* [CPP-405] Namespace rapidjson to avoid conflicts with other rapidjson usages
* [CPP-413] Enable Client Timestamps by Default
* [CPP-510] Try all contact points until one succeeds
* [CPP-585] Deprecate DowngradingConsistencyRetryPolicy
* [CPP-686] Pull in the latest rapidjson changes from master
* [CPP-688] Use plugin architecture for protocol version handling

1.6.0
===========

Features
--------
* [CPP-499] Allow binding local address of connections
* [CPP-657] Support virtual tables (aka system views)

Bug Fixes
--------
* [CPP-648] Attempting to get the metrics before the session is connected will crash (existing issue)
* [CPP-654] Use Boost atomics when building drivers for Visual Studio 2012 x86
* [CPP-662] The timerfd version of MicroTimer doesn't handle zero timeout
* [CPP-663] Mutex in SessionBase can be destroyed while it's still locked
* [CPP-665] Memory issue when socket fails to initialize
* [CPP-666] Metadata crash in schema metadata null tests
* [CPP-667] Timer handles still referenced
* [CPP-668] Unit test `ClusterUnitTest.ReconnectUpdateHosts` can hang
* [CPP-669] Result response's metadata can reference a prepared response that's already been freed
* [CPP-672] Fix memory issues found via `-fstanitize=address`
* [CPP-673] Cluster can hang attempting to close during reconnection
* [CPP-675] Chained callback should only call a single callback once
* [CPP-678] Fix Cassandra version for DSE 6.X releases

Other
--------
* [CPP-611] Reduce coalesce delay to better support latency workloads
* [CPP-615] Remove support for protocols v1 and v2
* [CPP-652] Decouple listeners from connection/initialization
* [CPP-653] Synchronously propagate keyspace updates

1.6.0-beta1
===========

Features
--------
* [CPP-404] Performance: Shared-nothing I/O workers (new internal architecture)
* [CPP-441] Move IO thread request scheduling to a "pull" model
* [CPP-453] Move token map calculation off the session thread
* [CPP-515] Remove support for libuv v0.10.x
* [CPP-518] Thread and connection pool refactor
* [CPP-616] Handle libuv v1.20.4+ version file updates

Bug Fixes
--------
* [CPP-589] Pathological hashing behavior in stream manager
* [CPP-590] Execution profiles are copied for every request (expensive to copy)

1.5.0
===========

Features
--------
* [CPP-578] Add NO_COMPACT startup option

Other
--------
* [CPP-566] Ignore control connection DOWN events if there are open connections
* [CPP-584] win: Message for Boost v1.66.0+ using CMake v3.10.x or lower

1.4.1
===========

Bug Fixes
--------
* [CPP-572] DC aware policy plan returns hosts that are not connected

1.4.0
===========

Features
--------
* [CPP-393] Allow prepared statements to be prepared on all nodes
* [CPP-394] Prepare statements on UP/ADD events
* [CPP-492] Support execution profiles
* [CPP-512] Include hash of result set metadata in prepared statement id
* [CPP-528] Per-query (and per-batch) keyspace support

Bug Fixes
--------
* [CPP-438] Broken build with GCC 7 and OpenSSL 1.1
* [CPP-535] Schema metadata views are not properly updated during drop event
* [CPP-540] NULL columns in table/view metadata caused by side-effect in a
  compiled out assertion
* [CPP-543] Fix dllimport/dllexport
* [CPP-546] Result metadata callback casting requests to invalid types
* [CPP-547] Pool request callback attempting to return a deleted connection

Other
-------
* [CPP-520] Correct exports in DSE API to use DSE_EXPORTS
* [CPP-523] Use arc4random or getrandom instead of /dev/urandom (if available)
* [CPP-526] Add a mutable wrapper around the request object
* [CPP-538] Deprecate watermarks

1.3.1
===========

Bug Fixes
--------
* [CPP-473] Missing symbol cass_cluster_set_queue_size_log
* [CPP-486] Compiler warning 'implicit-fallthrough' causing build errors
* [CPP-491] Unintentional switch case fallthrough in `RequestCallback`
* [CPP-496] CassWriteType CDC and VIEW missing
* [CPP-502] Incorrectly exported symbols cass_error_result_responses_received / required
* [CPP-513] Copy-on-write keyspace logic is incorrect
* [CPP-514] Prepared Statement Crash

Other
--------
* [CPP-449] Update vc_build.bat to include automated build for Visual Studio 2017

1.3.0
===========

Features
--------
* [CPP-360] Added the ability to specify custom memory allocators using
  `cass_alloc_set_functions()`
* [CPP-417] Added speculative execution metrics which can be obtained using
  `cass_session_get_speculative_execution_metrics()`
* [CPP-466] Token aware routing replicas are now randomly shuffled by default
  instead of using a random index

Bug Fixes
--------
* [CPP-368] API Functions that take strings now check for NULL and replace it
  with an empty string
* [CPP-437] Fixed batch encoding performance regression
* [CPP-477] Native protocol "support" messages are now properly decoded, but
  still remain unused
* [CPP-487] Fixed a Kerberos regression caused by providing an empty principal
* [CPP-501] Schema metadata is corrupted when views and indexes exist for a table
* [CPP-503] Schema meta race condition when a view is dropped from a table

Other
--------
* [CPP-136] Decoding now verifies buffer sizes when processing server responses
* [CPP-432] Removed dense table 'empty' columns from metadata
* [CPP-440] Improved encoding performance by preallocating buffers
* [CPP-444] Fixed const correctness of serialization functions
* [CPP-476] Replaced `strlen()` with `sizeof()` for static strings
* [CPP-478] Renamed the class `SpeculativeExecution` to `RequestExection` to
  reduce confusion when it appears in driver logs
* [CPP-480] Added more detailed documentation to help with using Kerberos
  authentication

Merged from OSS 2.7.0
--------
* [CPP-464] Adding a node breaks token map reconstruction
* Fixed an issue where UUID parsing could read past the supplied string
* Fixed various typos in `cassandra.h`
* Fixed missing header for intrinsics when building with MSVC15

1.2.0
===========

Features
--------
* Added support for DSE proxy authentication
* Added support for DSE `DateRangeType`
* Added support for adding DSE geospatial types as elements of collections
* Added support for `DurationType`
* Added support for OpenSSL 1.1
* Added support for protocol DSEv1 and V5 (beta)

1.1.0
===========

Features
--------
* Expose wkt parsing logic for public use:
  * `dse_point_from_wkt()`, `dse_point_from_wkt_n()`
  * `dse_line_string_iterator_reset_with_wkt()`,
    `dse_line_string_iterator_reset_with_wkt_n()`
  * `dse_polygon_iterator_reset_with_wkt()`,
    `dse_polygon_iterator_reset_with_wkt_n()`

Other
---------
* [CPP-416] LineString::to_wkt and Polygon::to_wkt now return "LINESTRING
  EMPTY" and "POLYGON EMPTY", respectively, for empty
  objects.
* [CPP-416] WKT parsing for "LINESTRING EMPTY" and "POLYGON EMPTY" now
* succeeds.

1.0.0
===========

Features
--------
* Added a host targeting policy for graph analytics queries. Graph queries with
  the source "a" will attempt to be routed directly to the analytics master
  node.
* Added geometric types support for graph queries.

1.0.0-rc1
===========

Features
--------
* Added a graph option to set graph query request timeout (default: no timeout)
* Added graph options to set the consistency level of graph query requests
* Added the ability to set the timestamp on graph statements

Other
--------
* Changed the default graph source from "default" to "g"
